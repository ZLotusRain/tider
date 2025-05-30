import os
import logging
from typing import Union
from contextlib import contextmanager

import oss2
from oss2.api import _Base
from oss2.compat import to_string
from oss2.models import PartInfo, RequestResult
from oss2.http import Response as BaseResponse, RequestError
from oss2 import determine_part_size, SizedFileAdapter

from tider.network import Response
from tider.utils.log import get_logger
from tider.exceptions import DownloadError, FileStoreError

logger = get_logger(__name__)
oss_logger = logging.getLogger('oss2.api')
oss_http_logger = logging.getLogger('oss2.http')


class OssResponse(BaseResponse):

    def read(self, amt=None):
        try:
            content = super(OssResponse, self).read(amt)
            if self._Response__all_read:
                self.response.close()
            return content
        except Exception:
            self.response.close()
            raise


class Session:

    def __init__(self, crawler):
        self.crawler = crawler
        self._quick_request = crawler.engine.explorer.try_explore

    def do_request(self, req, timeout):
        try:
            oss_http_logger.debug("Send request, method: {0}, url: {1}, params: {2}, headers: {3}, timeout: {4}, proxies: {5}".format(
                req.method, req.url, req.params, req.headers, timeout, req.proxies))
            return OssResponse(self._quick_request(url=req.url, method=req.method,
                                                   data=req.data,
                                                   params=req.params,
                                                   headers=req.headers,
                                                   stream=True,
                                                   timeout=timeout,
                                                   proxies=req.proxies,
                                                   raise_for_status=False, max_retries=2))
        except DownloadError as e:
            raise RequestError(e)


class OssBase(_Base):

    def _do(self, method, bucket_name, key, **kwargs):
        key = to_string(key)
        req = oss2.http.Request(method, self._make_url(bucket_name, key),
                                app_name=self.app_name,
                                proxies=self.proxies,
                                region=self.region,
                                product=self.product,
                                cloudbox_id=self.cloudbox_id,
                                **kwargs)
        self.auth._sign_request(req, bucket_name, key)

        resp = self.session.do_request(req, timeout=self.timeout)
        if resp.status // 100 != 2:
            e = oss2.exceptions.make_exception(resp)  # only consumed 4090 bytes here.
            # add read here to make sure response is consumed,
            # and the response will be read in _parse_result when using parse_func
            resp.read()
            oss_logger.info("Exception: {0}".format(e))
            raise e

        # Note that connections are only released back to the pool for reuse once all body data has been read;
        # be sure to either set stream to False or read the content property of the Response object.
        # For more details, please refer to http://docs.python-requests.org/en/master/user/advanced/#keep-alive.
        content_length = oss2.models._hget(resp.headers, 'content-length', int)
        if content_length is not None and content_length == 0:
            resp.read()

        return resp


class OssBucket(OssBase, oss2.Bucket):
    """Use modified `_do` and `_parse_result` instead."""


class OssService(OssBase, oss2.Service):
    """Use modified `_do` and `_parse_result` instead."""


class AliOSSFilesStore:

    def __init__(self, crawler, access_key_id, access_key_secret, endpoint,
                 domain="", bucket_name=None, timeout=None):
        self.crawler = crawler
        self.auth = oss2.Auth(access_key_id, access_key_secret)
        self.session = Session(crawler=crawler)

        self._domain = domain
        self._endpoint = endpoint
        self.timeout = timeout
        self._bucket_name = bucket_name

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        return cls(
            crawler=crawler,
            access_key_id=settings["OSS_ACCESS_KEY_ID"],
            access_key_secret=settings["OSS_ACCESS_KEY_SECRET"],
            endpoint=settings["OSS_ENDPOINT"],
            bucket_name=settings["OSS_BUCKET_NAME"],
            timeout=settings["OSS_DEFAULT_TIMEOUT"],
            domain=settings["OSS_DOMAIN"]
        )

    @contextmanager
    def _close_session(self, bos: Union[oss2.Bucket, oss2.Service]):
        try:
            yield
        finally:
            session = getattr(bos.session, 'session', None)
            session and session.close()

    def get_bucket(self, bucket_name):
        bucket_name = bucket_name or self._bucket_name
        return OssBucket(session=self.session, auth=self.auth, endpoint=self._endpoint, bucket_name=bucket_name)

    def list_buckets(self):
        service = OssService(self.auth, self._endpoint, session=self.session, connect_timeout=self.timeout)
        buckets = oss2.BucketIterator(service)
        return buckets

    @staticmethod
    def _ensure_read(func, *args, **kwargs):
        result = func(*args, **kwargs)
        if isinstance(result, RequestResult):
            result.resp.read()
        return result

    def new_bucket(self, bucket_name, root=None):
        bucket = self.get_bucket(bucket_name)
        if root is None:
            self._ensure_read(bucket.create_bucket)
        else:
            # set bucket authority
            root = eval("oss2.%s" % root)
            self._ensure_read(bucket.create_bucket, permission=root)

    def delete_bucket(self, bucket_name):
        bucket = self.get_bucket(bucket_name)
        try:
            # delete an empty bucket
            self._ensure_read(bucket.delete_bucket)
        except oss2.exceptions.BucketNotEmpty:
            logger.exception('bucket is not empty.')
        except oss2.exceptions.NoSuchBucket:
            logger.exception('bucket does not exist.')

    def view_bucket_root(self, bucket_name):
        bucket = self.get_bucket(bucket_name)
        return bucket.get_bucket_acl().acl

    def set_bucket_root(self, root, bucket_name):
        bucket = self.get_bucket(bucket_name)
        root = eval("oss2.%s" % root)
        self._ensure_read(bucket.put_bucket_acl, permission=root)

    def delete_object(self, filename, bucket_name):
        bucket = self.get_bucket(bucket_name)
        self._ensure_read(bucket.delete_object, key=filename)

    def upload_resume(self, key, filename, bucket_name):
        bucket = self.get_bucket(bucket_name)
        self._ensure_read(
            oss2.resumable_upload,
            bucket, key, filename, store=oss2.ResumableStore(root='/tmp'), multipart_threshold=100*1024,
            part_size=100*1024, num_threads=4
        )

    def upload_local_file(self, bucket, filename, buf):
        parts = []
        total_size = os.path.getsize(buf)
        part_size = determine_part_size(total_size, preferred_size=1024 * 100)
        upload_id = bucket.init_multipart_upload(filename).upload_id
        with open(buf, 'rb') as fo:
            part_number = 1
            offset = 0
            while offset < total_size:
                num_to_upload = min(part_size, total_size - offset)
                result = self._ensure_read(
                    bucket.upload_part,
                    filename, upload_id, part_number,
                    SizedFileAdapter(fo, num_to_upload)
                )
                parts.append(PartInfo(part_number, result.etag))
                offset += num_to_upload
                part_number += 1
        self._ensure_read(bucket.complete_multipart_upload, filename, upload_id, parts)
        parts.clear()

    def upload_response_content(self, bucket, filename, buf):
        upload_chunk = []
        upload_chunk_length = 0
        parts = []
        part_number = 1
        upload_id = bucket.init_multipart_upload(filename).upload_id
        try:
            for chunk in buf.iter_content(chunk_size=1024 * 100):
                # max chunk size = 8*1024
                upload_chunk.append(chunk)
                upload_chunk_length += len(chunk)
                # chunk size not equal to upload_chunk_length
                if upload_chunk_length < 1024 * 100:
                    continue
                upload_chunk = b''.join(upload_chunk)
                result = self._ensure_read(bucket.upload_part, filename, upload_id, part_number, upload_chunk)
                parts.append(PartInfo(part_number, result.etag, size=len(upload_chunk)))
                part_number += 1
                # reset
                del upload_chunk[:]
                upload_chunk_length = 0
            if upload_chunk:
                upload_chunk = b''.join(upload_chunk)
                result = self._ensure_read(bucket.upload_part, filename, upload_id, part_number, upload_chunk)
                parts.append(PartInfo(part_number, result.etag, size=len(upload_chunk)))
            self._ensure_read(bucket.complete_multipart_upload, filename, upload_id, parts)
        except DownloadError as e:
            logger.error(f"Failed to upload response content to oss, reason: {e}")

    def persist_file(self, path, buf, bucket_name=None, **_):
        try:
            bucket = self.get_bucket(bucket_name)
            if isinstance(buf, Response):
                self.upload_response_content(bucket, path, buf)
            elif isinstance(buf, bytes):
                self._ensure_read(bucket.put_object, path, buf)
            elif isinstance(buf, str) and os.path.exists(buf):
                # bucket.put_object_from_file(key=path, filename=buf)
                self.upload_local_file(bucket, path, buf)
            else:
                raise ValueError(f"Unsupported buffer type: {type(buf)}")
            return "%s/%s" % (self._domain, path)
        except oss2.exceptions.OssError as e:
            raise FileStoreError(e)

    def stat_file(self, path, bucket_name=None, **_):
        bucket = self.get_bucket(bucket_name)
        try:
            meta_info = bucket.get_object_meta(path)
            result = {'last_modified': meta_info.headers["Last-Modified"], 'checksum': meta_info.headers["ETag"]}
            meta_info.resp.read()
            return result
        except oss2.exceptions.OssError:
            return {}

    def close(self):
        self.crawler = None
        del self.session
