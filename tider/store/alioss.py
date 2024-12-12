import os
from contextlib import contextmanager
from typing import Union

import oss2
from oss2 import Bucket, Service
from oss2.models import PartInfo
from oss2 import determine_part_size, SizedFileAdapter


from tider.network import Response
from tider.utils.log import get_logger

logger = get_logger(__name__)


class AliOSSFilesStore:

    def __init__(self, access_key_id, access_key_secret, endpoint,
                 timeout=None, domain="", bucket_name=None):
        self.auth = oss2.Auth(access_key_id, access_key_secret)
        self._endpoint = endpoint
        self.timeout = timeout

        self._domain = domain
        self._bucket_name = bucket_name

    @classmethod
    def from_settings(cls, settings):
        return cls(
            access_key_id=settings["OSS_ACCESS_KEY_ID"],
            access_key_secret=settings["OSS_ACCESS_KEY_SECRET"],
            endpoint=settings["OSS_ENDPOINT"],
            bucket_name=settings["OSS_BUCKET_NAME"],
            timeout=settings["OSS_DEFAULT_TIMEOUT"],
            domain=settings["OSS_DOMAIN"]
        )

    @contextmanager
    def _close_session(self, bos: Union[Bucket, Service]):
        try:
            yield
        finally:
            bos.session.session.close()

    def get_bucket(self, bucket_name):
        bucket_name = bucket_name or self._bucket_name
        return oss2.Bucket(auth=self.auth, endpoint=self._endpoint, bucket_name=bucket_name)

    def list_buckets(self):
        service = oss2.Service(self.auth, self._endpoint, connect_timeout=self.timeout)
        with self._close_session(service):
            buckets = oss2.BucketIterator(service)
            return buckets

    def new_bucket(self, bucket_name, root=None):
        bucket = self.get_bucket(bucket_name)
        with self._close_session(bucket):
            if root is None:
                bucket.create_bucket()
            else:
                # set bucket authority
                root = eval("oss2.%s" % root)
                bucket.create_bucket(root)

    def delete_bucket(self, bucket_name):
        bucket = self.get_bucket(bucket_name)
        with self._close_session(bucket):
            try:
                # delete an empty bucket
                bucket.delete_bucket()
            except oss2.exceptions.BucketNotEmpty:
                logger.exception('bucket is not empty.')
            except oss2.exceptions.NoSuchBucket:
                logger.exception('bucket does not exist')

    def view_bucket_root(self, bucket_name):
        bucket = self.get_bucket(bucket_name)
        with self._close_session(bucket):
            root = bucket.get_bucket_acl().acl
            return root

    def set_bucket_root(self, root, bucket_name):
        bucket = self.get_bucket(bucket_name)
        with self._close_session(bucket):
            root = eval("oss2.%s" % root)
            bucket.put_bucket_acl(root)

    def delete_object(self, filename, bucket_name):
        bucket = self.get_bucket(bucket_name)
        with self._close_session(bucket):
            return bucket.delete_object(filename)

    def upload_resume(self, key, filename, bucket_name):
        bucket = self.get_bucket(bucket_name)
        with self._close_session(bucket):
            oss2.resumable_upload(
                bucket, key, filename, store=oss2.ResumableStore(root='/tmp'), multipart_threshold=100*1024,
                part_size=100*1024, num_threads=4
            )

    @staticmethod
    def upload_local_file(bucket, filename, buf):
        parts = []
        total_size = os.path.getsize(buf)
        # determine_part_size方法用于确定分片大小。
        part_size = determine_part_size(total_size, preferred_size=1024 * 100)
        upload_id = bucket.init_multipart_upload(filename).upload_id
        with open(buf, 'rb') as fo:
            part_number = 1
            offset = 0
            while offset < total_size:
                num_to_upload = min(part_size, total_size - offset)
                # 调用SizedFileAdapter(fileobj, size)方法会生成一个新的文件对象，重新计算起始追加位置。
                result = bucket.upload_part(filename, upload_id, part_number,
                                            SizedFileAdapter(fo, num_to_upload))
                parts.append(PartInfo(part_number, result.etag))
                offset += num_to_upload
                part_number += 1
        bucket.complete_multipart_upload(filename, upload_id, parts)
        parts.clear()

    @staticmethod
    def upload_response_content(bucket, filename, buf):
        upload_chunk = []
        upload_chunk_length = 0
        parts = []
        part_number = 1
        upload_id = bucket.init_multipart_upload(filename).upload_id
        for chunk in buf.iter_content(chunk_size=1024 * 100):
            # max chunk size = 8*1024
            upload_chunk.append(chunk)
            upload_chunk_length += len(chunk)
            # chunk size not equal to upload_chunk_length
            if upload_chunk_length < 1024 * 100:
                continue
            upload_chunk = b''.join(upload_chunk)
            result = bucket.upload_part(filename, upload_id, part_number, upload_chunk)
            parts.append(PartInfo(part_number, result.etag, size=len(upload_chunk)))
            part_number += 1
            # reset
            upload_chunk = []
            upload_chunk_length = 0
        if upload_chunk:
            upload_chunk = b''.join(upload_chunk)
            result = bucket.upload_part(filename, upload_id, part_number, upload_chunk)
            parts.append(PartInfo(part_number, result.etag, size=len(upload_chunk)))
        upload_chunk.clear()
        bucket.complete_multipart_upload(filename, upload_id, parts)

    def persist_file(self, path, buf, bucket_name=None, **_):
        bucket = self.get_bucket(bucket_name)
        with self._close_session(bucket):
            if isinstance(buf, Response):
                self.upload_response_content(bucket, path, buf)
            elif isinstance(buf, bytes):
                bucket.put_object(path, buf)
            elif isinstance(buf, str) and os.path.exists(buf):
                # bucket.put_object_from_file(key=path, filename=buf)
                self.upload_local_file(bucket, path, buf)
            else:
                raise ValueError(f"Unsupported buffer type: {type(buf)}")
            return "%s/%s" % (self._domain, path)

    def stat_file(self, path, bucket_name=None, **_):
        bucket = self.get_bucket(bucket_name)
        with self._close_session(bucket):
            try:
                meta_info = bucket.get_object_meta(path)
                return {'last_modified': meta_info.headers["Last-Modified"], 'checksum': meta_info.headers["ETag"]}
            except oss2.exceptions.OssError:
                return {}

    def close(self):
        pass
