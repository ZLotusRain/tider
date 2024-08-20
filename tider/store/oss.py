import os
import oss2
import logging
from oss2.models import PartInfo
from oss2 import determine_part_size, SizedFileAdapter

from tider.store import FilesStore
from tider.network import Response


logger = logging.getLogger(__name__)


class AliOSS(FilesStore):

    def __init__(self, access_key_id, access_key_secret, timeout, oss_prefix=""):
        self.auth = oss2.Auth(access_key_id, access_key_secret)
        self.timeout = timeout
        self.oss_prefix = oss_prefix

        self.service = None
        self._bucket_cache = {}

    @classmethod
    def from_settings(cls, settings):
        store_settings = settings.get("FILE_STORE_SETTINGS")
        return cls(access_key_id=store_settings["access_key_id"],
                   access_key_secret=store_settings["access_key_secret"],
                   timeout=store_settings["timeout"],
                   oss_prefix=store_settings["oss_prefix"])

    def init_service(self, endpoint):
        self.service = oss2.Service(self.auth, endpoint, self.timeout)

    def list_buckets(self, endpoint):
        if not self.service:
            self.init_service(endpoint)
        list_buckets = oss2.BucketIterator(self.service)
        return list_buckets

    def get_bucket(self, bucket_name, endpoint):
        return oss2.Bucket(auth=self.auth, endpoint=endpoint, bucket_name=bucket_name)

    def new_bucket(self, bucket_name, endpoint, root=None):
        bucket = self.get_bucket(bucket_name, endpoint)
        if root is None:
            bucket.create_bucket()
        else:
            root = eval("oss2.%s" % root)
            bucket.create_bucket(root)

    def delete_bucket(self, bucket_name, endpoint):
        bucket = self.get_bucket(bucket_name, endpoint)
        try:
            bucket.delete_bucket()
        except oss2.exceptions.BucketNotEmpty:
            logger.exception('bucket is not empty.')
        except oss2.exceptions.NoSuchBucket:
            logger.exception('bucket does not exist')

    def view_bucket_root(self, bucket_name, endpoint):
        bucket = self.get_bucket(bucket_name, endpoint)
        root = bucket.get_bucket_acl().acl
        return root

    def set_bucket_root(self, bucket_name, endpoint, root):
        bucket = self.get_bucket(bucket_name, endpoint)
        root = eval("oss2.%s" % root)
        bucket.put_bucket_acl(root)

    def is_file_exist(self, bucket_name, endpoint, file_name):
        bucket = self.get_bucket(bucket_name, endpoint)
        return bucket.object_exists(file_name)

    def upload_resume(self, bucket_name, endpoint, key, filename):
        bucket = self.get_bucket(bucket_name, endpoint)
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

    def persist_file(self, path, buf, bucket_name, endpoint, **kwargs):
        oss_url = ""
        try:
            bucket = self.get_bucket(bucket_name, endpoint)
            if isinstance(buf, Response):
                self.upload_response_content(bucket, path, buf)
            elif isinstance(buf, bytes):
                bucket.put_object(path, buf)
            elif isinstance(buf, str) and os.path.exists(buf):
                # bucket.put_object_from_file(key=path, filename=buf)
                self.upload_local_file(bucket, path, buf)
            else:
                raise ValueError(f"Unsupported buffer type: {type(buf)}")
            oss_url = "%s/%s" % (self.oss_prefix, path)
        except Exception as e:
            reason = str(e) or e.__class__.__name__
            logger.error(f"Failed to upload `{path}` to oss: {reason}")
        return oss_url

    def stat_file(self, bucket_name, endpoint, object_name, **kwargs):
        bucket = self.get_bucket(bucket_name, endpoint)
        meta_info = bucket.get_object_meta(object_name)
        return {'last_modified': meta_info.headers["Last-Modified"], 'checksum': meta_info.headers["ETag"]}
