import logging
import base64
import hashlib

from Crypto.Cipher import AES, DES
from Crypto.Util.Padding import pad, unpad
from binascii import b2a_hex, a2b_hex

logger = logging.getLogger(__name__)


def encrypt(source, key, method, mode="ECB", iv=None, block_size=16, style="pkcs7", encoding="base64"):
    if not isinstance(source, bytes):
        source = source.encode("utf-8")
    if not isinstance(key, bytes):
        key = key.encode("utf-8")
    if iv and not isinstance(iv, bytes):
        iv = iv.encode("utf-8")

    generator = None
    result = ""
    try:
        if method == "AES":
            if mode == "ECB":
                generator = AES.new(key, AES.MODE_ECB)
            elif mode == "CBC":
                generator = AES.new(key, AES.MODE_CBC, iv)
        elif method == "DES":
            if mode == "ECB":
                generator = DES.new(key, DES.MODE_ECB)  # 创建一个aes对象
            elif mode == "CBC":
                generator = DES.new(key, DES.MODE_CBC, iv)
        if not generator:
            return result
        result = generator.encrypt(pad(source, block_size, style))  # 加密明文
        if encoding == "base64":
            result = base64.b64encode(result)  # 将返回的字节型数据转进行base64编码
            result = result.decode("utf-8")  # 将字节型数据转换成python中的字符串类型
        else:
            result = b2a_hex(result).decode()
            # result = result.hex()
    except ValueError as e:
        logger.error(f">>> {method}加密失败[{e}]")
    return result


def decrypt(source, key, method, mode="ECB", iv=None, block_size=16, style="pkcs7", encoding="base64"):
    if not isinstance(source, bytes):
        if encoding == "base64":
            source = source.encode("utf-8")
            source = base64.b64decode(source)
        else:
            source = a2b_hex(source)
    if not isinstance(key, bytes):
        key = key.encode("utf-8")
    if iv and not isinstance(iv, bytes):
        iv = iv.encode("utf-8")

    generator = None
    result = ""
    try:
        if method == "AES":
            if mode == "ECB":
                generator = AES.new(key, AES.MODE_ECB)
            elif mode == "CBC":
                generator = AES.new(key, AES.MODE_CBC, iv)
        elif method == "DES":
            if mode == "ECB":
                generator = DES.new(key, DES.MODE_ECB)  # 创建一个aes对象
            elif mode == "CBC":
                generator = DES.new(key, DES.MODE_CBC, iv)
        if not generator:
            return result
        result = generator.decrypt(source)  # 解密
        result = unpad(result, block_size, style)
        result = result.decode("utf-8")
    except ValueError as e:
        logger.error(f">>> {method}加密失败[{e}]")
    return result


def set_md5(val):
    if not isinstance(val, bytes):
        val = val.encode("utf-8")
    m = hashlib.md5()
    m.update(val)
    return m.hexdigest()
