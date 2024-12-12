import sys
import html
import re
import base64
import hashlib
import urllib.parse

from Crypto.PublicKey import RSA
from Crypto.Cipher import AES, DES, PKCS1_v1_5
from Crypto.Util.Padding import pad, unpad
from binascii import b2a_hex, a2b_hex

from tider.utils.log import get_logger

logger = get_logger(__name__)


def rsa_encrypt(public_key, message):
    if "-----BEGIN PUBLIC KEY-----\n" not in public_key:
        public_key = '-----BEGIN PUBLIC KEY-----\n' + public_key + '\n-----END PUBLIC KEY-----'
    key = RSA.importKey(public_key)
    cipher = PKCS1_v1_5.new(key)
    encrypted_message = cipher.encrypt(message.encode())
    return base64.b64encode(encrypted_message).decode()


def unescape(string):
    string = urllib.parse.unquote(string)
    quoted = html.unescape(string).encode(sys.getfilesystemencoding()).decode('utf-8')
    # turn to chinese
    return re.sub(r'%u([a-fA-F0-9]{4}|[a-fA-F0-9]{2})', lambda m: chr(int(m.group(1), 16)), quoted)


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
                generator = DES.new(key, DES.MODE_ECB)
            elif mode == "CBC":
                generator = DES.new(key, DES.MODE_CBC, iv)
        if not generator:
            return result
        result = generator.encrypt(pad(source, block_size, style))
        if encoding == "base64":
            result = base64.b64encode(result).decode("utf-8")
        else:
            result = b2a_hex(result).decode()
    except ValueError as e:
        logger.error(f"Failed to encrypt message by {method}, reason: {e}")
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
                generator = DES.new(key, DES.MODE_ECB)
            elif mode == "CBC":
                generator = DES.new(key, DES.MODE_CBC, iv)
        if not generator:
            return result
        result = generator.decrypt(source)
        result = unpad(result, block_size, style)
        result = result.decode("utf-8")
    except ValueError as e:
        logger.error(f"Failed to decrypt message by {method}, reason: {e}")
    return result


def set_md5(val):
    if not isinstance(val, bytes):
        val = val.encode("utf-8")
    m = hashlib.md5()
    m.update(val)
    return m.hexdigest()
