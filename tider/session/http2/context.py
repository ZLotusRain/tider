import os
import sys
import ssl
import typing
import certifi
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def get_ca_bundle_from_env() -> typing.Optional[str]:
    if "SSL_CERT_FILE" in os.environ:
        ssl_file = Path(os.environ["SSL_CERT_FILE"])
        if ssl_file.is_file():
            return str(ssl_file)
    if "SSL_CERT_DIR" in os.environ:
        ssl_path = Path(os.environ["SSL_CERT_DIR"])
        if ssl_path.is_dir():
            return str(ssl_path)
    return None


class SSLContextFactory:

    DEFAULT_CA_BUNDLE_PATH = Path(certifi.where())
    DEFAULT_CIPHERS = ":".join([
        "ECDHE+AESGCM",
        "ECDHE+CHACHA20",
        "DHE+AESGCM",
        "DHE+CHACHA20",
        "ECDH+AESGCM",
        "DH+AESGCM",
        "ECDH+AES",
        "DH+AES",
        "RSA+AESGCM",
        "RSA+AES",
        "!aNULL",
        "!eNULL",
        "!MD5",
        "!DSS"
    ])

    def __init__(self, verify=False, cert=None, trust_env=True, http2=False):
        self.cert = cert
        self.verify = verify
        self.trust_env = trust_env
        self.http2 = http2

    @staticmethod
    def set_minimum_tls_version_1_2(context):
        if hasattr(context, 'minimum_version'):
            context.minimum_version = ssl.TLSVersion.TLSv1_2
        else:
            context.options |= ssl.OP_NO_SSLv2
            context.options |= ssl.OP_NO_SSLv3
            context.options |= ssl.OP_NO_TLSv1
            context.options |= ssl.OP_NO_TLSv1_1

    def _create_default_ssl_context(self):
        """
        Creates the default SSLContext object that's used for both verified
        and unverified connections.
        """
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        self.set_minimum_tls_version_1_2(context)
        context.options |= ssl.OP_NO_COMPRESSION
        context.set_ciphers(self.DEFAULT_CIPHERS)

        if ssl.HAS_ALPN:
            alpn_idents = ["http/1.1", "h2"] if self.http2 else ["http/1.1"]
            context.set_alpn_protocols(alpn_idents)

        if sys.version_info >= (3, 8):  # pragma: no cover
            keylogfile = os.environ.get("SSLKEYLOGFILE")
            if keylogfile and self.trust_env:
                context.keylog_filename = keylogfile

        return context

    def get_context(self):
        logger.debug(
            f"create ssl context => "
            f"verify={self.verify!r} "
            f"cert={self.cert!r} "
            f"trust_env={self.trust_env!r} "
            f"http2={self.http2!r}"
        )
        if self.verify:
            return self.load_ssl_context_verify()
        return self.load_ssl_context_no_verify()

    def load_ssl_context_no_verify(self):
        context = self._create_default_ssl_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        self._load_client_certs(context)
        return context

    def load_ssl_context_verify(self):
        """
        Return an SSL context for verified connections.
        """
        if self.trust_env and self.verify is True:
            ca_bundle = get_ca_bundle_from_env()
            if ca_bundle is not None:
                self.verify = ca_bundle

        if isinstance(self.verify, ssl.SSLContext):
            # Allow passing in our own SSLContext object that's pre-configured.
            context = self.verify
            self._load_client_certs(context)
            return context
        elif isinstance(self.verify, bool):
            ca_bundle_path = self.DEFAULT_CA_BUNDLE_PATH
        elif Path(self.verify).exists():
            ca_bundle_path = Path(self.verify)
        else:
            raise IOError(
                "Could not find a suitable TLS CA certificate bundle, "
                "invalid path: {}".format(self.verify)
            )

        context = self._create_default_ssl_context()
        context.verify_mode = ssl.CERT_REQUIRED
        context.check_hostname = True

        # Signal to server support for PHA in TLS 1.3. Raises an
        # AttributeError if only read-only access is implemented.
        if sys.version_info >= (3, 8):  # pragma: no cover
            try:
                context.post_handshake_auth = True
            except AttributeError:  # pragma: no cover
                pass

        # Disable using 'commonName' for SSLContext.check_hostname
        # when the 'subjectAltName' extension isn't available.
        try:
            context.hostname_checks_common_name = False
        except AttributeError:  # pragma: no cover
            pass

        if ca_bundle_path.is_file():
            logger.debug(f"load_verify_locations cafile={ca_bundle_path!s}")
            context.load_verify_locations(cafile=str(ca_bundle_path))
        elif ca_bundle_path.is_dir():
            logger.debug(f"load_verify_locations capath={ca_bundle_path!s}")
            context.load_verify_locations(capath=str(ca_bundle_path))

        self._load_client_certs(context)

        return context

    def _load_client_certs(self, ssl_context):
        """
        Loads client certificates into our SSLContext object
        """
        if self.cert is not None:
            if isinstance(self.cert, str):
                ssl_context.load_cert_chain(certfile=self.cert)
            elif isinstance(self.cert, tuple) and len(self.cert) == 2:
                ssl_context.load_cert_chain(certfile=self.cert[0], keyfile=self.cert[1])
            elif isinstance(self.cert, tuple) and len(self.cert) == 3:
                ssl_context.load_cert_chain(
                    certfile=self.cert[0],
                    keyfile=self.cert[1],
                    password=self.cert[2],  # type: ignore
                )


def create_ssl_context(cert=None, verify=True, trust_env=True, http2=False) -> ssl.SSLContext:
    return SSLContextFactory(
        cert=cert, verify=verify, trust_env=trust_env, http2=http2
    ).get_context()
