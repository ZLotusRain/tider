from typing import cast
from collections.abc import Mapping
from typing import NamedTuple
from urllib3.util.url import parse_url as urllib3_parse_url
from urllib.parse import parse_qsl, urlparse, urlunparse, unquote, ParseResult

from tider.utils.log import get_logger

logger = get_logger(__name__)


class urlparts(NamedTuple):
    """Named tuple representing parts of the URL."""

    scheme: str
    hostname: str
    port: int
    username: str
    password: str
    path: str
    query: Mapping


def url_to_parts(url):
    # type: (str) -> urlparts
    """Parse URL into :class:`urlparts` tuple of components."""
    scheme = urlparse(url).scheme
    schemeless = url[len(scheme) + 3:]
    # parse with HTTP URL semantics
    parts = urlparse('http://' + schemeless)
    path = parts.path or ''
    path = path[1:] if path and path[0] == '/' else path
    return urlparts(
        scheme,
        unquote(parts.hostname or '') or None,
        parts.port,
        unquote(parts.username or '') or None,
        unquote(parts.password or '') or None,
        unquote(path or '') or None,
        dict(parse_qsl(parts.query)),
    )


_parse_url = url_to_parts


def parse_url(url, encoding=None) -> ParseResult:
    """Return urlparsed url from the given argument (which could be an already
    parsed url)
    """
    if isinstance(url, ParseResult):
        return url
    if isinstance(url, bytes):
        url = url.decode(encoding or 'utf-8', errors='strict')
    return cast(ParseResult, urlparse(url))


def map_query(url=None, query=None):
    params = {}

    if not query:
        result = parse_url(url)
        query = result.query
        if not query and result.fragment:
            # https://example.com/#/?query=query
            query = parse_url(result.fragment).query
    query = query.split("&")
    for each in query:
        if "=" not in each:
            continue
        key, value = each.split("=", maxsplit=1)
        params[key] = value

    return params


def parse_url_host(url):
    return parse_url(url).netloc.lower()


def url_is_from_any_domain(url, domains):
    """Return True if the url belongs to any of the given domains"""
    # noinspection PyBroadException
    try:
        host = parse_url(url).netloc.lower()
        if not host:
            return False
    except Exception:
        return False
    domains = [d.lower() for d in domains]
    return any((host == d) or (host.endswith(f'.{d}')) for d in domains)


def url_has_any_extension(url, extensions):
    return url.split(".")[-1] in extensions


def get_auth_from_url(url):
    """Given an url with authentication components, extract them into a tuple of
    username,password.

    :rtype: (str,str)
    """
    parsed = urlparse(url)

    try:
        auth = (unquote(parsed.username), unquote(parsed.password))
    except (AttributeError, TypeError):
        auth = ("", "")

    return auth


def prepend_scheme_if_needed(url, new_scheme):
    """Given a URL that may or may not have a scheme, prepend the given scheme.
    Does not replace a present scheme with the one provided as an argument.

    :rtype: str
    """
    parsed = urllib3_parse_url(url)
    scheme, auth, host, port, path, query, fragment = parsed

    # A defect in urlparse determines that there isn't a netloc present in some
    # urls. We previously assumed parsing was overly cautious, and swapped the
    # netloc and path. Due to a lack of tests on the original defect, this is
    # maintained with parse_url for backwards compatibility.
    netloc = parsed.netloc
    if not netloc:
        netloc, path = path, netloc

    if auth:
        # parse_url doesn't provide the netloc with auth
        # so we'll add it ourselves.
        netloc = "@".join([auth, netloc])
    if scheme is None:
        scheme = new_scheme
    if path is None:
        path = ""

    return urlunparse((scheme, netloc, path, "", query, fragment))
