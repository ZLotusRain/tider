import logging
from urllib3.util import parse_url

logger = logging.getLogger(__name__)


def parse_url_params(url=None, query=None):
    params = {}

    query = query or parse_url(url).query or ""
    query = query.split("&")
    for each in query:
        if "=" not in each:
            continue
        key, value = each.split("=", maxsplit=1)
        params[key] = value

    return params


def parse_url_host(url):
    host = ""
    try:
        host = parse_url(url).host
        port = parse_url(url).port
        host = f"{host}:{port}" if port else host
    except Exception as e:
        logger.error(f"Unable to parse {url}: {e}")
    return host or ""


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
