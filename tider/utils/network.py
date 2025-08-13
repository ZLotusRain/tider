import os
import copy
import socket
import struct
import contextlib
from urllib.parse import urlparse

from requests.cookies import cookiejar_from_dict
from requests.utils import proxy_bypass, get_environ_proxies

from tider.network.compat import CookieCompatRequest, CookieCompatResponse


def _parse_content_type_header(header):
    """Returns content type and parameters from given header

    :param header: string
    :return: tuple containing content type and dictionary of
         parameters
    """

    tokens = header.split(";")
    content_type, params = tokens[0].strip(), tokens[1:]
    params_dict = {}
    items_to_strip = "\"' "

    for param in params:
        param = param.strip()
        if param:
            key, value = param, True
            index_of_equals = param.find("=")
            if index_of_equals != -1:
                key = param[:index_of_equals].strip(items_to_strip)
                value = param[index_of_equals + 1 :].strip(items_to_strip)
            params_dict[key.lower()] = value
    return content_type, params_dict


def guess_encoding_from_headers(headers):
    content_type = headers.get("Content-Type", "")
    if not content_type:
        return None

    content_type, params = _parse_content_type_header(content_type)

    if "charset" in params:
        charset = params["charset"].strip("'\"")
        if not charset.startswith("GB"):
            # maybe not standardized when using GB
            return params["charset"].strip("'\"")

    if "text" in content_type:
        return ""  # maybe "ISO-8859-1" or 'UTF-8' or "GB18030"

    if "application/json" in content_type:
        # Assume UTF-8 based on RFC 4627: https://www.ietf.org/rfc/rfc4627.txt since the charset was unset
        return "utf-8"


def cookiejar_from_str(s: str):
    if not s:
        return None
    d = {}
    for each in s.split(';'):
        each = each.strip()
        if '=' not in each:
            continue
        k, v = each.split('=', maxsplit=1)
        d[k] = v
    return cookiejar_from_dict(d)


def copy_cookie_jar(jar):
    if jar is None:
        return None

    if hasattr(jar, "copy"):
        # We're dealing with an instance of RequestsCookieJar
        return jar.copy()
    # We're dealing with a generic CookieJar instance
    new_jar = copy.copy(jar)
    new_jar.clear()
    for cookie in jar:
        new_jar.set_cookie(copy.copy(cookie))
    return new_jar


def extract_cookies_to_jar(jar, request, response):
    """Extract the cookies from the response into a CookieJar.

    :param jar: cookielib.CookieJar (not necessarily a RequestsCookieJar)
    :param request: prepared request
    :param response: urllib3.HTTPResponse-like object
    """
    req = CookieCompatRequest(request)
    res = CookieCompatResponse(response)
    jar.extract_cookies(res, req)


def is_ipv4_address(string_ip):
    """
    :rtype: bool
    """
    try:
        socket.inet_aton(string_ip)
    except OSError:
        return False
    return True


def is_valid_cidr(string_network):
    """
    Very simple check of the cidr format in no_proxy variable.

    :rtype: bool
    """
    if string_network.count("/") == 1:
        try:
            mask = int(string_network.split("/")[1])
        except ValueError:
            return False

        if mask < 1 or mask > 32:
            return False

        try:
            socket.inet_aton(string_network.split("/")[0])
        except OSError:
            return False
    else:
        return False
    return True


def address_in_network(ip, net):
    """This function allows you to check if an IP belongs to a network subnet

    Example: returns True if ip = 192.168.1.1 and net = 192.168.1.0/24
             returns False if ip = 192.168.1.1 and net = 192.168.100.0/24

    :rtype: bool
    """
    ipaddr = struct.unpack("=L", socket.inet_aton(ip))[0]
    netaddr, bits = net.split("/")
    netmask = struct.unpack("=L", socket.inet_aton(dotted_netmask(int(bits))))[0]
    network = struct.unpack("=L", socket.inet_aton(netaddr))[0] & netmask
    return (ipaddr & netmask) == (network & netmask)


def dotted_netmask(mask):
    """Converts mask from /xx format to xxx.xxx.xxx.xxx

    Example: if mask is 24 function returns 255.255.255.0

    :rtype: str
    """
    bits = 0xFFFFFFFF ^ (1 << 32 - mask) - 1
    return socket.inet_ntoa(struct.pack(">I", bits))


@contextlib.contextmanager
def set_environ(env_name, value):
    """Set the environment variable 'env_name' to 'value'

    Save previous value, yield, and then restore the previous value stored in
    the environment variable 'env_name'.

    If 'value' is None, do nothing"""
    value_changed = value is not None
    if value_changed:
        old_value = os.environ.get(env_name)
        os.environ[env_name] = value
    try:
        yield
    finally:
        if value_changed:
            if old_value is None:
                del os.environ[env_name]
            else:
                os.environ[env_name] = old_value


def should_bypass_proxies(url, no_proxy):
    """
    Returns whether we should bypass proxies or not.

    :rtype: bool
    """
    # Prioritize lowercase environment variables over uppercase
    # to keep a consistent behaviour with other http projects (curl, wget).
    def get_proxy(key):
        return os.environ.get(key) or os.environ.get(key.upper())

    # First check whether no_proxy is defined. If it is, check that the URL
    # we're getting isn't in the no_proxy list.
    no_proxy_arg = no_proxy
    if no_proxy is None:
        no_proxy = get_proxy("no_proxy")
    parsed = urlparse(url)

    if parsed.hostname is None:
        # URLs don't always have hostnames, e.g. file:/// urls.
        return True

    if no_proxy:
        # We need to check whether we match here. We need to see if we match
        # the end of the hostname, both with and without the port.
        no_proxy = (host for host in no_proxy.replace(" ", "").split(",") if host)

        if is_ipv4_address(parsed.hostname):
            for proxy_ip in no_proxy:
                if is_valid_cidr(proxy_ip):
                    if address_in_network(parsed.hostname, proxy_ip):
                        return True
                elif parsed.hostname == proxy_ip:
                    # If no_proxy ip was defined in plain IP notation instead of cidr notation &
                    # matches the IP of the index
                    return True
        else:
            host_with_port = parsed.hostname
            if parsed.port:
                host_with_port += f":{parsed.port}"

            for host in no_proxy:
                if parsed.hostname.endswith(host) or host_with_port.endswith(host):
                    # The URL does match something in no_proxy, so we don't want
                    # to apply the proxies on this URL.
                    return True

    with set_environ("no_proxy", no_proxy_arg):
        # parsed.hostname can be `None` in cases such as a file URI.
        try:
            bypass = proxy_bypass(parsed.hostname)
        except (TypeError, socket.gaierror):
            bypass = False

    if bypass:
        return True

    return False


def resolve_proxies(url, proxies, trust_env=True):
    """This method takes proxy information from a request and configuration
    input to resolve a mapping of target proxies. This will consider settings
    such a NO_PROXY to strip proxy configurations.

    :param url: url
    :param proxies: A dictionary of schemes or schemes and hosts to proxy URLs
    :param trust_env: Boolean declaring whether to trust environment configs

    :rtype: dict
    """
    proxies = proxies if proxies is not None else {}
    scheme = urlparse(url).scheme
    no_proxy = proxies.get("no_proxy")
    new_proxies = proxies.copy()

    if trust_env and not should_bypass_proxies(url, no_proxy=no_proxy):
        environ_proxies = get_environ_proxies(url, no_proxy=no_proxy)

        proxy = environ_proxies.get(scheme, environ_proxies.get("all"))

        if proxy:
            new_proxies.setdefault(scheme, proxy)
    return new_proxies
