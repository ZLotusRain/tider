import warnings
from typing import Mapping
from datetime import timedelta
from collections import OrderedDict
from kombu.utils import cached_property

from http.cookiejar import CookieJar
from urllib.parse import urlparse, urljoin
from requests.models import PreparedRequest, CaseInsensitiveDict
from requests.utils import (
    should_bypass_proxies,
    get_environ_proxies,
    get_netrc_auth,
    requote_uri,
    rewind_body,
    to_key_val_list
)
from requests.cookies import (
    extract_cookies_to_jar,
    merge_cookies,
    cookiejar_from_dict,
    RequestsCookieJar
)
from requests.exceptions import (
    ChunkedEncodingError,
    ContentDecodingError,
    TooManyRedirects,
    InvalidSchema
)
from requests.status_codes import codes
from requests.sessions import Session as _Session
from requests.sessions import SessionRedirectMixin as _SessionRedirectMixin

from tider.network import Request
from tider.session.adapters import HTTPAdapter, HTTP2Adapter
from tider.utils.misc import try_copy
from tider.utils.time import preferred_clock


def merge_setting(request_setting, session_setting, dict_class=OrderedDict):
    """Determines appropriate setting for a given request, taking into account
    the explicit setting on that request, and the setting in the session. If a
    setting is a dictionary, they will be merged together using `dict_class`
    """

    if session_setting is None:
        return request_setting

    if request_setting is None:
        return session_setting

    # Bypass if not a dictionary (e.g. verify)
    if not (
        isinstance(session_setting, Mapping) and isinstance(request_setting, Mapping)
    ):
        return request_setting

    merged_setting = dict_class(to_key_val_list(session_setting))
    merged_setting.update(to_key_val_list(request_setting))

    # Remove keys that are set to None. Extract keys first to avoid altering
    # the dictionary during iteration.
    none_keys = [k for (k, v) in merged_setting.items() if v is None]
    for key in none_keys:
        del merged_setting[key]

    return merged_setting


def resolve_proxies(request, proxies, trust_env=True):
    """This method takes proxy information from a request and configuration
    input to resolve a mapping of target proxies. This will consider settings
    such a NO_PROXY to strip proxy configurations.

    :param request: Request or PreparedRequest
    :param proxies: A dictionary of schemes or schemes and hosts to proxy URLs
    :param trust_env: Boolean declaring whether to trust environment configs

    :rtype: dict
    """
    proxies = dict(proxies or {})
    url = request.url
    scheme = urlparse(url).scheme
    no_proxy = proxies.get("no_proxy")

    if trust_env and not should_bypass_proxies(url, no_proxy=no_proxy):
        environ_proxies = get_environ_proxies(url, no_proxy=no_proxy)
        proxy = environ_proxies.get(scheme, environ_proxies.get("all"))
        if proxy:
            proxies.setdefault(scheme, proxy)
    return proxies


class SessionRedirectMixin(_SessionRedirectMixin):

    def resolve_redirects(
        self,
        resp,
        req,
        stream=False,
        timeout=None,
        verify=True,
        cert=None,
        proxies=None,
        allow_redirects=True,
        **adapter_kwargs,
    ):
        """Receives a Response. Returns a generator of Responses or Requests."""

        hist = []  # keep track of history

        previous_fragment = urlparse(req.url).fragment
        while resp.redirect_target:
            if len(hist) >= self.max_redirects:
                raise TooManyRedirects(
                    f"Exceeded {self.max_redirects} redirects.", response=resp
                )

            # Update history and keep track of redirects.
            # resp.history must ignore the original request in this loop
            hist.append(resp)
            resp.history = hist[1:]

            url = resp.redirect_target
            prepared_request = req.copy()
            # Handle redirection without scheme (see: RFC 1808 Section 4)
            if url.startswith("//"):
                parsed_rurl = urlparse(resp.url)
                url = ":".join([str(parsed_rurl.scheme), url])

            # Normalize url case and attach previous fragment if needed (RFC 7231 7.1.2)
            parsed = urlparse(url)
            if parsed.fragment == "" and previous_fragment:
                parsed = parsed._replace(fragment=previous_fragment)
            elif parsed.fragment:
                previous_fragment = parsed.fragment
            url = parsed.geturl()

            # Facilitate relative 'location' headers, as allowed by RFC 7231.
            # (e.g. '/path/to/resource' instead of 'http://domain.tld/path/to/resource')
            # Compliant with RFC3986, we percent encode the url.
            if not parsed.netloc:
                url = urljoin(resp.url, requote_uri(url))
            else:
                url = requote_uri(url)

            prepared_request.url = str(url)

            self.rebuild_method(prepared_request, resp)

            # https://github.com/psf/requests/issues/1084
            if resp.status_code not in (
                    codes.temporary_redirect,
                    codes.permanent_redirect,
            ):
                # https://github.com/psf/requests/issues/3490
                purged_headers = ("Content-Length", "Content-Type", "Transfer-Encoding")
                for header in purged_headers:
                    prepared_request.headers.pop(header, None)
                prepared_request.body = None

            headers = prepared_request.headers
            headers.pop("Cookie", None)

            # Extract any cookies sent on the response to the cookiejar
            # in the new request. Because we've mutated our copied prepared
            # request, use the old one that we haven't yet touched.
            extract_cookies_to_jar(prepared_request._cookies, req, resp.raw)
            merge_cookies(prepared_request._cookies, self.cookies)
            prepared_request.prepare_cookies(prepared_request._cookies)

            # Rebuild auth and proxy information.
            proxies = self.rebuild_proxies(prepared_request, proxies)
            self.rebuild_auth(prepared_request, resp)

            # A failed tell() sets `_body_position` to `object()`. This non-None
            # value ensures `rewindable` will be True, allowing us to raise an
            # UnrewindableBodyError, instead of hanging the connection.
            rewindable = prepared_request._body_position is not None and (
                    "Content-Length" in headers or "Transfer-Encoding" in headers
            )

            # Attempt to rewind consumed file-like object.
            if rewindable:
                rewind_body(prepared_request)

            # Override the original request.
            req = prepared_request

            if not allow_redirects:
                resp._next = req
                break
            try:
                resp.content  # Consume socket so it can be released
            except (ChunkedEncodingError, ContentDecodingError, RuntimeError):
                resp.raw.read(decode_content=False)

            # Release the connection back into the pool.
            resp.close()

            resp = self.send(
                req,
                stream=stream,
                timeout=timeout,
                verify=verify,
                cert=cert,
                proxies=proxies,
                allow_redirects=False,
                **adapter_kwargs,
            )

            extract_cookies_to_jar(self.cookies, prepared_request, resp.raw)
            yield resp

    def rebuild_auth(self, prepared_request, response):
        """When being redirected we may want to strip authentication from the
        request to avoid leaking credentials. This method intelligently removes
        and reapplies authentication where possible to avoid credential loss.
        """
        headers = prepared_request.headers
        url = prepared_request.url

        # changes: use response.url instead of response.request.url
        if "Authorization" in headers and self.should_strip_auth(
                response.url, url
        ):
            # If we get redirected to a new host, we should strip out any
            # authentication headers.
            del headers["Authorization"]

        # .netrc might have more auth for us on our new host.
        new_auth = get_netrc_auth(url) if self.trust_env else None
        if new_auth is not None:
            prepared_request.prepare_auth(new_auth)


class Session(_Session, SessionRedirectMixin):

    def __init__(self, http2=False, proxies=None,
                 pool_connections=100, pool_maxsize=100, max_retries=0):
        super().__init__()
        self.proxies = dict(proxies or {})
        self.mount(
            "https://",
            HTTPAdapter(
                pool_connections=pool_connections,
                pool_maxsize=pool_maxsize,
                max_retries=max_retries
            )
        )
        self.mount(
            "http://",
            HTTPAdapter(
                pool_connections=pool_connections,
                pool_maxsize=pool_maxsize,
                max_retries=max_retries
            )
        )

        self.http2 = http2
        self.pool_connections = pool_connections
        self.pool_maxsize = pool_maxsize
        self._http2_inited = False

    @cached_property
    def http2adapter(self):
        self._http2_inited = True
        return HTTP2Adapter(
            pool_connections=self.pool_connections,
            pool_maxsize=self.pool_maxsize
        )

    def evict_manager_for(self, proxy):
        for adapter in self.adapters.values():
            adapter.evict_manager_for(proxy)
        if self._http2_inited:
            self.http2adapter.evict_manager_for(proxy)

    def request(
        self,
        method,
        url,
        params=None,
        data=None,
        headers=None,
        cookies=None,
        files=None,
        auth=None,
        timeout=None,
        allow_redirects=True,
        proxies=None,
        hooks=None,
        stream=None,
        verify=None,
        cert=None,
        json=None
    ):
        if hooks is not None:
            warnings.warn("Parameter `hooks` is not supported in tider", DeprecationWarning)

        method = method.upper()
        headers = dict(headers or {})
        params = try_copy(params or {})
        data = try_copy(data or {})
        files = try_copy(files or [])
        cookies = try_copy(cookies or {})
        proxies = dict(proxies or {})

        # Bootstrap CookieJar.
        if not isinstance(cookies, CookieJar):
            cookies = cookiejar_from_dict(cookies)

        # Merge with session cookies
        merged_cookies = merge_cookies(
            merge_cookies(RequestsCookieJar(), self.cookies), cookies
        )

        # Set environment's basic authentication if not explicitly set.
        if self.trust_env and not auth and not self.auth:
            auth = get_netrc_auth(url)

        # Create the PreparedRequest.
        prep = PreparedRequest()
        prep.prepare(
            method=method,
            url=url,
            headers=merge_setting(
                headers, self.headers, dict_class=CaseInsensitiveDict
            ),
            files=files,
            data=data,
            json=json,
            params=merge_setting(params, self.params),
            auth=merge_setting(auth, self.auth),
            cookies=merged_cookies
        )

        settings = self.merge_environment_settings(
            prep.url, proxies, stream, verify, cert
        )

        # Send the request.
        send_kwargs = {
            "timeout": timeout,
            "allow_redirects": allow_redirects,
        }
        send_kwargs.update(settings)
        resp = self.send(prep, **send_kwargs)

        return resp

    def get_adapter(self, url):
        """
        Returns the appropriate connection adapter for the given URL.

        :rtype: requests.adapters.BaseAdapter
        """
        if self.http2:
            return self.http2adapter
        for (prefix, adapter) in self.adapters.items():
            if url.lower().startswith(prefix.lower()):
                return adapter

        # Nothing matches :-/
        raise InvalidSchema(f"No connection adapters were found for {url!r}")

    def send(self, request, **kwargs):
        # It's possible that users might accidentally send a Request object.
        # Guard against that specific failure case.
        if isinstance(request, Request):
            raise ValueError("You can only send PreparedRequests.")

        kwargs.setdefault("stream", self.stream)
        kwargs.setdefault("verify", self.verify)
        kwargs.setdefault("cert", self.cert)
        if "proxies" not in kwargs:
            kwargs["proxies"] = resolve_proxies(request, self.proxies, self.trust_env)
        if self.http2:
            kwargs.setdefault("trust_env", self.trust_env)

        # Set up variables needed for resolve_redirects
        allow_redirects = kwargs.pop("allow_redirects", True)
        stream = kwargs.get("stream")

        # Get the appropriate adapter to use
        adapter = self.get_adapter(url=request.url)

        # Start time (approximately) of the request
        start = preferred_clock()

        # Send the request
        r = adapter.send(request, **kwargs)

        # Total elapsed time of the request (approximately)
        elapsed = preferred_clock() - start
        r.elapsed = timedelta(seconds=elapsed)

        # Persist cookies
        extract_cookies_to_jar(self.cookies, request, r.raw)

        # Redirect resolving generator.
        gen = self.resolve_redirects(r, request, allow_redirects=allow_redirects, **kwargs)
        history = [resp for resp in gen]

        # Shuffle things around if there's history.
        if history:
            # Insert the first (original) request at the start
            history.insert(0, r)
            # Get the last request made
            r = history.pop()
            r.history = history

        if not stream:
            r.content

        return r

    def close(self):
        """Closes all adapters and as such the session"""
        for v in self.adapters.values():
            v.close()
        self._http2_inited and self.http2adapter.close()
