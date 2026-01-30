from threading import RLock
from collections import deque
from typing import Union

from tider import Promise, Request, Item, Response
from tider.spiders import Spider
from tider.extractors.link_extractor import LinkExtractor, FiletypeCategory
from tider.utils.url import parse_url_host, url_is_from_any_domain
from tider.utils.imports import symbol_by_name
from tider.utils.functional import iter_generator


class Seen:

    def __init__(self, lock=None):
        self._seen = set()
        self._lock = lock if lock is not None else RLock()

    def add(self, item):
        self._seen.add(item)

    def __contains__(self, item):
        return item in self._seen

    def __enter__(self):
        self._lock.acquire()
        return self

    def __exit__(self, *exc_info):
        self._lock.release()


class ProbeRule:
    """Accept rules."""

    name = None

    def __init__(self, max_depth=2, stop_on_found=True, allow_regexes=(), deny_regexes=(), allow_titles=(), deny_titles=(),
                 allow_domains=(), deny_domains=(), restrict_xpaths=(), tags=('a', 'area'), attrs=('href',),
                 deny_extensions=None, restrict_css=(), file_only=False, file_hints=(), include_extensions=(),
                 ignored_file_types=(FiletypeCategory.PossiblyDangerousFiles, ), guess_extension=True):
        self.max_depth = max_depth
        self.stop_on_found = stop_on_found

        self.link_extractor = LinkExtractor(
            allow=allow_regexes, deny=deny_regexes, allow_domains=allow_domains, deny_domains=deny_domains,
            restrict_xpaths=restrict_xpaths, tags=tags, attrs=attrs, allow_titles=allow_titles,
            deny_titles=deny_titles, deny_extensions=deny_extensions, restrict_css=restrict_css, file_only=file_only,
            ignored_file_types=ignored_file_types, guess_extension=guess_extension, include_extensions=include_extensions,
            file_hints=file_hints, unique=True, strip=True,
        )
        self._found = deque()
        self._done_flag = deque([None], maxlen=1)
        self._processing = []

    @property
    def done(self):
        return not self._done_flag

    def get_links(self, response):
        if self.done:
            return
        if response.meta.get('depth', 0) == self.max_depth:
            return
        if response.selector.type == 'html' and response.xpath('//a'):
            links = self.link_extractor.extract_links(response)
            self.process_links(links)
            # add all to processing instead of trying stop.
            for link in links:
                if not self.link_allowed(link, response):
                    continue
                self._processing.append(link)
                yield link

    def link_allowed(self, link, response) -> bool:
        """Overrides this method to see if a link should be allowed."""
        url = response.url
        try:
            host = parse_url_host(url)
            parts = host.split(".")
            if host:
                domains = [host]
                if len(parts) == 3:
                    domains.append(host.split('.', maxsplit=1)[-1])
                elif len(parts) == 4:
                    domains.append(".".join((parts[1], parts[2], parts[3])))
                    if not host.endswith(('edu.cn', 'gov.cn', 'com.cn', 'org.cn')):
                        domains.append(".".join((parts[2], parts[3])))
                if not url_is_from_any_domain(link['url'], domains):
                    return False
        except ValueError:
            pass
        return True

    def process_links(self, links):
        pass

    def discard(self, link):
        self._processing.remove(link)

    def stop(self, meta):
        try:
            self._done_flag.pop()
            yield from iter_generator(self.process_result(meta))
        except IndexError:
            pass
        self._processing.clear()

    def obtain(self, item: Union[dict, Item]):
        if isinstance(item, dict):
            item = Item.from_dict(item)
        self._found.append(item)

    @property
    def found(self):
        return len(self._found) > 0

    @property
    def items(self):
        return list(self._found)

    def parse(self, response: Response):
        """Overrides this method to parse the response related to this rule."""

    def on_processed(self, link, meta):
        try:
            if link is not None:
                # avoid compare non-link object.
                self.discard(link)
        except ValueError:
            pass
        if not self._processing or self.found and self.stop_on_found:
            yield from self.stop(meta)

    def process_result(self, meta):
        """Overrides this method to process the probe result based on the specific rule."""


class HttpsToHttpMiddleware:

    def process_response(self, request, response):
        is_start_request = request.meta.get('is_start_request')
        is_https_request = request.url.startswith('https://')
        if not is_start_request or not is_https_request:
            return response

        retry_with_http = request.meta.get('retry_with_http')
        https_errors = (
            not response.status_code or
            'WRONG_VERSION_NUMBER' in str(response._error) or
            'EOF occurred in violation of protocol' in str(response._error) or
            '631 Internal Server Error' in str(response._error) or
            '<h1>404 Not Found</h1>' in response.text
        )
        if retry_with_http and https_errors:
            url = request.url.replace('https', 'http')
            meta = request.meta
            meta.update(depth=0, retry_times=0)
            request = request.replace(meta=meta, dup_check=False)
            return request.replace(url=url, meta=meta, dup_check=False)
        return response


class ProbeSpider(Spider):
    seen_cls = Seen
    rules: Union[list, dict] = {}

    custom_settings = {
        "EXPLORER_MIDDLEWARES": {
            "tider.spiders.probe.HttpsToHttpMiddleware": 1,
        }
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        rules_map = {}
        iter_rules = (
            ((None, r) for r in self.rules)
            if isinstance(self.rules, list)
            else self.rules.items()
        )
        for name, rule in iter_rules:
            rule_cls = symbol_by_name(rule) if isinstance(rule, str) else rule
            if not issubclass(rule_cls, ProbeRule):
                raise ValueError("The rule element must be a subclass of `tider.spiders.probe.ProbeRule`")
            if name is None:
                name = getattr(rule_cls, 'name', None) or rule_cls.__name__
            if name in rules_map:
                raise ValueError(f"Duplicated rule: {name}")
            rules_map[name] = rule_cls
        self.rules = rules_map

    def _compile_rules(self):
        rules = {}
        for name, rule in self.rules.items():
            if issubclass(rule, ProbeRule):
                rule = self.build_rule(rule_cls=rule)
            if not isinstance(rule, ProbeRule):
                raise ValueError('Unrecognized rule: {}.'.format(name))
            rules[name] = rule
        return rules

    def build_rule(self, rule_cls):
        """Overrides this method to initiate the rule object."""
        return rule_cls()

    def gen_start_urls(self, message):
        """Overrides this method to generate start urls."""

    def prepare_request_kwargs(self, url, response: Response):
        """Overrides this method to prepare request kwargs."""

    def _build_request(self, url, response=None, source_link=None, accept_rules=None):
        headers = {"User-Agent": self.default_ua}
        host = parse_url_host(url)
        if host:
            # maybe conflict with playwright.
            # https://github.com/puppeteer/puppeteer/issues/5077
            headers["Host"] = host
        request_kwargs = {
            'meta': {},
            'max_retries': 3,
            'max_parse_times': 3,
            'headers': headers,
            'encoding': 'utf-8',
        }
        request_kwargs.update(self.prepare_request_kwargs(url, response=response) or {})

        meta = request_kwargs.pop('meta')
        if not source_link:
            meta['retry_with_http'] = True
        cb_kwargs = {'accept_rules': accept_rules or [], 'source_link': source_link}
        return Request(url=url, callback=self._parse, errback=self._parse, cb_kwargs=cb_kwargs, meta=meta, **request_kwargs)

    def start_requests(self, message=None, **kwargs):
        if not message:
            return

        reqs = []
        start_urls = []
        accept_rules = list(self.rules.keys())
        for start_url in iter_generator(self.gen_start_urls(message)):
            if not start_url or self.url_probed(start_url, message):
                continue
            start_urls.append(start_url)
            reqs.append(self._build_request(start_url, accept_rules=accept_rules))
        if not reqs:
            return
        rules = self._compile_rules()
        probe_meta = {
            'start_urls': start_urls,
            'seen': self.seen_cls(),
            'rules': rules,
            'message': message.copy(),
        }
        promise = Promise(reqs=reqs, values={'probe_meta': probe_meta}, callback=self.process_all)
        return promise.then()

    def url_probed(self, url, message) -> bool:
        """Overrides this method to see if a start url has been probed."""
        return False

    def link_allowed(self, link, response) -> bool:
        """Overrides this method to see if a link should be allowed."""
        return True

    def retry_on_errors(self, response):
        """Generate retry requests on response errors"""

    def _parse(self, response):
        probe_meta = response.cb_kwargs['probe_meta']
        accept_rules = response.cb_kwargs.get('accept_rules') or []
        source_link = response.cb_kwargs.get('source_link')

        soup = response.soup('lxml')
        refresh_meta = soup.find('meta', attrs={'http-equiv': 'refresh'})
        refresh_url = refresh_meta.get('content', '').split('url=')[-1] if refresh_meta else None
        should_refresh = refresh_url and refresh_url not in response.url
        if not source_link and should_refresh:
            parse_times = response.request.meta.get('parse_times', 0) - 1
            yield response.retry(url=response.urljoin(refresh_url), meta={'depth': 0, 'parse_times': parse_times})
            return

        probe_rules = probe_meta['rules']
        if not self.response_allowed(response):
            for rule in accept_rules:
                # link may be None if parsing start.
                yield from probe_rules[rule].on_processed(link=source_link, meta=probe_meta)
            return

        if response.failed:
            has_retried_on_failure = False
            response.meta['depth'] = max(0, response.meta.get('depth', 0) - 1)
            for request in iter_generator(self.retry_on_errors(response)):
                request.cb_kwargs.setdefault('source_link', source_link)
                yield request
                has_retried_on_failure = True
            if not has_retried_on_failure:
                for rule in accept_rules:
                    # link may be None if parsing start.
                    yield from probe_rules[rule].on_processed(link=source_link, meta=probe_meta)
            return

        if not source_link:
            log_message = f'Start parsing start url: {response.url}'
        else:
            log_message = f'Start parsing {source_link["title"] or source_link["text"]}: {source_link["url"]}'
        self.logger.info(f'{log_message}, rules: {",".join(accept_rules).strip(",")}')

        for rule in accept_rules:
            for item in iter_generator(probe_rules[rule].parse(response)):
                probe_rules[rule].obtain(item)

        links_accepts = {}
        start_urls = probe_meta['start_urls']
        seen = probe_meta['seen']
        with seen:
            for rule in accept_rules:
                # push child links
                links = probe_rules[rule].get_links(response)
                for new_link in iter_generator(links):
                    # avoid stopping rule before all links processed.
                    if new_link in seen:
                        probe_rules[rule].discard(new_link)
                    elif new_link['url'] in start_urls or new_link['url'].strip('/') in start_urls:
                        # exclude first page
                        probe_rules[rule].discard(new_link)
                    elif probe_rules[rule].found and probe_rules[rule].stop_on_found:
                        # maybe found in links
                        probe_rules[rule].discard(new_link)
                    else:
                        if new_link not in links_accepts:
                            links_accepts[new_link] = []
                        links_accepts[new_link].append(rule)
            for new_link in links_accepts:
                seen.add(new_link)

        for link, new_accepts in links_accepts.items():
            if not new_accepts:
                continue
            yield self._build_request(link["url"], response=response, source_link=link, accept_rules=new_accepts)

        for rule in accept_rules:
            # pop current processed link.
            yield from probe_rules[rule].on_processed(link=source_link, meta=probe_meta)

    def response_allowed(self, response) -> bool:
        """Overrides this method to see if a response should be parsed."""
        return True

    def process_all(self, probe_meta):
        """Overrides this method to process all the probe result."""
