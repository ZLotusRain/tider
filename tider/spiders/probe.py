from threading import RLock
from collections import deque
from typing import Union

from tider import Promise, Request, Item, Response
from tider.spiders import Spider
from tider.extractors.link_extractor import LinkExtractor
from tider.utils.url import parse_url_host, url_is_from_any_domain
from tider.utils.misc import symbol_by_name, arg_to_iter
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


class Rule:
    """Accept rules."""

    name = None

    def __init__(self, max_depth=2, allow_regexes=(), deny_regexes=(), allow_titles=(), deny_titles=(),
                 allow_domains=(), deny_domains=(), restrict_xpaths=(), tags=('a', 'area'), attrs=('href',),
                 deny_extensions=None, restrict_css=(), file_only=False, ignored_file_types=('Possibly dangerous files',),
                 guess_extension=True, include_extensions=(), file_hints=(), stop_on_found=True):

        self.max_depth = max_depth
        self.stop_on_found = stop_on_found

        tags = set(arg_to_iter(tags))
        for tag in list(tags):
            tags.add(tag.upper())
            tags.add(tag.lower())
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

    def get_links(self, response):
        if not self._done_flag:
            return
        links = self.link_extractor.extract_links(response)
        self.process_links(links)
        if self.found and self.stop_on_found:
            yield from self.maybe_on_stop(response.cb_kwargs['probe_meta'])
            return
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

    def maybe_on_stop(self, meta):
        if self._processing:
            return
        try:
            self._done_flag.pop()
            self._processing.clear()
            yield from iter_generator(self.process_result(meta))
        except IndexError:
            pass

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
            self._processing.remove(link)
        except ValueError:
            pass
        yield from self.maybe_on_stop(meta)

    def process_result(self, meta):
        """Overrides this method to process the probe result based on the specific rule."""


class ProbeSpider(Spider):

    rules: Union[list, dict] = {}

    def __init__(self, retry_on_https_errors=True, **kwargs):
        super().__init__(**kwargs)
        if isinstance(self.rules, list):
            self.rules = {str(idx): rule for idx, rule in enumerate(self.rules)}
        rules = {}
        for name, rule in dict(self.rules).items():
            if isinstance(rule, str):
                rule = symbol_by_name(rule)
            if isinstance(rule, Rule):
                raise ValueError("The rule element must be a class, not an object")
            name = rule.name or name
            rules[name] = rule
        self.rules = rules
        self.retry_on_https_errors = retry_on_https_errors

    def _compile_rules(self):
        rules = {}
        for name, rule in self.rules.items():
            if issubclass(rule, Rule):
                rule = self.build_rule(rule_cls=rule)
            if not isinstance(rule, Rule):
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

    def _build_request(self, url, response=None, link=None, rules=None):
        headers = {"User-Agent": self.default_ua}
        host = parse_url_host(url)
        if host:
            # maybe conflict with playwright.
            # https://github.com/puppeteer/puppeteer/issues/5077
            headers["Host"] = host
        request_kwargs = {
            'max_retries': 3,
            'max_parse_times': 3,
            'headers': headers,
            'encoding': 'utf-8',
        }
        request_kwargs.update(self.prepare_request_kwargs(url, response=response) or {})

        cb_kwargs = {'rules': rules}
        if link:
            cb_kwargs['link'] = link
        else:
            cb_kwargs['start'] = True
        return Request(url=url, callback=self._parse, errback=self._parse, cb_kwargs=cb_kwargs, meta={'depth': 0}, **request_kwargs)

    def start_requests(self, message=None, **kwargs):
        if not message:
            return

        reqs = []
        start_urls = []
        rules = self._compile_rules()
        for start_url in iter_generator(self.gen_start_urls(message)):
            if not start_url or self.url_probed(start_url, message):
                continue
            start_urls.append(start_url)
            reqs.append(self._build_request(start_url, rules=rules))
        if not reqs:
            return
        probe_meta = {
            'start_urls': start_urls,
            'seen': Seen(),
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
        link_rules = response.cb_kwargs['rules']
        link = response.cb_kwargs.get('link')
        start = response.cb_kwargs.get('start', False)
        probe_meta = response.cb_kwargs['probe_meta']
        probe_rules = probe_meta['rules']
        http_errors = (
            response.request.url.startswith('http://') and
            ('WRONG_VERSION_NUMBER' in str(response._error) or
             'EOF occurred in violation of protocol' in str(response._error) or
             not response.status_code)
        )
        if start and http_errors and self.retry_on_https_errors:
            url = response.url.replace('https', 'http')
            yield response.retry(url=url, meta={'depth': 0})
            return
        if not self.response_allowed(response):
            for rule in link_rules:
                if link:
                    yield from probe_rules[rule].on_processed(link=link, meta=probe_meta)
                elif start:
                    yield from probe_rules[rule].maybe_on_stop(meta=probe_meta)
            return
        if not response.ok:
            retry = False
            response.meta['depth'] = max(0, response.meta.get('depth', 0) - 1)
            for request in iter_generator(self.retry_on_errors(response)):
                request.cb_kwargs.setdefault('link', link)
                yield request
                # maybe break here.
                retry = True
            if not retry:
                for rule in link_rules:
                    if link:
                        yield from probe_rules[rule].on_processed(link=link, meta=probe_meta)
                    elif start:
                        yield from probe_rules[rule].maybe_on_stop(meta=probe_meta)
            return

        rules = link_rules.copy()
        if not link:
            self.logger.info(f'Start parsing start url: {response.url}, rules: {",".join(rules).strip(",")}')
        else:
            self.logger.info(f'Start parsing {link["title"] or link["text"]}: {link["url"]}, rules: {",".join(rules).strip(",")}')
        for rule in link_rules:
            for item in iter_generator(probe_rules[rule].parse(response)):
                probe_rules[rule].obtain(item)

        links_d = {}
        start_urls = probe_meta['start_urls']
        seen = probe_meta['seen']
        with seen:
            for rule in rules:
                links = probe_rules[rule].get_links(response)
                for new_link in links:
                    if new_link in seen or new_link['url'] in start_urls or new_link['url'].strip('/') in start_urls:
                        yield from probe_rules[rule].on_processed(new_link, meta=probe_meta)
                    else:
                        if new_link not in links_d:
                            links_d[new_link] = []
                        links_d[new_link].append(rule)
            for new_link in links_d:
                seen.add(new_link)

        for rule in rules:
            if link:
                yield from probe_rules[rule].on_processed(link=link, meta=probe_meta)
            elif start:
                yield from probe_rules[rule].maybe_on_stop(meta=probe_meta)

        for new_link in links_d:
            # extracted links
            link_rules = links_d[new_link]
            for rule in link_rules.copy():
                if (
                        response.meta.get('depth', 0) == probe_rules[rule].max_depth or
                        (probe_rules[rule].found and probe_rules[rule].stop_on_found)
                ):
                    try:
                        # stop probing the rule.
                        link_rules.remove(rule)
                        yield from probe_rules[rule].on_processed(new_link, meta=probe_meta)
                    except ValueError:
                        # maybe removed by other parsers
                        pass
            if link_rules:
                yield self._build_request(new_link["url"], response=response, link=new_link, rules=link_rules)

    def response_allowed(self, response) -> bool:
        """Overrides this method to see if a response should be parsed."""
        return True

    def process_all(self, probe_meta):
        """Overrides this method to process all the probe result."""
