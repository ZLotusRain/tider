import re
import json
import _string
import logging
import jmespath
import simpleeval
from string import Formatter as BaseFormatter
from typing import Union, Any, List, Iterable, Tuple

from tider import Request, Item, Response
from tider.spiders import Spider
from tider.spiders.graph.functions import Functions
from tider.selector import Selector
from tider.utils.misc import symbol_by_name
from tider.utils.url import parse_url_host


__all__ = ('Page', 'GraphSpider', )


class Formatter(BaseFormatter):

    def get_field(self, field_name: str, args, kwargs) -> Tuple[Any, str]:
        first, rest = _string.formatter_field_name_split(field_name)

        obj = self.get_value(first, args, kwargs)
        try:
            json.dumps(obj)
            expression = field_name
            if isinstance(first, int):
                expression = field_name.replace(str(first), "@", 1)
            else:
                obj = {first: obj}
            obj = jmespath.search(expression, obj)
        except json.JSONDecodeError:
            # loop through the rest of the field_name, doing
            #  getattr or getitem as needed
            for is_attr, i in rest:
                if is_attr:
                    obj = getattr(obj, i)
                else:
                    obj = obj[i]

        return obj, first


def _format(format_string: str, *args, **kwargs) -> str:
    if format_string.startswith('context://'):
        field_name = format_string[len('context://'):]
        if field_name == '':
            field_name = "0"
        if field_name.isdigit():
            return args[int(field_name)]
        else:
            return kwargs[field_name]
    else:
        return Formatter().format(format_string, *args, **kwargs)


def _format_dict(d: dict, *args, **kwargs) -> dict:
    """Format all keys from dict ``d`` whose value can be formatted."""
    for k in d:
        if isinstance(d[k], dict):
            _format_dict(d[k], *args, **kwargs)
        elif isinstance(d[k], str):
            d[k] = _format(d[k], *args, **kwargs)
        elif isinstance(d[k], list):
            for idx, item in enumerate(d[k]):
                if isinstance(item, dict):
                    _format_dict(item, *args, **kwargs)
                elif isinstance(item, str):
                    d[k][idx] = _format(item, *args, **kwargs)
    return d


class Page:
    """Route/Hub"""

    EXTRACT_CONFIG_ARGS = ('value', 'xpath', 'xpath_all', 'regex', 'regex_all', 'jsonpath', 'functions')

    def __init__(self, spider, page_id, context=None, request=None, items=None, cookies=None,
                 page_size=0, end_page=-1, total_page=None, total_num=None, next_pages=None,
                 log=None, cb_kwargs=None, level=0, **_):
        self.page_id = page_id
        self.level = level
        self.context = {}
        self.cb_kwargs = cb_kwargs or {}

        self.request = request or {}
        self.items = [items] if isinstance(items, dict) else items or []

        # list-related
        self.page_size = max(1, page_size)
        self.end_page = end_page
        self.total_page = 1

        self._spider = spider
        if isinstance(log, str):
            self._log = {'msg': log}
        else:
            self._log = log or {}
        self._total_page_rule = total_page
        self._total_num_rule = total_num
        self.cookies = dict(cookies or {})
        self._page_context = context or {}
        self.context = {k: self.extract(config_or_val=v) for k, v in self._page_context.items()}  # promise
        self._next_pages = next_pages or {}

    def start_requests(self) -> Iterable:
        if self.request:
            yield self.gen_request()
        else:
            pages = self._next_pages.get('pages', [])
            callback = self._next_pages.get('callback', None)
            yield from self.gen_next_pages(pages=pages, callback=callback)

    def gen_next_pages(self, pages=None, callback=None, context=None, item=None):
        context = context or {}
        for page in pages:
            condition = page.get('condition')
            if condition:
                op = condition.get('operator')
                lvalue = self.extract(config_or_val=condition['lvalue']['value'], context=context)
                rvalue = self.extract(config_or_val=condition['rvalue']['value'], context=context)
                flag = self._evaluate_condition(lvalue=lvalue, rvalue=rvalue, operator=op)
                if not flag:
                    continue
            page_id = page['page_id']
            pass_item = page.get('pass_item', True)
            pass_context = page.get('pass_context', False)
            iter_config = page.get('iterator', {})
            if iter_config:
                for iter_context in self.resolve_iterator(**iter_config):
                    context.update(iter_context)
                    next_page = self.get_next_page(page_id, pass_context=pass_context, context=context,
                                                   pass_item=pass_item, item=item)
                    yield from next_page.start_requests()
            else:
                next_page = self.get_next_page(page_id, pass_context=pass_context, context=context,
                                               pass_item=pass_item, item=item)
                yield from next_page.start_requests()

    def get_next_page(self, page_id, pass_context=False, context=None, pass_item=True, item=None):
        page_context = {}
        if pass_context:
            page_context = self.context.copy()
        page_context.update(**(context or {}))
        if pass_item and item:
            page_context.update({'item': item})
        page = self._spider.get_page(page_id, context=page_context, cookies=self.cookies)
        return page

    def update_context(self, context):
        self.context.update(context)

    def log(self, message, level=logging.INFO, context=None):
        format_kwargs = self.context.copy()
        format_kwargs.update(**dict(context or {}))
        self._spider.log(message=_format(message, **format_kwargs), level=level)

    def get_total_page(self, response):
        total_page = 1
        try:
            if self._total_page_rule:
                total_page = self._extract_total_page(response)
            if self._total_num_rule:
                total_num = self._extract_total_num(response)
                total_page = total_num // self.page_size + 1
        except (IndexError, ValueError):
            self._spider.logger.warning("Failed to extract total page.")
            total_page = 1
        return total_page

    def _extract_total_page(self, response):
        return int(self.extract(source=response, config_or_val=self._total_page_rule))

    def _extract_total_num(self, response):
        return int(self.extract(source=response, config_or_val=self._total_num_rule))

    @staticmethod
    def _evaluate_condition(lvalue: Any, rvalue: Any, operator: str) -> bool:
        return simpleeval.simple_eval(expr=f'lvalue {operator} rvalue', names={'lvalue': lvalue, 'rvalue': rvalue})

    def resolve_iterator(self, params: dict = None, actions: list = None, syntax: str = None,
                         condition: dict = None, iter_var: str = "", iterable: Any = None,
                         iter_context: dict = None) -> Iterable:
        iter_context = iter_context or {}
        params = params or {}
        iter_context.update({k: self.extract(config_or_val=v, context=iter_context) for k, v in params.items()})
        actions = actions or []
        if syntax == 'for':
            iterable = self.extract(config_or_val=iterable, context=iter_context)
            if isinstance(iterable, dict):
                iter_var = [each.strip() for each in iter_var.split(',') if each.strip() != '_']
                for k, v in iterable.items():
                    iter_context.update(dict(zip(iter_var, [k, v])))
                    if actions:
                        yield from self._perform_actions(actions, action_context=iter_context)
                    else:
                        yield dict(iter_context)
            else:
                for v in iterable:
                    if iter_var.strip() != '_':
                        iter_context.update({iter_var: v})
                    if actions:
                        yield from self._perform_actions(actions, action_context=iter_context)
                    else:
                        yield dict(iter_context)
        elif syntax == 'while':
            op = condition.get('operator')
            lvalue = self.extract(config_or_val=condition['lvalue']['value'], context=iter_context)
            rvalue = self.extract(config_or_val=condition['rvalue']['value'], context=iter_context)
            flag = self._evaluate_condition(lvalue=lvalue, rvalue=rvalue, operator=op)
            while flag and actions:
                # actions is required to avoid infinite loop.
                iter_context.update({condition['lvalue']['name']: lvalue})
                yield from self._perform_actions(actions, action_context=iter_context)
                lvalue = self.extract(config_or_val=condition['lvalue']['value'], context=iter_context)
                flag = self._evaluate_condition(lvalue=lvalue, rvalue=rvalue, operator=op)

    def _perform_actions(self, actions, action_context=None):
        action_context = action_context or {}
        for action in actions:
            if action['type'] == 'iterator':
                iter_config = action.copy()
                iter_config.pop('type')
                yield from self.resolve_iterator(**iter_config, iter_context=action_context)
            elif action['type'] == 'function_call':
                function_name = action['name']
                function_params = action.get('params')
                action_context[action['target']] = self.call(name=function_name, params=function_params, context=action_context)
            elif action['type'] == 'yield':
                yield dict(action_context)

    def to_dict(self):
        return {
            "page_id": self.page_id,
            "request": self.request,
            "items": self.items,
            "page_size": self.page_size,
            "total_page": self._total_page_rule,
            "total_num": self._total_num_rule,
            "end_page": self.end_page,
            "context": self._page_context,
            "next_pages": self._next_pages,
            "log": self._log,
        }

    def call(self, name, args=(), params=None, context=None):
        if params:
            params = {k: self.extract(config_or_val=v, context=context) for k, v in params.items()}
        return self._spider.call(name, args=args, params=params)

    def extract(self, config_or_val: Any = None, context: dict = None,
                source: Union[Response, Selector, dict, str, None] = None) -> Any:
        ctx = self.context.copy()
        ctx.update(**dict(context or {}))
        if isinstance(config_or_val, str):
            if config_or_val.startswith('context://'):
                path = config_or_val.split('context://')[-1]
                return jmespath.search(path, ctx)
            return _format(config_or_val, **ctx)
        if not isinstance(config_or_val, dict):
            return config_or_val
        if all([k not in self.EXTRACT_CONFIG_ARGS for k in config_or_val]):
            return _format_dict(dict(config_or_val), **ctx)
        # config_or_val maybe can't be extracted -> TypeError: unexpected keyword argument
        return self._extract(source=source, context=ctx, **config_or_val)

    def _extract(self, source: Union[Response, Selector, dict, str, None] = None, value: Any = None,
                 xpath: str = None, xpath_all: str = None, regex: str = None, regex_all: str = None,
                 jsonpath: str = None, context: dict = None, functions: Union[List[dict], dict, None] = None) -> Any:
        result = None
        if value is not None:
            result = value
            if isinstance(value, str) and value.startswith('context://'):
                path = value.split('context://')[-1]
                result = jmespath.search(path, data=context)
            if hasattr(result, 'copy'):
                result = result.copy()
        if isinstance(source, Response):
            source = source.selector
        if xpath or xpath_all:
            if isinstance(source, str):
                source = Selector(text=source)
                try:
                    source = source.xpath('//body/*')[0]
                except IndexError:
                    pass
            if isinstance(source, Selector):
                if xpath:
                    result = source.xpath(xpath)[0].get()
                else:
                    result = source.xpath(xpath_all).getall()
        elif jsonpath:
            if isinstance(source, dict):
                result = jmespath.search(jsonpath, source)
            elif isinstance(source, Selector):
                result = source.jmespath(jsonpath).getall()
                if len(result) == 1:
                    result = result[0]
            elif isinstance(source, str):
                try:
                    source = json.loads(source)
                    result = jmespath.search(jsonpath, source)
                except json.JSONDecodeError:
                    pass
        if regex or regex_all:
            if result is not None:
                source = result
            if isinstance(source, str):
                if regex:
                    result = re.findall(regex, source)[0]
                else:
                    result = re.findall(regex_all, source)
            elif isinstance(source, Selector):
                if regex:
                    result = source.re_first(regex)
                else:
                    result = source.re(regex_all)

        functions = functions or []
        if isinstance(functions, dict):
            functions = [functions]
        for function in functions:
            result = self.call(function['name'], (result, ), params=function.get('params'), context=context)
        if isinstance(result, str):
            result = _format(result, **context)
        elif isinstance(result, dict):
            result = _format_dict(result, **context)
        return result

    def update_request_kwargs(self, request_kwargs):
        pass

    def gen_request(self, context=None):
        context = context or {}
        context.setdefault('page_num', 1)
        context.setdefault('page_size', self.page_size)
        context.setdefault('total_page', self.total_page)
        request_context = self.request.get('context', {})
        context.update({k: self.extract(config_or_val=v, context=context) for k, v in request_context.items()})

        url = self.extract(config_or_val=self.request['url'], context=context)
        default_h = self._spider.headers.copy()
        default_h.update({'Host': parse_url_host(url)})
        headers = self.extract(config_or_val=self.request.get('headers'), context=context) or {}
        headers.update(default_h)

        request_kwargs = {
            'url': url,
            'headers': headers,
            'params': self.extract(config_or_val=self.request.get('params'), context=context) or {},
            'data': self.extract(config_or_val=self.request.get('data'), context=context) or {},
            'json': self.extract(config_or_val=self.request.get('json'), context=context) or {},
            'proxy_schema': self.request.get('proxy_schema', 0),
            'allow_redirects': self.request.get('allow_redirects', False),
        }
        # maybe add sign or other auth headers.
        self.update_request_kwargs(request_kwargs)
        if 'method' in self.request:
            request_kwargs.update(method=self.request.get('method'))
        if 'max_retries' in self.request:
            request_kwargs.update(max_retries=self.request.get('max_retries'))
        if 'max_parse_times' in self.request:
            request_kwargs.update(max_parse_times=self.request.get('max_parse_times'))
        if 'ignored_status_codes' in self.request:
            request_kwargs.update(ignored_status_codes=self.request.get('ignored_status_codes'))
        if 'raise_for_status' in self.request:
            request_kwargs.update(raise_for_status=self.request.get('raise_for_status'))
        cookies = self.request.get('cookies', {})
        cookies.update(dict(self.cookies))
        return Request(callback=self.parse, errback=self.parse, cb_kwargs=context, cookies=cookies, **request_kwargs)

    def blocked_by_security(self, response: Response) -> bool:
        """
        Check if the response is blocked by website security mechanisms.
        """
        if (
            response.ok
            or response.status_code >= 500
            or response.status_code == 404
        ):
            return False
        return True

    def handle_security_block(self, response: Response):
        """
        Handle/Bypass security blocks.
        """
        yield response.retry(reset_proxy=True)

    def parse(self, response: Response):
        self.cookies.update(response.cookies)

        if self.blocked_by_security(response):
            yield from self.handle_security_block(response)
            return

        context = response.cb_kwargs
        if self._log:
            self.log(message=self._log['msg'], level=self._log.get('level') or logging.INFO, context=context)
        total_page = self.get_total_page(response)
        self.total_page = max(self.total_page, total_page) if total_page != -1 else total_page
        total_page = min(total_page, self.end_page) if self.end_page != -1 else total_page

        found = False
        selector = response.selector
        for item_config in self.items:
            nodes = item_config.get('nodes')
            fields = item_config.get('fields')
            if not fields:
                continue
            if not nodes:
                nodes = [selector]  # the whole text
            else:
                nodes = self.extract(config_or_val=nodes, source=response, context=context)
            if isinstance(nodes, (dict, str)):
                nodes = [nodes]
            for node in nodes:
                item_context = context.copy()
                new_item = dict(self.context.get('item', {}))
                for name, field_config in fields.items():
                    item_context.update(item=new_item)
                    new_item[name] = self.extract(source=node, config_or_val=field_config, context=item_context)
                found = True
                next_pages = item_config.get('next_pages') or {}
                pages = next_pages.get('pages')
                if pages:
                    callback = next_pages.get('callback')
                    yield from self.gen_next_pages(pages=pages, context=item_context, item=new_item, callback=callback)
                else:
                    yield from self.finalize_item(new_item)
        context = context.copy()
        page_num = context.get('page_num', 1)
        if page_num + 1 <= total_page or (total_page == -1 and found):
            context.update({"page_num": page_num + 1, "total_page": total_page})
            yield self.gen_request(context=context)
        else:
            pages = self._next_pages.get('pages', [])
            callback = self._next_pages.get('callback', None)
            yield from self.gen_next_pages(pages=pages, callback=callback, context=context)

    def finalize_item(self, item):
        yield from self._spider.finalize_item(item)


class GraphSpider(Spider):

    item_cls = Item
    page_cls = Page

    name = 'graph'

    def __init__(self, name, url=None, headers=None, context=None, pages=None, start_page='start',
                 page_cls=None, page_config=None, scrape_all=False, **kwargs):
        super().__init__(name, **kwargs)
        self._pages = {}
        for page in pages:
            if isinstance(page, str):
                # pages -> dict
                page_id = page
                page = pages[page_id]
            else:
                # pages -> list
                page_id = page.pop('page_id')
            self._pages[page_id] = page
        self._context = context or {}
        self._functions = Functions(spider=self)

        self.url = url
        if page_cls is not None:
            self.page_cls = symbol_by_name(page_cls) or self.page_cls
        self._page_config = page_config or {}
        self.scrape_all = scrape_all
        self._start_page = start_page
        self.headers = headers or {"User-Agent": self.default_ua}

    def _build_page(self, page_id=None, level=0, context=None, request=None,
                    page_size=0, end_page=-1, total_page=None, total_num=None, items=None, next_pages=None,
                    log=None, cb_kwargs=None, cookies=None, page_cls=None, **kwargs):
        page_context = self._context.copy()
        page_context.update(**(context or {}))
        if page_cls:
            page_cls = symbol_by_name(page_cls)
        page_cls = page_cls or self.page_cls
        return page_cls(spider=self, page_id=page_id, level=level, context=page_context, request=request,
                        page_size=page_size, end_page=end_page, total_page=total_page, total_num=total_num,
                        items=items, cookies=cookies, next_pages=next_pages, log=log, cb_kwargs=cb_kwargs, **kwargs)

    def get_page(self, page_id, context=None, cookies=None):
        context = {k: {'value': v} for k, v in (context or {}).items()}  # can be extracted.
        page_config = self._page_config.copy()
        page_config.update(dict(self._pages[page_id]))
        page_context = page_config.pop('context', {})
        page_context.update(context)
        return self._build_page(page_id=page_id, context=page_context, cookies=cookies, **page_config)

    def call(self, name, args=(), params=None):
        return self._functions.call(name, *args, params=params)

    def start_requests(self, **kwargs):
        try:
            page = self.get_page(self._start_page)
        except KeyError:
            raise ValueError(f"Start page {self._start_page} not found")
        yield from page.start_requests()

    def finalize_item(self, item, item_cls=None):
        # maybe multi items
        item_cls = item_cls or self.item_cls
        yield item_cls.from_dict(item)
