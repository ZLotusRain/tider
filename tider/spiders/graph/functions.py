import json
import datetime
import inspect
import jmespath
import simpleeval
from urllib.parse import urljoin, quote, unquote
from collections import namedtuple

from tider.selector import extract_regex, Selector
from tider.utils.misc import symbol_by_name


func_info_t = namedtuple('func_info_t', [
    'alias', 'description', 'signature', 'args', 'required', 'variadic',
])


def get_methods(obj):
    for name, method in inspect.getmembers(obj, predicate=inspect.isfunction):
        yield name, method


def meta(*args, **kwargs):
    def _assign_meta(func):
        func.meta = func_info_t(*args, **kwargs)
        return func
    return _assign_meta


class Functions:

    funcs = {}
    metas = {}

    def __init__(self, spider):
        self._spider = spider
        self._find_inner_funcs()

    @classmethod
    def register(cls, *args, **kwargs):
        if args:
            return cls._register(**kwargs)(*args)
        return cls._register(**kwargs)

    @classmethod
    def _register(cls, name=None, alias=None, signature=None, args=None,
                  desc=None, variadic=None):

        def _inner(fun):
            func_name = name or fun.__name__
            if func_name.startswith('_func_'):
                func_name = func_name[len('_func_'):]
            description = desc or (fun.__doc__ or '').strip().split('\n')[0]
            cls.funcs[func_name] = fun
            cls.metas[func_name] = func_info_t(
                alias=alias, description=description, signature=signature,
                args=args, variadic=variadic, required=[]
            )
            if alias:
                cls.funcs[alias] = fun
            return fun
        return _inner

    def _find_inner_funcs(self):
        for name, method in get_methods(self):
            if not name.startswith('_func_'):
                continue
            func_meta = getattr(method, 'meta', None)
            if func_meta is not None:
                self.funcs[name[6:]] = method
                self.metas[name[6:]] = func_meta

    def _resolve_func_params(self, name, params):
        func_meta = self.metas.get(name, {})
        args = func_meta.args or []
        for arg in args:
            if len(arg) == 2:
                param, typ = arg
            else:
                param, typ, _ = arg
            params[param] = typ(params[param])
        return params

    def call(self, name, *args, params=None):
        if '.' in name:
            func = symbol_by_name(name)
        else:
            func = self.funcs.get(name)
        if not func:
            raise ValueError('UnKnown Function {}.'.format(name))
        params = dict(params or {})
        func_parameters = inspect.signature(func).parameters
        func_params_list = list(func_parameters.items())
        if func_params_list[0][0] == 'spider':
            func_params_list = func_params_list[1:]
        for arg in args:
            for param, typ in func_params_list:
                if param not in params:
                    params[param] = arg
                    break
        params = self._resolve_func_params(name, params)
        if 'spider' in func_parameters:
            params['spider'] = self._spider
        return func(**params)


def function_call(**kwargs):
    return Functions.register(**kwargs)


@function_call(args=[('days', int, '')])
def get_relative_date(date=None, days=0, seconds=0, microseconds=0,
                      milliseconds=0, minutes=0, hours=0, weeks=0):
    date = date or datetime.datetime.today()
    offset = datetime.timedelta(days=days, seconds=seconds, microseconds=microseconds,
                                milliseconds=milliseconds, minutes=minutes, hours=hours, weeks=weeks)
    return date + offset


@function_call()
def strptime(date_string, fmt="%Y-%m-%d"):
    return datetime.datetime.strptime(date_string, fmt)


@function_call()
def _func_urljoin(url, base=None, spider=None):
    base = base or spider.url
    return urljoin(base, url)


@function_call()
def _func_url_by_page_num(page_num, urls):
    for url, page_range in urls.items():
        if not isinstance(page_range, (list, tuple)):
            try:
                if page_num == int(page_range):
                    return url
            except (TypeError, ValueError):
                pass
            continue
        start, end = page_range
        try:
            start = int(start)
        except (TypeError, ValueError):
            start = None
        try:
            end = int(end)
        except (TypeError, ValueError):
            end = None
        if start is not None and end is not None:
            if page_num in range(start, end + 1):
                return url
        elif start is not None:
            if page_num >= start:
                return url
        elif end is not None:
            if page_num <= end:
                return url


@function_call()
def _func_quote(url):
    return quote(url)


@function_call()
def _func_unquote(url):
    return unquote(url)


@function_call()
def _func_re(text, regex):
    return extract_regex(regex, text)[0]


@function_call()
def _func_re_all(text, regex):
    return extract_regex(regex, text)


@function_call()
def _func_slice(string, start=None, end=None):
    try:
        start = int(start)
    except (TypeError, ValueError):
        start = None
    try:
        end = int(end)
    except (TypeError, ValueError):
        end = None
    if start is not None and end is not None:
        return string[start:end]
    elif start is not None:
        return string[start:]
    elif end is not None:
        return string[:end]


@function_call()
def _func_single_format(string, format_arg):
    return string.format(format_arg)


@function_call()
def _func_split(string, separator):
    return string.split(separator)


@function_call()
def _func_split_first(string, separator):
    return string.split(separator)[0]


@function_call()
def _func_split_last(string, separator):
    return string.split(separator)[-1]


@function_call()
def _func_split_index(string, separator, index):
    return string.split(separator)[index]


@function_call()
def _func_index(obj, index):
    return obj[index]


@function_call()
def _func_replace(string, old, new):
    return string.replace(old, new)


@function_call()
def _func_xpath(text, query):
    selector = Selector(text=text)
    try:
        selector = selector.xpath('//body/*')[0]
    except IndexError:
        pass
    return selector.xpath(query)[0].get()


@function_call()
def _func_xpath_all(text, query):
    selector = Selector(text=text)
    try:
        selector = selector.xpath('//body/*')[0]
    except IndexError:
        pass
    return selector.xpath(query).getall()


@function_call()
def _func_abs(number):
    return abs(number)


@function_call()
def _func_avg(array):
    if array:
        return sum(array) / len(array)
    else:
        return None


@function_call()
def _func_contains(subject, search):
    return search in subject


@function_call()
def _func_join(separator, array):
    return separator.join(array)


@function_call()
def _func_add(arg1, arg2):
    return arg1 + arg2


@function_call()
def _func_eval(expression):
    return simpleeval.simple_eval(expression)


@function_call()
def _func_to_string(obj):
    if isinstance(obj, str):
        return obj
    else:
        return json.dumps(obj, separators=(',', ':'), default=str, ensure_ascii=False)


@function_call()
def _func_update(obj, value, path=""):
    if not path:
        obj.update(value)
    else:
        part = jmespath.search(path, obj)
        part.update(value)
    return obj


@function_call()
def _func_keys(obj):
    return list(obj.keys())


@function_call()
def _func_values(obj):
    return list(obj.values())
