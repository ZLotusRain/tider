import re
from re import RegexFlag
# import types
import typing as t
# from functools import partial


class SearchableDict(dict):
    def __init__(self, defaults: t.Optional[dict] = None) -> None:
        dict.__init__(self, defaults or {})

    def get_namespace(
            self, namespace: str, lowercase: bool = True, trim_namespace: bool = True
    ) -> t.Dict[str, t.Any]:
        """Returns a dictionary containing a subset of configuration options
        that match the specified namespace/prefix. Example usage::

            test['IMAGE_STORE_TYPE'] = 'fs'
            test['IMAGE_STORE_PATH'] = '/var/app/images'
            test['IMAGE_STORE_BASE_URL'] = 'http://img.website.com'
            image_store_config = test.get_namespace('IMAGE_STORE_')

        The resulting dictionary `image_store_config` would look like::

            {
                'type': 'fs',
                'path': '/var/app/images',
                'base_url': 'http://img.website.com'
            }

        This is often useful when configuration options map directly to
        keyword arguments in functions or class constructors.

        :param namespace: a configuration namespace
        :param lowercase: a flag indicating if the keys of the resulting
                          dictionary should be lowercase
        :param trim_namespace: a flag indicating if the keys of the resulting
                          dictionary should not include the namespace
        """
        rv = {}
        for k, v in self.items():
            if not k.startswith(namespace):
                continue
            if trim_namespace:
                key = k[len(namespace):]
            else:
                key = k
            if lowercase:
                key = key.lower()
            rv[key] = v
        return rv

    def search(self, pattern: str, flags: t.Union[int, RegexFlag] = 0):
        prog = re.compile(pattern, flags)
        possible_keys = [each.string for each in filter(lambda x: x is not None, map(prog.search, self.keys()))]
        return {k: self.__getitem__(k) for k in possible_keys}


if __name__ == "__main__":
    # todo 根据value的特征获取指定key
    test = SearchableDict({"name": 1, "company_name": 123})
    print(test.search(r"企业名称|单位名称"))
