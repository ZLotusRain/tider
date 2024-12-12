import warnings
import traceback
from collections import defaultdict

from tider.utils.misc import walk_modules
from tider.utils.spider import iter_spider_classes


class SpiderLoader:
    """
    SpiderLoader is a class which locates and loads spiders
    in a Tider project.
    """
    def __init__(self, settings):
        self._spider_schemas = settings.getdict('SPIDER_SCHEMAS').copy() or {'default': []}
        if 'default' not in self._spider_schemas:
            self._spider_schemas['default'] = []
        self._spider_schemas['default'].append({'SPIDER_MODULES': settings.getlist('SPIDER_MODULES').copy()})

        self._schemas = defaultdict(dict)
        self.warn_only = settings.getbool('SPIDER_LOADER_WARN_ONLY')
        self._found = defaultdict(list)
        self._load_all_spiders()

    def _check_name_duplicates(self):
        dupes = []
        for name, locations in self._found.items():
            dupes.extend([
                f"  {cls} named {name!r}[{schema}] (in {mod})"
                for schema, mod, cls in locations
                if len(locations) > 1
            ])

        if dupes:
            dupes_string = "\n\n".join(dupes)
            warnings.warn(
                f"There are several spiders with the same name:\n\n"
                f"{dupes_string}\n\n  This can cause unexpected behavior.",
                category=UserWarning,
            )

    def _load_spiders(self, module, custom_settings=None, schema='default'):
        for spider_cls in iter_spider_classes(module):
            spider_cls.name = spider_cls.name or spider_cls.__name__
            name = spider_cls.name

            custom_settings = dict(custom_settings or {})
            spider_cls.custom_settings = spider_cls.custom_settings or {}
            # spider is higher than project settings
            custom_settings.update(spider_cls.custom_settings)
            spider_cls.custom_settings = custom_settings

            found_name = spider_cls.name if schema == 'default' else f'{schema}.{spider_cls.name}'
            self._found[found_name].append((schema, module.__name__, spider_cls.__name__))
            self._schemas[schema][name] = spider_cls

    def _load_all_spiders(self):
        # may load the same spider multiple times
        for schema in self._spider_schemas:
            spider_settings = self._spider_schemas[schema].copy()
            for spider_setting in spider_settings:
                spider_modules = spider_setting.pop('SPIDER_MODULES')
                if isinstance(spider_modules, str):
                    spider_modules = spider_modules.split(',')
                spider_modules = list(spider_modules)
                for each in spider_modules:
                    try:
                        for module in walk_modules(each):
                            self._load_spiders(module, spider_setting, schema=schema)
                    except (ImportError, SyntaxError):
                        if self.warn_only:
                            warnings.warn(
                                f"\n{traceback.format_exc()}Could not load spiders "
                                f"from module '{each}'. "
                                "See above traceback for details.",
                                category=RuntimeWarning,
                            )
                        else:
                            raise
        self._check_name_duplicates()

    def load(self, spider_name, schema='default'):
        """
        Return the Spider class for the given spider name. If the spider
        name is not found, raise a KeyError.
        """
        schema = schema or 'default'
        hint_name = spider_name if schema == 'default' else f'{schema}.{spider_name}'
        try:
            return self._schemas[schema][spider_name]
        except KeyError:
            for schema in self._schemas:
                if spider_name in self._schemas[schema]:
                    raise KeyError(f"Spider not found: {hint_name}, maybe you meant `{schema}.{spider_name}`?")
            raise KeyError(f"Spider not found: {hint_name}")

    def list(self, schema='default'):
        """
        Return a list with the names of all spiders available in the specified schema.
        """
        return list(self._schemas[schema].keys())

    def list_all(self):
        """
        Return a dict with the names and schemas of all spiders available in the project.
        """
        return {schema: list(self._schemas[schema].keys()) for schema in self._schemas}
