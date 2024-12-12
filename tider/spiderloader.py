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
        self._schemas = settings.getdict('SPIDER_SCHEMAS').copy()
        self._schemas = self._schemas or {'default': []}
        if 'default' not in self._schemas:
            self._schemas['default'] = []
        self._spider_modules = settings.getlist('SPIDER_MODULES').copy()
        self._schemas['default'].append({'SPIDER_MODULES': self._spider_modules})

        self._spiders = defaultdict()
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
            name = spider_cls.name if schema == 'default' else f'{schema}.{spider_cls.name}'

            custom_settings = dict(custom_settings or {})
            spider_cls.custom_settings = spider_cls.custom_settings or {}
            custom_settings.update(spider_cls.custom_settings)
            spider_cls.custom_settings = custom_settings

            self._found[name].append((schema, module.__name__, spider_cls.__name__))
            self._spiders[name] = spider_cls

    def _load_all_spiders(self):
        # may load the same spider multiple times
        for schema in self._schemas:
            sources = self._schemas[schema].copy()
            for source in sources:
                spider_modules = source.pop('SPIDER_MODULES')
                if isinstance(spider_modules, str):
                    spider_modules = spider_modules.split(',')
                spider_modules = list(spider_modules)
                for each in spider_modules:
                    try:
                        for module in walk_modules(each):
                            self._load_spiders(module, source, schema=schema)
                    except ImportError:
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
        spider_name = spider_name if schema == 'default' else f'{schema}.{spider_name}'
        try:
            return self._spiders[spider_name]
        except KeyError:
            for schema in self._schemas:
                if f'{schema}.{spider_name}' in self._spiders:
                    raise KeyError(f"Spider not found: {spider_name}, maybe you meant `{schema}.{spider_name}`?")
            raise KeyError(f"Spider not found: {spider_name}")

    def list(self):
        """
        Return a list with the names and schemas of all spiders available in the project.
        """
        return list(self._spiders.keys())
