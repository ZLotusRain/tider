import warnings
import traceback
from collections import defaultdict

from tider.utils.spider import iter_spider_classes
from tider.utils.misc import walk_modules, symbol_by_name


class SpiderLoader:
    """
    SpiderLoader is a class which locates and loads spiders
    in a Tider project.
    """
    def __init__(self, settings, schema=None):
        self.schema = schema
        self.warn_only = settings.getbool('SPIDER_LOADER_WARN_ONLY')
        self.spider_settings = []
        self._extract_spider_settings(settings)

        self._spiders = {}
        self._found = defaultdict(list)
        self._load_all_spiders()

    def _extract_spider_settings(self, settings):
        spider_schemas = settings.getdict('SPIDER_SCHEMAS')
        spider_modules = settings.getlist('SPIDER_MODULES')
        spider = settings.get('SPIDER')
        if not self.schema:
            # load all spiders
            for spider_schema in spider_schemas.values():
                self.spider_settings.extend(spider_schema)  # spider custom settings
            self.spider_settings.append({'SPIDER_MODULES': spider_modules})
            self.spider_settings.append({'SPIDER': spider})
        else:
            try:
                # load spider settings in specific schema
                self.spider_settings = spider_schemas[self.schema]
            except KeyError:
                # load spiders in base settings when no schema provided
                self.spider_settings.append({'SPIDER_MODULES': spider_modules})
                self.spider_settings.append({'SPIDER': spider})
        spider_schemas and settings.pop('SPIDER_SCHEMAS')
        spider_modules and settings.pop('SPIDER_MODULES')
        spider and settings.pop('SPIDER')

    def _check_name_duplicates(self):
        dupes = []
        for name, locations in self._found.items():
            dupes.extend([
                f"  {cls} named {name!r} (in {mod})"
                for mod, cls in locations
                if len(locations) > 1
            ])

        if dupes:
            dupes_string = "\n\n".join(dupes)
            warnings.warn(
                f"There are several spiders with the same name in schema {self.schema}:\n\n"
                f"{dupes_string}\n\n  This can cause unexpected behavior.",
                category=UserWarning,
            )

    def _load_spiders(self, module, settings):
        for spider_cls in iter_spider_classes(module):
            spider_cls.name = spider_cls.name or spider_cls.__name__
            spider_cls.custom_settings = spider_cls.custom_settings or {}
            spider_cls.custom_settings.update(settings)
            self._found[spider_cls.name].append((module.__name__, spider_cls.__name__))
            self._spiders[spider_cls.name] = spider_cls

    def _load_all_spiders(self):
        for spider_setting in self.spider_settings:
            names = spider_setting.get('SPIDER_MODULES')
            if isinstance(names, str):
                names = names.split(',')
            for name in names or []:
                try:
                    for module in walk_modules(name):
                        self._load_spiders(module, spider_setting)
                except ImportError:
                    if self.warn_only:
                        warnings.warn(
                            f"\n{traceback.format_exc()}Could not load spiders "
                            f"from module '{name}'. "
                            "See above traceback for details.",
                            category=RuntimeWarning,
                        )
                    else:
                        raise
            if spider_setting.get('SPIDER'):
                spider_cls = symbol_by_name(spider_setting.get('SPIDER'))
                spider_cls.name = spider_cls.name or spider_cls.__name__
                spider_cls.custom_settings = spider_cls.custom_settings or {}
                spider_cls.custom_settings.update(spider_setting)
                self._found[spider_cls.name].append((spider_cls.__module__, spider_cls.__name__))
                self._spiders[spider_cls.name] = spider_cls
        self._check_name_duplicates()

    def load(self, spider_name):
        """
        Return the Spider class for the given spider name. If the spider
        name is not found, raise a KeyError.
        """
        try:
            return self._spiders[spider_name]
        except KeyError:
            raise KeyError(f"Spider not found: {spider_name}")

    def list(self):
        """
        Return a list with the names of all spiders available in the project.
        """
        return list(self._spiders.keys())
