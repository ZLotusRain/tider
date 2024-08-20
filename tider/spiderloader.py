import warnings
from collections import defaultdict

from tider.settings import BaseSettings
from tider.utils.misc import symbol_by_name, walk_modules
from tider.utils.spider import iter_spider_classes, get_spider_name


class SpiderSettings(BaseSettings):

    def __init__(self, values=None, priority='project'):
        # Do not pass kwarg values here. We don't want to promote user-defined
        # dicts, and we want to update, not replace, default dicts with the
        # values given by the user
        super().__init__()
        # Promote default dictionaries to BaseSettings instances for per-key
        # priorities
        for name, val in self.items():
            if isinstance(val, dict):
                self.set(name, BaseSettings(val, 'default'), 'default')
        self.update(values, priority)


class SpiderLoader:
    """
    SpiderLoader is a class which locates and loads spiders
    in a Tider project.
    """
    def __init__(self, settings, schema=None):
        self.settings = settings
        self._schema = []
        schemas = self.settings.get('SPIDER_SCHEMAS', {})
        schemas and self.settings.pop('SPIDER_SCHEMAS')
        if not schema:
            for schema in schemas.values():
                self._schema.extend(schema)  # spider specific settings
        else:
            self._schema = schemas[schema]
        self._spiders = {}
        self._found = defaultdict(list)
        self._load_all_spiders()

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
                "There are several spiders with the same name:\n\n"
                f"{dupes_string}\n\n  This can cause unexpected behavior.",
                category=UserWarning,
            )

    def _load_spiders(self, module, settings):
        for spider_cls in iter_spider_classes(module):
            spider_name = get_spider_name(spider_cls, settings)
            self._found[spider_name].append((module.__name__, spider_cls.__name__))
            self._spiders[spider_name] = (spider_cls, settings)

    def _load_all_spiders(self):
        if not self._schema:
            self._schema.append(self.settings.copy())
        for spider_settings in self._schema:
            settings = SpiderSettings(spider_settings, priority='project')
            spider_module = spider_settings.get("SPIDER_MODULE")
            if spider_module:
                for module in walk_modules(spider_module):
                    self._load_spiders(module, settings)
            elif spider_settings.get("SPIDER"):
                spider_cls = symbol_by_name(spider_settings["SPIDER"])
                spider_name = get_spider_name(spider_cls, settings)
                self._found[spider_name].append((spider_cls.__module__, spider_cls.__name__))
                self._spiders[spider_name] = (spider_cls, settings)
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

    def load_settings(self, spider_name):
        try:
            return self._spiders[spider_name][1]
        except KeyError:
            raise KeyError(f"Spider not found: {spider_name}")

    def list(self):
        """
        Return a list with the names of all spiders available in the project.
        """
        return list(self._spiders.keys())
