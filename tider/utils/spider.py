import inspect


def get_spider_name(spider_cls, settings=None):
    name_from_settings = None
    if settings:
        name_from_settings = settings.get("SPIDER_NAME")
    spider_name = (getattr(spider_cls, "name", None)
                   or name_from_settings
                   or spider_cls.__name__)
    return spider_name


def iter_spider_classes(module):
    """Return an iterator over all spider classes defined in the given module
    that can be instantiated (i.e. which have name)
    """
    # this needs to be imported here until get rid of the spider manager
    # singleton in tider.spiders.spider
    from tider.spiders import Spider

    for obj in vars(module).values():
        if (
            inspect.isclass(obj)
            and issubclass(obj, Spider)
            and obj.__module__ == module.__name__
            # and getattr(obj, 'name', None)
        ):
            yield obj
