import sys
import os
import click
from importlib import import_module

from tider.utils.spider import iter_spider_classes


def _import_file(filepath):
    abspath = os.path.abspath(filepath)
    dirname, file = os.path.split(abspath)
    fname, fext = os.path.splitext(file)
    if fext not in ('.py', '.pyw'):
        raise ValueError(f"Not a Python source file: {abspath}")
    if dirname:
        sys.path = [dirname] + sys.path
    try:
        module = import_module(fname)
    finally:
        if dirname:
            sys.path.pop(0)
    return module


@click.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True
    }
)
@click.option('--filename')
@click.pass_context
def runspider(ctx, filename, **kwargs):
    """Run the spider defined in the given file"""
    try:
        module = _import_file(filename)
    except (ImportError, ValueError) as e:
        raise click.UsageError(f"Unable to load {filename!r}: {e}\n")
    spclasses = list(iter_spider_classes(module))
    if not spclasses:
        raise click.UsageError(f"No spider found in file: {filename}\n")
    spidercls = spclasses.pop()
    app = ctx.obj.app
    crawler = app.Crawler(spidercls)
    crawler.crawl()
    ctx.exit(crawler.exitcode)
