import click

from tider import Tider
from tider.spiders import Spider
from tider.network import Request


class _BenchSpider(Spider):
    """A spider that crawls Baidu for 10k times"""
    name = 'baidu_benchmark'
    baseurl = 'https://www.baidu.com'

    def start_requests(self, **kwargs):
        for idx in range(10000):
            yield Request(url=f'{self.baseurl}?index={idx}')

    def parse(self, response):
        self.logger.info(f'Crawled {response.url}: {response}')


@click.command()
@click.pass_context
def bench(ctx):
    tider = Tider(spider=_BenchSpider, settings=ctx.obj.settings)
    tider.crawl()
