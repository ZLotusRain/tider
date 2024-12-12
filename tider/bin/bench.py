import click
import json

from tider.spiders import Spider
from tider.network import Request


class _BenchSpider(Spider):
    """A spider that crawls the specific url for 10k times"""
    name = 'Benchmark'
    baseurl = None

    def __init__(self, url, times, xpath=None, proxies=None, **kwargs):
        super().__init__(**kwargs)
        self.baseurl = url
        self.times = times
        self.xpath = xpath
        self.proxies = proxies

    def start_requests(self, **kwargs):
        for idx in range(self.times):
            yield Request(url=self.baseurl.replace('%idx', str(idx)), proxies=self.proxies)

    def parse(self, response):
        if self.xpath:
            response = response.xpath(query=self.xpath)
        self.logger.info(f'Crawled {response.url}: {response}')


@click.command()
@click.option('--url',
              type=str,
              default='https://www.baidu.com?index=%idx',
              help='Url for polling.')
@click.option('--times',
              type=int,
              default=10000,
              help="Polling times.")
@click.option('--xpath',
              type=str,
              help="Xpath to extract.")
@click.option('--proxies',
              type=str,
              help="Request proxies")
@click.pass_context
def bench(ctx, url, times, xpath=None, proxies=None):
    app = ctx.obj.app
    crawler = app.Crawler(_BenchSpider, debug=True)
    if proxies:
        proxies = json.loads(proxies)
    crawler.crawl(url=url, times=times, xpath=xpath, proxies=proxies)
    ctx.exit(crawler.exitcode)
