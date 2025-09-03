from tider import Response, Item
from tider.spiders import Spider
from tider.extractors.link_extractor import LinkExtractor
from tider.extractors.article_extractor import ArticleExtractor
from tider.extractors.table_extractor import HtmlTableExtractor


class ExtractSpider(Spider):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.article_extractor = ArticleExtractor()
        self.link_extractor = LinkExtractor(
            ignored_file_types=('Possibly dangerous files', 'Video files'), file_only=True, unique=True)
        self.table_extractor = HtmlTableExtractor()

    def parse(self, response: Response):
        article = self.article_extractor.extract_article(response)
        dummy_resp = Response.dummy(text=article.content, url=response.url, encoding='utf-8')
        tables = [
            {'title': table.title, 'content': table.content, 'orientation': table.orientation}
            for table in self.table_extractor.extract_tables(dummy_resp)
        ]
        dummy_resp.close()
        attachments = []
        if response.selector.type == 'html':
            attachments = [link.to_dict() for link in self.link_extractor.extract_links(response)]
        yield Item.from_dict(dict(
            url=response.url,
            title=article.title,
            content=article.content,
            publish_date=article.publish_date,
            author=article.author,
            tables=tables,
            attachments=attachments
        ))
