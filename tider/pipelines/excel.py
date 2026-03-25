import json
import pathlib
import openpyxl
from openpyxl.cell import Cell
from collections import defaultdict

from tider.item import Item
from tider.utils.log import get_logger

logger = get_logger(__name__)


class TitleRow:

    __slots__ = ("name", "titles")

    def __init__(self, name: str, titles: list):
        self.name = name
        self.titles = titles

    def __eq__(self, other):
        if isinstance(other, TitleRow):
            return self.name == other.name and sorted(self.titles) == sorted(other.titles)
        return False

    def __hash__(self) -> int:
        titles = sorted(self.titles)
        hashed = hash("")
        for each in titles:
            hashed ^= hash(each)
        return hashed ^ hash(self.name)


class ExcelPipeline:

    def __init__(self, crawler, output):
        self.crawler = crawler
        self.wb = openpyxl.Workbook()
        self.sheets = defaultdict(list)
        self.output = pathlib.Path(output)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler=crawler, output=crawler.settings["EXCEL_PIPELINE_OUTPUT"])

    def process_item(self, item):
        if not isinstance(item, Item):
            item = Item.from_dict(item)
        if item.discarded:
            return
        name = item.__class__.__name__

        titles = []
        values = []
        for key, value in item.items():
            if not isinstance(value, str):
                try:
                    value = json.dumps(value, ensure_ascii=False)
                except json.JSONDecodeError:
                    pass
            values.append(value)
            titles.append(key)
        row = TitleRow(name=name, titles=titles)
        self.sheets[row].append(values)
        logger.info(f'Obtained item: {item.jsonify()}')

    def close(self):
        if not self.sheets:
            self.wb.close()
            return
        row_idx = 0
        for sheet_idx, sheet in enumerate(self.sheets):
            titles = sheet.titles
            if sheet_idx == 0:
                ws = self.wb.worksheets[sheet_idx]
            else:
                ws = self.wb.create_sheet()
            ws.insert_rows(idx=0, amount=1)
            for col_idx, content in enumerate(titles, 1):
                cell = Cell(ws, row=row_idx, column=col_idx, value=content)
                ws._cells[(row_idx, col_idx)] = cell
            for row in self.sheets[sheet]:
                ws.append(row)

        output = self.output
        if output.is_dir():
            output = output / (self.crawler.spidername + '.xlsx')
        self.wb.save(output)
        self.wb.close()
