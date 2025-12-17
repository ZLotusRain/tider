import json
import openpyxl
from openpyxl.cell import Cell
from collections import defaultdict

from tider.item import Item
from tider.utils.log import get_logger

logger = get_logger(__name__)


class TitleRow:

    __slots__ = ["titles"]

    def __init__(self, titles: list):
        self.titles = titles

    def __hash__(self) -> int:
        titles = sorted(self.titles)
        hashed = hash("")
        for each in titles:
            hashed ^= hash(each)
        return hashed


class ExcelPipeline:

    def __init__(self, output):
        self.wb = openpyxl.Workbook()
        self.sheets = defaultdict(list)
        self.output = output

    @classmethod
    def from_settings(cls, settings):
        return cls(output=settings["EXCEL_PIPELINE_OUTPUT"])

    def process_item(self, item):
        data = item
        if isinstance(item, Item):
            if item.discarded:
                return
            data = dict(item)
        if not isinstance(data, dict):
            return
        titles = list(data.keys())
        row = TitleRow(titles)
        value_row = []
        for each in list(data.values()):
            if not isinstance(each, str):
                try:
                    each = json.dumps(each, ensure_ascii=False)
                except json.JSONDecodeError:
                    each = str(each)
            value_row.append(each)
        self.sheets[row].append(value_row)
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
        self.wb.save(self.output)
        self.wb.close()
