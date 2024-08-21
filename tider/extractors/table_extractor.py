from bs4 import Tag


class HtmlTableExtractor:

    def __init__(self, table: Tag, transformer=None):
        self._table = table
        self._transformer = transformer or str
        self._output = []

    def extract(self):
        row_idx = col_idx = 0
        for tr in self._table.find_all('tr'):
            min_row_span = 1
            for cell in tr.find_all(lambda x: x.name in ('th', 'td')):
                row_span = int(cell.get('rowspan', 1))
                min_row_span = min(min_row_span, row_span)

                col_span = int(cell.get('colspan', 1))

                # find the right index
                while True:
                    if self._check_cell_validity(row_idx, col_idx):
                        break
                    col_idx += 1

                self._insert(row_idx, col_idx, row_span, col_span, self._transformer(cell.get_text()))

                # update col_idx
                col_idx += col_span

            # update row_idx
            row_idx += min_row_span
            col_idx = 0
        return self._output

    def _check_cell_validity(self, i, j):
        """
        check if a cell (i, j) can be put into self._output
        """
        if i >= len(self._output):
            return True
        if j >= len(self._output[i]):
            return True
        if self._output[i][j] is None:
            return True
        return False

    def _insert(self, i, j, height, width, val):
        for ii in range(i, i+height):
            for jj in range(j, j+width):
                self._insert_cell(ii, jj, val)

    def _insert_cell(self, i, j, val):
        while i >= len(self._output):
            self._output.append([])
        while j >= len(self._output[i]):
            self._output[i].append(None)

        if self._output[i][j] is None:
            self._output[i][j] = val
