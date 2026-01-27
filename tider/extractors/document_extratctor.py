import re
import uuid
import math
import base64
import os
import sys
import tempfile
import inspect
import shlex
import subprocess
from datetime import time
from io import BytesIO
from threading import RLock
from contextlib import suppress

from typing import Mapping, Any, Union, BinaryIO
from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag, Comment, Doctype

from tider import Request, Promise
from tider.platforms import IS_WINDOWS
from tider.utils.file import fix_xlsx_styles
from tider.utils.functional import evaluate_callable, noop
from tider.utils.misc import arg_to_iter
from tider.exceptions import ImproperlyConfigured

_magika_dependency_exc_info = None
try:
    from magika import Magika
except ImportError:
    Magika = object
    _magika_dependency_exc_info = sys.exc_info()

_markitdown_dependency_exc_info = None
try:
    import six
    from markdownify import (
        should_remove_whitespace_inside,
        should_remove_whitespace_outside,
        re_html_heading,
        re_extract_newlines
    )
    from markitdown import StreamInfo, DocumentConverterResult
    from markitdown.converters._markdownify import _CustomMarkdownify as BaseMarkdownify
    from markitdown.converters import HtmlConverter as BaseHtmlConverter, PdfConverter
    from markitdown.converter_utils.docx.pre_process import pre_process_docx
except ImportError:
    # Preserve the error and stack trace for later
    StreamInfo = BaseHtmlConverter = BaseMarkdownify = PdfConverter = DocumentConverterResult = object
    should_remove_whitespace_inside = should_remove_whitespace_outside = pre_process_docx = noop()
    re_html_heading = re_extract_newlines = None
    _markitdown_dependency_exc_info = sys.exc_info()

_docx_dependency_exc_info = None
try:
    import docx
    from docx.table import Table
    from docx.oxml.ns import qn
    from docx.text.paragraph import Paragraph
except ImportError:
    _docx_dependency_exc_info = sys.exc_info()

_win32_dependency_exc_info = None
try:
    import win32com.client
except ImportError:
    _win32_dependency_exc_info = sys.exc_info()

_mammoth_dependency_exc_info = None
try:
    import mammoth
except ImportError:
    _mammoth_dependency_exc_info = sys.exc_info()

_excel_dependency_exc_info = None
try:
    import numpy as np
    from pandas import ExcelFile as BaseExcelFile
    from pandas.io.excel._xlrd import XlrdReader as BaseXlrdReader
    from pandas.io.excel._openpyxl import OpenpyxlReader as BaseOpenpyxlReader
    from pandas.io.excel._calamine import CalamineReader
    from pandas.io.excel._odfreader import ODFReader
    from pandas.io.excel._pyxlsb import PyxlsbReader
    from pandas._typing import Scalar
except ImportError:
    BaseExcelFile = BaseXlrdReader = BaseOpenpyxlReader = object
    _excel_dependency_exc_info = sys.exc_info()

_xlrd_dependency_exc_info = None
try:
    import xlrd
except ImportError:
    _xlrd_dependency_exc_info = sys.exc_info()

_openpyxl_dependency_exc_info = None
try:
    import openpyxl
except ImportError:
    _openpyxl_dependency_exc_info = sys.exc_info()

_pymupdf_dependency_exc_info = None
try:
    import pymupdf
except ImportError:
    _pymupdf_dependency_exc_info = sys.exc_info()


class Markdownify(BaseMarkdownify):

    def __init__(self, ignored_tags=None, **options):
        super().__init__(**options)
        self.ignored_tags = arg_to_iter(ignored_tags)

    def process_tag(self, node, parent_tags=None):
        # For the top-level element, initialize the parent context with an empty set.
        if parent_tags is None:
            parent_tags = set()

        # Collect child elements to process, ignoring whitespace-only text elements
        # adjacent to the inner/outer boundaries of block elements.
        should_remove_inside = should_remove_whitespace_inside(node)

        def _can_ignore(el):
            if isinstance(el, Tag):
                # Tags are always processed.
                if el.name in self.ignored_tags:
                    return True
                return False
            elif isinstance(el, (Comment, Doctype)):
                # Comment and Doctype elements are always ignored.
                # (subclasses of NavigableString, must test first)
                return True
            elif isinstance(el, NavigableString):
                if six.text_type(el).strip() != '':
                    # Non-whitespace text nodes are always processed.
                    return False
                elif should_remove_inside and (not el.previous_sibling or not el.next_sibling):
                    # Inside block elements (excluding <pre>), ignore adjacent whitespace elements.
                    return True
                elif should_remove_whitespace_outside(el.previous_sibling) or should_remove_whitespace_outside(el.next_sibling):
                    # Outside block elements (including <pre>), ignore adjacent whitespace elements.
                    return True
                else:
                    return False
            elif el is None:
                return True
            else:
                raise ValueError('Unexpected element type: %s' % type(el))

        children_to_convert = [el for el in node.children if not _can_ignore(el)]

        # Create a copy of this tag's parent context, then update it to include this tag
        # to propagate down into the children.
        parent_tags_for_children = set(parent_tags)
        parent_tags_for_children.add(node.name)

        # if this tag is a heading or table cell, add an '_inline' parent pseudo-tag
        if (
            re_html_heading.match(node.name) is not None  # headings
            or node.name in {'td', 'th'}  # table cells
        ):
            parent_tags_for_children.add('_inline')

        # if this tag is a preformatted element, add a '_noformat' parent pseudo-tag
        if node.name in {'pre', 'code', 'kbd', 'samp'}:
            parent_tags_for_children.add('_noformat')

        # Convert the children elements into a list of result strings.
        child_strings = [
            self.process_element(el, parent_tags=parent_tags_for_children)
            for el in children_to_convert
        ]

        # Remove empty string values.
        child_strings = [s for s in child_strings if s]

        # Collapse newlines at child element boundaries, if needed.
        if node.name == 'pre' or node.find_parent('pre'):
            # Inside <pre> blocks, do not collapse newlines.
            pass
        else:
            # Collapse newlines at child element boundaries.
            updated_child_strings = ['']  # so the first lookback works
            for child_string in child_strings:
                # Separate the leading/trailing newlines from the content.
                leading_nl, content, trailing_nl = re_extract_newlines.match(child_string).groups()

                # If the last child had trailing newlines and this child has leading newlines,
                # use the larger newline count, limited to 2.
                if updated_child_strings[-1] and leading_nl:
                    prev_trailing_nl = updated_child_strings.pop()  # will be replaced by the collapsed value
                    num_newlines = min(2, max(len(prev_trailing_nl), len(leading_nl)))
                    leading_nl = '\n' * num_newlines

                # Add the results to the updated child string list.
                updated_child_strings.extend([leading_nl, content, trailing_nl])

            child_strings = updated_child_strings

        # Join all child text strings into a single string.
        text = ''.join(child_strings)

        # apply this tag's final conversion function
        convert_fn = self.get_conv_fn_cached(node.name)
        if convert_fn is not None:
            text = convert_fn(node, text, parent_tags=parent_tags)

        return text


class HtmlConverter(BaseHtmlConverter):

    def convert(
        self,
        file_stream: BinaryIO,
        stream_info: StreamInfo,
        **kwargs: Any,  # Options to pass to the converter
    ) -> DocumentConverterResult:
        # Parse the stream
        encoding = "utf-8" if stream_info.charset is None else stream_info.charset
        soup = BeautifulSoup(file_stream, "html.parser", from_encoding=encoding)

        # Remove javascript and style blocks
        for script in soup(["script", "style"]):
            script.extract()

        # Print only the main content
        body_elm = soup.find("body")
        if body_elm:
            webpage_text = Markdownify(**kwargs).convert_soup(body_elm)
        else:
            webpage_text = Markdownify(**kwargs).convert_soup(soup)

        assert isinstance(webpage_text, str)

        # remove leading and trailing \n
        webpage_text = webpage_text.strip()

        return DocumentConverterResult(
            markdown=webpage_text,
            title=None if soup.title is None else soup.title.string,
        )


class XlrdReader(BaseXlrdReader):

    ILLEGAL_EXCEL_CHARACTERS_RE = re.compile(r'[\000-\010]|[\013-\014]|[\016-\037]')

    def get_sheet_data(
            self, sheet, file_rows_needed: int | None = None
    ) -> list[list[Scalar]]:
        from xlrd import (
            XL_CELL_BOOLEAN,
            XL_CELL_DATE,
            XL_CELL_ERROR,
            XL_CELL_NUMBER,
            xldate,
        )

        epoch1904 = self.book.datemode

        def _parse_cell(cell_contents, cell_typ):
            """
            converts the contents of the cell into a pandas appropriate object
            """
            if cell_typ == XL_CELL_DATE:
                # Use the newer xlrd datetime handling.
                try:
                    cell_contents = xldate.xldate_as_datetime(cell_contents, epoch1904)
                except OverflowError:
                    return cell_contents

                # Excel doesn't distinguish between dates and time,
                # so we treat dates on the epoch as times only.
                # Also, Excel supports 1900 and 1904 epochs.
                year = (cell_contents.timetuple())[0:3]
                if (not epoch1904 and year == (1899, 12, 31)) or (
                        epoch1904 and year == (1904, 1, 1)
                ):
                    cell_contents = time(
                        cell_contents.hour,
                        cell_contents.minute,
                        cell_contents.second,
                        cell_contents.microsecond,
                    )

            elif cell_typ == XL_CELL_ERROR:
                cell_contents = np.nan
            elif cell_typ == XL_CELL_BOOLEAN:
                cell_contents = bool(cell_contents)
            elif cell_typ == XL_CELL_NUMBER:
                # GH5394 - Excel 'numbers' are always floats
                # it's a minimal perf hit and less surprising
                if math.isfinite(cell_contents):
                    # GH54564 - don't attempt to convert NaN/Inf
                    val = int(cell_contents)
                    if val == cell_contents:
                        cell_contents = val
            return cell_contents

        data = []

        nrows = sheet.nrows
        if file_rows_needed is not None:
            nrows = min(nrows, file_rows_needed)
        invalid_rows = 0
        for i in range(nrows):
            if invalid_rows > 100:
                break
            row = [
                _parse_cell(value, typ)
                for value, typ in zip(sheet.row_values(i), sheet.row_types(i))
            ]
            for idx in range(len(row)):
                if isinstance(row[idx], str):
                    row[idx] = self.ILLEGAL_EXCEL_CHARACTERS_RE.sub(r'', row[idx])
            if all([x is None or not str(x).strip() or x is np.nan for x in row]):
                invalid_rows += 1
            max_idx = 0
            continuous_none = 0
            for idx in range(len(row)):
                if continuous_none > 100:
                    break
                if row[idx] is not None and row[idx] is not np.nan:
                    max_idx = idx
                    continuous_none = 0
                continuous_none += 1
            row = row[:max_idx]
            data.append(row)

        return data


class OpenpyxlReader(BaseOpenpyxlReader):

    ILLEGAL_EXCEL_CHARACTERS_RE = re.compile(r'[\000-\010]|[\013-\014]|[\016-\037]')

    def get_sheet_data(
        self, sheet, file_rows_needed: int | None = None
    ) -> list[list[Scalar]]:
        if self.book.read_only:
            sheet.reset_dimensions()

        data: list[list[Scalar]] = []
        last_row_with_data = -1
        invalid_rows = 0
        for row_number, row in enumerate(sheet.rows):
            if invalid_rows > 100:
                break
            converted_row = []
            for cell in row:
                converted_cell = self._convert_cell(cell)
                if isinstance(converted_cell, str):
                    converted_cell = self.ILLEGAL_EXCEL_CHARACTERS_RE.sub(r'', converted_cell)
                converted_row.append(converted_cell)
            while converted_row and (converted_row[-1] == "" or converted_row[-1] is None):
                # trim trailing empty elements
                converted_row.pop()
            if all([x is None or not str(x).strip() or x is np.nan for x in row]):
                invalid_rows += 1
            if converted_row:
                last_row_with_data = row_number
            data.append(converted_row)
            if file_rows_needed is not None and len(data) >= file_rows_needed:
                break

        # Trim trailing empty rows
        data = data[: last_row_with_data + 1]

        if len(data) > 0:
            # extend rows to max width
            max_width = max(len(data_row) for data_row in data)
            if min(len(data_row) for data_row in data) < max_width:
                empty_cell: list[Scalar] = [""]
                data = [
                    data_row + (max_width - len(data_row)) * empty_cell
                    for data_row in data
                ]

        return data


class ExcelFile(BaseExcelFile):

    _engines: Mapping[str, Any] = {
        "xlrd": XlrdReader,
        "openpyxl": OpenpyxlReader,
        "odf": ODFReader,
        "pyxlsb": PyxlsbReader,
        "calamine": CalamineReader,
    }


class XlsxParser:

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._html_converter = HtmlConverter()

    def extract(self, content, to_markdown=False, ignored_tags=None, **kwargs):
        if _excel_dependency_exc_info:
            raise ImproperlyConfigured(
                "You need to install the pandas library to extract excel files."
            ) from _excel_dependency_exc_info[
                1
            ].with_traceback(  # type: ignore[union-attr]
                _excel_dependency_exc_info[2]
            )

        if _openpyxl_dependency_exc_info:
            raise ImproperlyConfigured(
                "You need to install the openpyxl library to extract .xlsx files."
            ) from _openpyxl_dependency_exc_info[
                1
            ].with_traceback(  # type: ignore[union-attr]
                _openpyxl_dependency_exc_info[2]
            )

        if content.startswith((b'<!DOCTYPE', b'<?xml')):
            if to_markdown:
                return self._html_converter.convert_string(
                    content.decode('utf-8'),
                    ignored_tags=ignored_tags,
                    **kwargs
                ).markdown.strip()
            return content.decode('utf-8')

        document = ""
        file = fix_xlsx_styles(content)
        try:
            excel = ExcelFile(file, engine="openpyxl")
            sheets = excel.parse(sheet_name=None)
            for s in sheets:
                html_content = sheets[s].to_html(index=False)
                if to_markdown:
                    document += f"## {s}\n"
                    document += (
                            self._html_converter.convert_string(
                                html_content, **kwargs
                            ).markdown.strip()
                            + "\n\n"
                    )
                else:
                    document += f"<h2>{s}<h2><br></br>"
                    document += html_content
        finally:
            file.close()
        return document


class XlsParser:

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._html_converter = HtmlConverter()

    def extract(self, content, to_markdown=False, ignored_tags=None, **kwargs):
        if _excel_dependency_exc_info:
            raise ImproperlyConfigured(
                "You need to install the pandas library to extract excel files."
            ) from _excel_dependency_exc_info[
                1
            ].with_traceback(  # type: ignore[union-attr]
                _excel_dependency_exc_info[2]
            )

        if _xlrd_dependency_exc_info:
            raise ImproperlyConfigured(
                "You need to install the xlrd library to extract .xls files."
            ) from _xlrd_dependency_exc_info[
                1
            ].with_traceback(  # type: ignore[union-attr]
                _xlrd_dependency_exc_info[2]
            )

        if content.startswith((b'<!DOCTYPE', b'<?xml')):
            if to_markdown:
                return self._html_converter.convert_string(
                    content.decode('utf-8'),
                    ignored_tags=ignored_tags,
                    **kwargs
                ).markdown.strip()
            return content.decode('utf-8')

        file = BytesIO(content)
        document = ""
        try:
            excel = ExcelFile(file, engine="xlrd")
            sheets = excel.parse(sheet_name=None)
            for s in sheets:
                html_content = sheets[s].to_html(index=False)
                if to_markdown:
                    document += f"## {s}\n"
                    document += (
                        self._html_converter.convert_string(
                            html_content, ignored_tags=ignored_tags, **kwargs
                        ).markdown.strip()
                        + "\n\n"
                    )
                else:
                    document += f"<h2>{s}<h2><br></br>"
                    document += html_content
        finally:
            file.close()
        return document


class WordParser:

    def __init__(self, parse_engine=None, libreoffice_executable=None, **kwargs):
        super().__init__(**kwargs)
        self.parse_engine = parse_engine or 'default'
        self.libreoffice_executable = libreoffice_executable
        self._engines = {
            "default": self._extract_by_docx,
            "mammoth": self._extract_by_mammoth,
        }

        self._html_converter = HtmlConverter()

    def _check_file_type(self, content):
        hex_header = content[:4].hex().upper()
        if hex_header == 'D0CF11E0':
            return "doc"
        elif hex_header == '504B0304':
            return "docx"
        raise ValueError('Not a valid word file')

    def extract(self, content, filetype=None, to_markdown=False, ignored_tags=None, **kwargs):
        filetype = filetype or self._check_file_type(content)
        if filetype == "doc":
            content = self._doc_to_docx(content)
        return self._engines[self.parse_engine](content, to_markdown=to_markdown, ignored_tags=ignored_tags, **kwargs)

    def _doc_to_docx(self, content):
        if IS_WINDOWS:
            return self._doc_to_docx_on_windows(content)
        return self._doc_to_docx_on_unix(content)

    def _doc_to_docx_on_unix(self, content):
        libreoffice_executable = self.libreoffice_executable or "/usr/lib64/libreoffice/program/soffice.bin"
        if not os.path.exists(libreoffice_executable):
            raise ImproperlyConfigured("You need to install the libreoffice suites to convert doc to docx.")

        tempdir = tempfile.gettempdir()
        unique_id = str(uuid.uuid4())
        path = f"{tempdir}/{unique_id}.doc"
        with open(path, "wb") as fw:
            fw.write(content)
        output_path = f"{tempdir}/{unique_id}.docx"
        user_installation_file = f"{tempdir}/LibO_{unique_id}"
        user_installation = f"file://{user_installation_file}"
        cmd = f"""
         {libreoffice_executable} -env:SingleAppInstance="false" -env:UserInstallation="{user_installation}" 
         --headless --convert-to docx --outdir {tempdir} {path}
         """
        try:
            args = shlex.split(cmd, posix=not IS_WINDOWS)
            subprocess.run(args, timeout=30, check=True)
            with open(output_path, "rb") as fo:
                return fo.read()
        finally:
            with suppress(FileNotFoundError):
                os.remove(path)
                os.remove(user_installation_file)
                os.remove(output_path)

    def _doc_to_docx_on_windows(self, content):
        if _win32_dependency_exc_info:
            raise ImproperlyConfigured(
                "You need to install the pywin32 library to convert doc to docx."
            ) from _win32_dependency_exc_info[
                1
            ].with_traceback(  # type: ignore[union-attr]
                _win32_dependency_exc_info[2]
            )

        tempdir = tempfile.gettempdir()
        unique_id = str(uuid.uuid4())
        path = f"{tempdir}/{unique_id}.doc"
        with open(path, "wb") as fw:
            fw.write(content)
        output_path = f"{tempdir}/{unique_id}.docx"
        try:
            word = win32com.client.DispatchEx("Word.Application")
            doc = word.Documents.Open(os.path.abspath(path))
            doc.SaveAs(os.path.abspath(output_path), FileFormat=16)  # 16 for wdFormatXMLDocument (.docx)
            doc.Close()
            word.Quit()
            with open(output_path, "rb") as fo:
                return fo.read()
        finally:
            with suppress(FileNotFoundError):
                os.remove(path)
                os.remove(output_path)

    def _extract_by_mammoth(self, content, to_markdown=False, ignored_tags=None, style_map=None, **kwargs):
        if _mammoth_dependency_exc_info:
            raise ImproperlyConfigured(
                "You need to install the mammoth library to use mammoth engine to extract .docx files."
            ) from _mammoth_dependency_exc_info[
                1
            ].with_traceback(  # type: ignore[union-attr]
                _mammoth_dependency_exc_info[2]
            )

        docx_file = BytesIO(content)
        try:
            if not to_markdown:
                result = mammoth.convert_to_html(docx_file, style_map=style_map)
                return result.value
            pre_process_stream = pre_process_docx(docx_file)
            return self._html_converter.convert_string(
                mammoth.convert_to_html(pre_process_stream, style_map=style_map).value,
                ignored_tags=ignored_tags,
                **kwargs,
            ).markdown
        finally:
            docx_file.close()

    def _extract_by_docx(self, content, to_markdown=False, ignored_tags=None, **kwargs):
        if _docx_dependency_exc_info:
            raise ImproperlyConfigured(
                "You need to install the python-docx library to extract .docx files."
            ) from _docx_dependency_exc_info[
                1
            ].with_traceback(  # type: ignore[union-attr]
                _docx_dependency_exc_info[2]
            )

        document = ""
        image_sha1s = set()
        stream = BytesIO(content)
        # https://github.com/python-openxml/python-docx/issues/1364
        # https://www.reddit.com/r/Python/comments/j0gl8t/psa_pythonlxml_memory_leaks_and_a_solution/?rdt=62683
        if to_markdown:
            stream = pre_process_docx(stream)
        try:
            docx_file = docx.Document(stream)
            # use document.element.xpath()/document.element.body.iterchildren() to iter all
            for el in docx_file.iter_inner_content():
                related_parts = el.part.related_parts
                if isinstance(el, Paragraph):
                    paragraph = self._extract_docx_paragraph(el, related_parts, image_sha1s)
                    document += paragraph
                elif isinstance(el, Table):
                    # text = f"<table>{el._element.xpath('string(.)')}<table/>"
                    html_table = self._extract_docx_table(el.table)
                    document += html_table
                document += '\n'
        finally:
            stream.close()
        if to_markdown:
            document = self._html_converter.convert_string(document, ignored_tags=ignored_tags, **kwargs).markdown
        return document

    def _extract_docx_paragraph(self, el, related_parts, image_sha1s=None):
        paragraph = ""
        # for inner in el.iter_inner_content():
        for wr in el._element.xpath('.//w:r'):
            paragraph += f"<p>{wr.text}</p>"
            w_drawings = wr.xpath('.//w:drawing')
            # https://github.com/python-openxml/python-docx/issues/650
            for w_drawing in w_drawings:
                embeds = w_drawing.xpath('.//a:blip/@r:embed')
                if not embeds:
                    continue
                embed = embeds[0]
                image_part = related_parts[embed]  # from docx.parts.image import ImagePart
                sha1 = image_part.sha1
                if sha1 in image_sha1s:
                    continue
                image_sha1s.add(sha1)
                # ext = image_part.image.ext  # 'jpg'
                content_type = image_part.content_type or ""  # 'image/jpeg'
                content_type = content_type.split("/")[-1] or "png"
                filename = image_part.filename  # 'image.jpg'
                # partname = image_part.partname  # '/word/media/image1.jpeg'
                image_content = image_part.blob
                encoded_src = base64.b64encode(image_content).decode()
                src = "data:{0};base64,{1}".format(content_type, encoded_src)
                paragraph += f"<img alt='{filename}' src='{src}'></img>"
        paragraph = f"<div>{paragraph}</div>"
        return paragraph

    def _extract_docx_table(self, table):
        grid = []
        html_table = ""
        for row_idx, row in enumerate(table.rows):
            # while index < len(row.cells):
            #     cell = row.cells[index]
            #     row_span = cell.grid_span
            #     index += row_span
            #     html_row += f"<td rowspan={row_span}>{cell.text}</td>"
            # html_table += f"<tr>{html_row}</tr>"
            cells = []
            cell_idx = 0
            for cell in row._element.tc_lst:
                tc_pr = cell.tcPr
                vMerge = tc_pr.find(qn('w:vMerge')) if tc_pr is not None else None
                if vMerge is not None:
                    val = vMerge.get(qn('w:val'))
                    if val == 'restart':
                        cells.append(cell)
                    else:
                        # continue
                        try:
                            prev_cell = grid[row_idx - 1][cell_idx]
                            cells.append(prev_cell)
                        except IndexError:
                            cells.append(cell)
                else:
                    cells.append(cell)
                grid_span = tc_pr.find(qn('w:gridSpan')) if tc_pr is not None else None
                if grid_span is not None:
                    span_val = int(grid_span.get(qn('w:val')))
                    # fill grid span
                    for _ in range(span_val - 1):
                        cells.append(cell)
                cell_idx += 1
            grid.append(cells)
        for row in grid:
            html_row = ""
            for cell in row:
                w_t = cell.xpath(".//w:t")
                text = w_t[0].text if w_t else ''
                html_row += f"<td>{text}</td>"
            html_table += f"<tr>{html_row}</tr>"
        html_table = f"<table>{html_table}</table>"
        return html_table


class PdfParser:

    def __init__(self, parse_engine=None, enable_ocr=False, page_func=None, context=None, callback=None, **kwargs):
        super().__init__(**kwargs)
        self.parse_engine = parse_engine or 'default'
        self._engines = {
            "default": self._extract,
            "markitdown": self._extract_by_markitdown
        }
        self.page_func = page_func
        self.context = context or {}
        self.callback = evaluate_callable(callback)

        self._enable_ocr = enable_ocr
        if enable_ocr:
            try:
                import pymupdf.layout
            except ImportError as exc:
                raise ImproperlyConfigured(
                    "You need to install the pymupdf-layout library to convert content to markdown."
                ) from exc
        self._lock = RLock()

    def extract(self, content, to_markdown=False, **kwargs):
        pdf_broken = False
        begin = content[0:20]
        if begin.find(rb'%PDF-1.') < 0:
            pdf_broken = True
        idx = content.rfind(rb'%%EOF')
        if idx < 0:
            pdf_broken = True
        # end = content[(0 if idx - 100 < 0 else idx - 100): idx + 5]
        # if not re.search(rb'startxref\s+\d+\s+%%EOF$', end):
        #     pdf_broken = True
        if pdf_broken:
            raise RuntimeError(f"Pdf has broken")
        return self._engines[self.parse_engine](content, to_markdown=to_markdown, **kwargs)

    def _extract(self, content, to_markdown=False, **kwargs):
        if _pymupdf_dependency_exc_info:
            raise ImproperlyConfigured(
                "You need to install the pymupdf library to process pdf files by default."
            ) from _pymupdf_dependency_exc_info[
                1
            ].with_traceback(  # type: ignore[union-attr]
                _pymupdf_dependency_exc_info[2]
            )

        pymupdf4llm = None
        if to_markdown:
            try:
                import pymupdf4llm
            except ImportError as exc:
                raise ImproperlyConfigured(
                    "You need to install the pymupdf4llm library to convert content to markdown."
                ) from exc

        # https://github.com/pymupdf/PyMuPDF/issues/1701#issuecomment-1117069672
        pdf_contents = {}
        requests = []
        with self._lock:
            pdf = pymupdf.Document(stream=content)  # don't add filetype here.
            # don't iter pdf.pages(): ValueError: bad start page number
            for idx in range(len(pdf)):
                page = pdf[idx]
                page_index = page.number
                # cdrawings = page.get_cdrawings()  # get_drawings() maybe consume up memory.
                if self.page_func:
                    processed = self.page_func(page, page_index)
                elif to_markdown:
                    new_pdf = pymupdf.open()
                    if 'widgets' in inspect.signature(new_pdf.insert_pdf).parameters:
                        new_pdf.insert_pdf(docsrc=pdf, from_page=page_index, to_page=page_index, links=0, annots=0, widgets=0)
                    else:
                        new_pdf.insert_pdf(docsrc=pdf, from_page=page_index, to_page=page_index, links=0, annots=0)
                    processed = pymupdf4llm.to_markdown(new_pdf)
                    new_pdf.close()
                else:
                    page_text = page.get_text().strip()
                    if page_text.startswith(r'\x'):
                        page_text = ""
                    processed = page_text
                if isinstance(processed, str):
                    pdf_contents[page_index] = processed
                elif isinstance(processed, Request):
                    requests.append(processed)
                else:
                    raise ValueError(f"Page processors must return string or Request. got {type(processed)} instead.")
        if requests:
            context = self.context.copy()
            context.update({"pdf_contents": pdf_contents})
            promise = Promise(reqs=requests, callback=self.callback, values=context)
            return promise.then()
        pdf_contents = pdf_contents.items()
        pdf_contents = [x[1] for x in sorted(pdf_contents)]
        return "\n".join(pdf_contents)
    
    def _extract_by_markitdown(self, content, to_markdown=False, **kwargs):
        if _markitdown_dependency_exc_info:
            raise ImproperlyConfigured(
                "You need to install the markitdown library to enable markitdown engine to parse pdf files."
            ) from _markitdown_dependency_exc_info[
                1
            ].with_traceback(  # type: ignore[union-attr]
                _markitdown_dependency_exc_info[2]
            )
        file_stream = BytesIO(content)
        return PdfConverter().convert(file_stream, StreamInfo(), **kwargs)


class DocumentExtractor:

    _parsers = {
        "xls": XlsParser,
        "xlsx": XlsxParser,
        "doc": WordParser,
        "docx": WordParser,
        "pdf": PdfParser,
    }

    def __init__(self, custom_parsers=None, to_markdown=False, ignored_tags=(), parser_config=None):
        custom_parsers = custom_parsers or {}
        for filetype, parser in custom_parsers.items():
            if not inspect.isclass(parser):
                raise ImproperlyConfigured(f"Custom parser {parser} must be a class.")
            if not hasattr(parser, "extract"):
                raise ImproperlyConfigured(f"Custom parser {parser} must have an extract method.")
            self._parsers[filetype] = parser

        self.parser_config = parser_config
        self._magika = Magika()
        self.to_markdown = to_markdown
        self.ignored_tags = arg_to_iter(ignored_tags)

    def _get_parser(self, filetype, parser_config=None):
        parser_config = parser_config or {}
        init_kwargs = parser_config.get(filetype, {})
        try:
            return self._parsers[filetype](**init_kwargs)
        except KeyError:
            raise TypeError(f"No matched parser for {filetype}.")

    def extract(self, response):
        return self._extract(response.content, filetype=response.filetype)

    def _extract(self, content, filetype=None, **kwargs) -> Union[str, Promise]:
        if self.to_markdown and _markitdown_dependency_exc_info:
            raise ImproperlyConfigured(
                "You need to install the markitdown library to convert content to markdown."
            ) from _markitdown_dependency_exc_info[
                1
            ].with_traceback(  # type: ignore[union-attr]
                _markitdown_dependency_exc_info[2]
            )

        filetype = filetype or self._guess_filetype(content)
        parser = self._get_parser(filetype, self.parser_config)
        return parser.extract(
            content, filetype=filetype, to_markdown=self.to_markdown, ignored_tags=self.ignored_tags, **kwargs)

    def _guess_filetype(self, content):
        if _magika_dependency_exc_info:
            raise ImproperlyConfigured(
                "No filetype provided and missing magika library to guess filetype."
            ) from _magika_dependency_exc_info[
                1
            ].with_traceback(  # type: ignore[union-attr]
                _magika_dependency_exc_info[2]
            )

        result = self._magika.identify_bytes(content)
        if result.status == "ok" and result.prediction.output.label != "unknown":
            if len(result.prediction.output.extensions) > 0:
                return result.prediction.output.extensions[0]
