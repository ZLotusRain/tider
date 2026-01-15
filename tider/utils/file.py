import re
import zipfile
from io import BytesIO

__all__ = ('iter_lines', 'fix_xlsx_styles', )


def iter_lines(path, encoding='utf-8', errors='strict', reverse=False):
    if not reverse:
        with open(path, "r", encoding=encoding) as fp:
            for line in fp:
                yield line
    else:
        with open(path, "rb+") as fp:
            fp.seek(0, 2)
            position = fp.tell()
            line = b""
            for index in range(position-1, -1, -1):
                fp.seek(index)
                current_char = fp.read(1)
                if line and current_char == b'\n':
                    line = line.decode(encoding=encoding, errors=errors).strip('\n')
                    yield line
                    line = b""
                line = current_char + line


def fix_xlsx_styles(content):
    fp = BytesIO()
    zout = zipfile.ZipFile(fp, "w")

    source = BytesIO(content)
    zin = zipfile.ZipFile(source, "r")
    for item in zin.infolist():
        buffer = zin.read(item.filename)
        if item.filename == "xl/styles.xml":
            styles = buffer.decode("utf-8")
            styles = styles.replace("<x:fill />", "")
            styles = styles.replace('<fills count="1"><fill/></fills>', '')
            cell_styles = re.findall(r'(<cellStyle\s*xfId.*?/>)', styles)
            for style in cell_styles:
                if not re.findall(r'(name=".*?")', style):
                    new_style = style.replace("/>", ' name=""/>')
                    styles = styles.replace(style, new_style)
            buffer = styles.encode("utf-8")
        zout.writestr(item, buffer)
    zout.close()
    zin.close()
    source.close()

    return fp
