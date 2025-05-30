def iter_lines(path, encoding='utf-8', errors='strict', callback=None, reverse=False):
    if not reverse:
        with open(path, "r", encoding=encoding) as fp:
            for line in fp:
                if callback:
                    line = callback(line)
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
                    if callback:
                        line = callback(line)
                    yield line
                    line = b""
                line = current_char + line
