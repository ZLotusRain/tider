import re
from re import Match
from pprint import pformat
from typing import List, Any, Callable, Pattern, Dict, Union

UNKNOWN_SIMPLE_FORMAT_KEY = """
Unknown format %{0} in string {1!r}.
Possible causes: Did you forget to escape the expand sign (use '%%{0!r}'),
or did you escape and the value was expanded twice? (%%N -> %N -> %hostname)?
""".strip()

RE_FORMAT = re.compile(r'%(\w)')


def enforce_str(s, encoding='utf-8'):
    """Given a string object, regardless of type, returns a representation of
    that string in the native string type, encoding and decoding where
    necessary. This assumes ASCII unless told otherwise.
    """
    if isinstance(s, str):
        out = s
    else:
        out = s.decode(encoding)

    return out


def str_to_list(s: Union[str, List]) -> List[str]:
    """Convert string to list."""
    if isinstance(s, str):
        return s.split(',')
    return s


def str_to_unicode(string: str) -> str:
    """Convert string to unicode."""
    return ''.join(rf'\u{ord(x):04x}' if len(hex(ord(x))[2:]) <= 4 else rf'\U{ord(x):08x}' for x in string)


def indent(t, indent=0, sep='\n'):
    # type: (str, int, str) -> str
    """Indent text."""
    return sep.join(' ' * indent + p for p in t.split(sep))


def truncate(s, maxlen=128, suffix='...'):
    # type: (str, int, str) -> str
    """Truncate text to a maximum number of characters."""
    if maxlen and len(s) >= maxlen:
        return s[:maxlen].rsplit(' ', 1)[0] + suffix
    return s


def pluralize(n, text, suffix='s'):
    # type: (int, str, str) -> str
    """Pluralize term when n is greater than one."""
    if n != 1:
        return text + suffix
    return text


def pretty(value, width=80, nl_width=80, sep='\n', **kw):
    # type: (str, int, int, str, **Any) -> str
    """Format value for printing to console."""
    if isinstance(value, dict):
        return f'{{{sep} {pformat(value, 4, nl_width)[1:]}'
    elif isinstance(value, tuple):
        return '{}{}{}'.format(
            sep, ' ' * 4, pformat(value, width=nl_width, **kw),
        )
    else:
        return pformat(value, width=width, **kw)


def simple_format(
        s: str, keys: Dict[str, Union[str, Callable]],
        pattern: Pattern[str] = RE_FORMAT, expand: str = r'\1') -> str:
    """Format string, expanding abbreviations in keys'."""
    if s:
        keys.setdefault('%', '%')

        def resolve(match: Match) -> Union[str, Any]:
            key = match.expand(expand)
            try:
                resolver = keys[key]
            except KeyError:
                raise ValueError(UNKNOWN_SIMPLE_FORMAT_KEY.format(key, s))
            if callable(resolver):
                return resolver()
            return resolver

        return pattern.sub(resolve, s)
    return s


def _get_emojis_from_file(fp):
    # memory efficient, fast.
    for line in fp:
        if line.startswith("#") or not line.strip():
            continue
        codes = line.split('; ')[0].strip()
        if ".." in codes:
            start, end = codes.split('..')
            if len(start) > 4:
                start = rf"\U{start.rjust(8, '0')}"
            else:
                start = rf"\u{start.rjust(4, '0')}"
            if len(end) > 4:
                end = rf"\U{end.rjust(8, '0')}"
            else:
                end = rf"\u{end.rjust(4, '0')}"
            yield f"{start}-{end}"
        else:
            result = ""
            codes = codes.split(' ')
            for code in codes:
                # padding_length = (4 - (len(end) % 4)) % 4
                if len(code) > 4:
                    code = rf"\U{code.rjust(8, '0')}"
                else:
                    code = rf"\u{code.rjust(4, '0')}"
                result += code
            yield result


def _get_emojis():
    curr_dir = __file__.rsplit('\\', maxsplit=1)[0].rsplit('/', maxsplit=1)[0]
    with open(f"{curr_dir}/emoji-sequences.txt", 'r', encoding='utf-8') as fo:
        for emoji in _get_emojis_from_file(fo):
            yield emoji
    with open(f"{curr_dir}/emoji-zwj-sequences.txt", "r", encoding='utf-8') as fo:
        for emoji in _get_emojis_from_file(fo):
            yield emoji


def remove_emojis(text):
    """Remove emojis from text."""
    for emoji in _get_emojis():
        if '-' in emoji or emoji.endswith('FE0F'):
            pattern = f"[{emoji}]+"
        else:
            pattern = emoji
        emoji_pattern = re.compile(pattern=pattern, flags=re.UNICODE)
        text = emoji_pattern.sub(r'', text)
    return text
