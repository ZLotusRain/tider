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
