import logging

logger = logging.getLogger(__name__)


def bs4_tag_to_str(tag):
    try:
        result = str(tag, errors="ignore")
    except Exception as e:
        logger.error(f"Can't convert :class:`bs4.Tag` to string: {e}")
        result = ""
    return result
