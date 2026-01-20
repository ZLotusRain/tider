import re
import json
from datetime import datetime
from bs4 import Tag
from copy import deepcopy

from tider.utils.url import parse_url_host
from tider.extractors.link_extractor import FileExtExtractor

__all__ = ('ArticleExtractor', )

NEGATIVE_KEYWORDS = {
    'combx',
    'comment',
    'contact',
    'contribution',
    'copyright',
    'copy-right',
    'disclaimer',
    'header',
    'foot',
    'footer',
    'footnote',
    'link',
    'media',
    'meta',
    'modal',
    'pop',
    'pop-content',
    'promo',
    'qrcode',
    'recommend',
    'related',
    'report-infor',
    'scroll',
    'share',
    'shoutbox',
    'social',
    'sponsor',
    'tags',
    'widget',
}

POSITIVE_KEYWORDS = {
    'article',
    'body',
    'content',
    'detail',
    'entry',
    'head',
    'hentry',
    'news',
    'page',
    'pagination',
    'post',
    'text',
    'txt'
}

DATETIME_PATTERNS = [
    r"(\d{4}年\d{1,2}月\d{1,2}日\s*?[0-1]?[0-9]:[0-5]?[0-9]:[0-5]?[0-9])",
    r"(\d{4}年\d{1,2}月\d{1,2}日\s*?[2][0-3]:[0-5]?[0-9]:[0-5]?[0-9])",
    r"(\d{4}年\d{1,2}月\d{1,2}日\s*?[0-1]?[0-9]:[0-5]?[0-9])",
    r"(\d{4}年\d{1,2}月\d{1,2}日\s*?[2][0-3]:[0-5]?[0-9])",
    r"(\d{4}年\d{1,2}月\d{1,2}日\s*?[1-24]\d时[0-60]\d分)([1-24]\d时)",
    r"(\d{2}年\d{1,2}月\d{1,2}日\s*?[0-1]?[0-9]:[0-5]?[0-9]:[0-5]?[0-9])",
    r"(\d{2}年\d{1,2}月\d{1,2}日\s*?[2][0-3]:[0-5]?[0-9]:[0-5]?[0-9])",
    r"(\d{2}年\d{1,2}月\d{1,2}日\s*?[0-1]?[0-9]:[0-5]?[0-9])",
    r"(\d{2}年\d{1,2}月\d{1,2}日\s*?[2][0-3]:[0-5]?[0-9])",
    r"(\d{2}年\d{1,2}月\d{1,2}日\s*?[1-24]\d时[0-60]\d分)([1-24]\d时)",
    r"(\d{1,2}月\d{1,2}日\s*?[0-1]?[0-9]:[0-5]?[0-9]:[0-5]?[0-9])",
    r"(\d{1,2}月\d{1,2}日\s*?[2][0-3]:[0-5]?[0-9]:[0-5]?[0-9])",
    r"(\d{1,2}月\d{1,2}日\s*?[0-1]?[0-9]:[0-5]?[0-9])",
    r"(\d{1,2}月\d{1,2}日\s*?[2][0-3]:[0-5]?[0-9])",
    r"(\d{1,2}月\d{1,2}日\s*?[1-24]\d时[0-60]\d分)([1-24]\d时)",
    r"(\d{4}年\d{1,2}月\d{1,2}日)",
    r"(\d{2}年\d{1,2}月\d{1,2}日)",
    r"(\d{1,2}月\d{1,2}日)"
    
    r"(\d{4}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s*?[0-1]?[0-9]:[0-5]?[0-9]:[0-5]?[0-9])",
    r"(\d{4}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s*?[2][0-3]:[0-5]?[0-9]:[0-5]?[0-9])",
    r"(\d{4}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s*?[0-1]?[0-9]:[0-5]?[0-9])",
    r"(\d{4}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s*?[2][0-3]:[0-5]?[0-9])",
    r"(\d{4}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s*?[1-24]\d时[0-60]\d分)([1-24]\d时)",
    r"(\d{2}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s*?[0-1]?[0-9]:[0-5]?[0-9]:[0-5]?[0-9])",
    r"(\d{2}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s*?[2][0-3]:[0-5]?[0-9]:[0-5]?[0-9])",
    r"(\d{2}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s*?[0-1]?[0-9]:[0-5]?[0-9])",
    r"(\d{2}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s*?[2][0-3]:[0-5]?[0-9])",
    r"(\d{2}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s*?[1-24]\d时[0-60]\d分)([1-24]\d时)",

    r"(\d{4}[-|/|.]\d{1,2}[-|/|.]\d{1,2})",
    r"(\d{2}[-|/|.]\d{1,2}[-|/|.]\d{1,2})",
]

AUTHOR_PATTERN = r"(?:责编|责任编辑|作者|编辑|文|原创|撰文|来源|发布机构|发布单位)[：|:| |丨|/]\s*([\u4E00-\u9FA5a-zA-Z]{2,20})[^\u4E00-\u9FA5|:|：]"

TITLE_PATTERNS = [
    r"关于.*?通知",
    r"关于.*?公告"
]

IGNORED_PARAGRAPHS = (
    '首页', '分享到', '分享：', '分享', '打印此页', '关闭窗口', '打印', '关闭', '保存',
    '【打印本页】', '【关闭窗口】', '【打印页面】', '【关闭页面】', '【TOP】',
    '上一篇：', '下一篇：', '字体：【大中小】', '字体大小：[大中小]', '字号：大中小',
    '【字体：大中小】', '【字体：小中大】', '【字体：大中小】打印', '【字体：大中小】打印分享：',
    '【字体:  大  中  小】', '字号:〖大 中 小〗', '字体:', '【字体：大 中 小】',
    '扫一扫在手机打开当前页', '扫码查看手机版', '是否打开信息无障碍浏览',
)

# if one tag in the follow list does not contain any child node nor content, it could be removed
TAGS_CAN_BE_REMOVE_IF_EMPTY = ['section', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'div']


def get_longest_common_sub_string(str1: str, str2: str) -> str:
    if not all([str1, str2]):
        return ''
    matrix = [[0] * (len(str2) + 1) for _ in range(len(str1) + 1)]
    max_length = 0
    start_position = 0
    for index_of_str1 in range(1, len(str1) + 1):
        for index_of_str2 in range(1, len(str2) + 1):
            if str1[index_of_str1 - 1] == str2[index_of_str2 - 1]:
                matrix[index_of_str1][index_of_str2] = matrix[index_of_str1 - 1][index_of_str2 - 1] + 1
                if matrix[index_of_str1][index_of_str2] > max_length:
                    max_length = matrix[index_of_str1][index_of_str2]
                    start_position = index_of_str1 - max_length
            else:
                matrix[index_of_str1][index_of_str2] = 0
    return str1[start_position: start_position + max_length]


class Article:

    __slots__ = ('content', 'title', 'source', 'author', 'publish_date', )

    _AVAILABLE_KEYS = ('content', 'title', 'source', 'author', 'publish_date', )

    def __init__(self, content: str = "", title: str = "", source: str = "",
                 publish_date: str = "", author: str = ""):
        self.content = content
        self.title = title
        self.source = source
        self.publish_date = publish_date
        self.author = author

    def __getitem__(self, key):
        if key not in self._AVAILABLE_KEYS:
            raise KeyError(f"{self.__class__.__name__} does not support key: {key}")
        return getattr(self, key)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __hash__(self) -> int:
        return (
            hash(self.title) ^ hash(self.publish_date) ^ hash(self.author) ^ hash(self.content)
        )


class Candidate:

    __slots__ = ('node', '_path', 'score', 'source')

    def __init__(self, node: Tag, score=0):
        self.node = node

        self._path = None
        self.score = score
        self.source = None

    def finalize_source(self, node=None):
        if self.source:
            return
        node = node or self.node
        source = self.source = deepcopy(node)
        for _ in range(2):
            node = node.parent
            if not node:
                break
            source.parent = deepcopy(node)
            source = source.parent

    @property
    def path(self):
        if self._path is None:
            reverse_path = []
            node = self.node
            while node:
                if node.parent:
                    idx = node.parent.contents.index(node)
                else:
                    idx = 0
                node_id = (idx, node.name, tuple(node.attrs), node.string)
                reverse_path.append(node_id)
                node = node.parent
            self._path = tuple(reverse_path)
        return self._path

    def get_text(self, separator: str = "", strip: bool = False):
        return self.node.get_text(separator=separator, strip=strip)

    def __hash__(self):
        return hash(self.node)

    def __eq__(self, other):
        return self.node == other.node


class ArticleExtractor:

    PARAGRAPH_LENGTH_THRESHOLD = 3

    def __init__(self, allow_ignored=False, ignored_paragraphs=None, paragraph_min_length=None,
                 title=None, on_extract=None):
        self._tag_regexes = {
            'positives': r'|'.join(POSITIVE_KEYWORDS),
            'negatives': r'|'.join(NEGATIVE_KEYWORDS),
        }
        self._title = title  # provided title.
        self.on_extract = on_extract
        self.ext_extractor = FileExtExtractor(guess=False)
        self.ignored_paragraphs = set()
        if not allow_ignored:
            ignored_paragraphs = ignored_paragraphs or []
            self.ignored_paragraphs = set(ignored_paragraphs) | set(IGNORED_PARAGRAPHS)
        self.paragraph_min_length = paragraph_min_length or self.PARAGRAPH_LENGTH_THRESHOLD

    @staticmethod
    def _format_title(title):
        punc_table = {
            '，': ',',
            '。': '.',
            '！': '!',
            '？': '?',
            '：': ':',
            '；': ';',
            '“': '"',
            '”': '"',
            '‘': "'",
            '’': "'",
            '（': '(',
            '）': ')',
            '【': '[',
            '】': ']',
            '《': '<',
            '》': '>',
            '、': ',',
            '—': '-',
            '…': '...',
        }
        title = title.replace('\n', '').strip().lower()
        for cn, en in punc_table.items():
            title = title.replace(cn, en)
        return title

    @staticmethod
    def _get_tag_name(tag: Tag) -> str:
        return tag.name.lower()

    @staticmethod
    def _get_text(tag: Tag):
        s = tag.get_text().replace("\xa0", "").strip()
        spaces = ["&nbsp;", "&ensp;", "&emsp;", "&thinsp;", "&zwnj;", "&zwj;", "&#x0020;", "&#x0009;",
                  "&#x000A;", "&#x000D;", "&#12288;", " ", " "]
        keywords = ["\r", "\n", "\t"] + spaces
        for key in keywords:
            s = s.replace(key, "")
        return s.replace(" ", "").strip()

    def _clean_root(self, root: Tag) -> Tag:
        invalid_nodes = []
        for each in root.find_all(recursive=True):
            if self._get_tag_name(each) in ("script", "style"):
                invalid_nodes.append(each)
            elif self._get_tag_name(each) == 'a':
                if not each.get('href') and not each.get('title'):
                    invalid_nodes.append(each)
            elif self._get_tag_name(each) == 'div' and not each.find_all(recursive=False) and not each.get_text().strip():
                invalid_nodes.append(each)
            elif self._get_text(each) in self.ignored_paragraphs:
                invalid_nodes.append(each)
            elif not self._get_text(each) and self._get_tag_name(each) in TAGS_CAN_BE_REMOVE_IF_EMPTY and not [c for c in each.children]:
                invalid_nodes.append(each)
            elif self._get_tag_name(each) == 'b' and not self._get_text(each):
                invalid_nodes.append(each)
            elif self._get_tag_name(each) in ('h2', 'h3', 'h4'):
                for small in each.find_all('small'):
                    invalid_nodes.append(small)
            elif 'display:none' in each.get('style', '').replace(" ", ""):
                invalid_nodes.append(each)
            elif each.get('aria-hidden', '').lower() == 'true':
                invalid_nodes.append(each)
        for each in invalid_nodes:
            node = self._find_only_child_parent(each)
            each.extract()
            contents = node.contents
            text = "".join(c.get_text().strip() for c in contents).strip()
            if not text and self._get_tag_name(node) not in ('p', 'b', 'td', 'h1', 'h2', 'h3', 'h4' 'h5'):
                # don't affect select candidates.
                node.extract()
        return root

    def _clean_node(self, node: Tag) -> Tag:
        for child in node.children:
            if not isinstance(child, Tag):
                continue
            child = self._clean_node(child)
            if not self._get_text(child) and self._get_tag_name(child) in TAGS_CAN_BE_REMOVE_IF_EMPTY and not [c for c in child.children]:
                child.extract()
        return node

    def _find_only_child_parent(self, node: Tag, stop_util=('body', )):
        stop_util = set(stop_util) | {'body', } if stop_util else {'body', }
        while (
            node.parent
            and self._get_tag_name(node.parent) not in stop_util
            and len([c for c in node.parent.children if isinstance(c, Tag) and self._get_tag_name(c) != 'br']) == 1
        ):
            node = node.parent
        return node

    def _get_link_density(self, tag):
        link_text = ""
        text = tag.get_text().strip()
        for a_tag in tag.find_all("a"):
            href = a_tag.get('href')
            title = a_tag.get('title') or a_tag.get('popover') or a_tag.get_text().strip()
            if href and not (self.ext_extractor.extract(href, source_type='url') or self.ext_extractor.extract(title)):
                link_text += a_tag.text or ""
        # text maybe in attrs.
        return min(float(len(link_text)) / max(len(text), 1), 1)

    def _tag_weight(self, e):
        multiplier = 1
        hints = " ".join(e.get('class') or []) + e.get('id', '')
        hints = hints.lower()
        if re.search(self._tag_regexes['positives'], hints):
            multiplier += 1
        if re.search(self._tag_regexes['negatives'], hints):
            multiplier -= 1
        return (multiplier - 1) * 25

    def gen_candidate_score(self, elem):
        name = self._get_tag_name(elem)
        score = self._tag_weight(elem)
        score_map = {
            "div": 5,
            "ucapcontent": 10,
            "blockquote": 3,
            "form": -3,
            "th": -5
        }
        score += score_map.get(name, 0)
        return score

    def _get_candidates(self, body, response):
        candidates = []
        invalid_elems = []

        def _filter(t):
            flag = False
            if self._get_tag_name(t) in ('p', 'b', 'td', 'span', 'iframe', 'img', 'div'):
                flag = True
                tp = t.parent
                while tp and self._get_tag_name(tp) not in ('p', 'b', 'span'):
                    # td tag may include the whole content.
                    tp = tp.parent
                if tp:
                    flag = False
            return flag

        for elem in body.find_all(name=_filter):
            score = 1
            inner_text = elem.get_text().strip()
            if self._get_tag_name(elem) == 'div':
                if (
                    elem.get('id')
                    or not self._get_text(elem)
                    or any(filter(lambda x: isinstance(x, Tag) and self._get_tag_name(x) != 'br', elem.contents))
                ):
                    continue
                # div which only contains innerText
                elem.name = 'p'
            if self._get_tag_name(elem) in ('p', 'span'):
                if not self._get_text(elem) and all([not isinstance(e, Tag) or self._get_tag_name(e) == 'br' for e in elem.contents]):
                    invalid_elems.append(elem)
                    continue
            if self._get_tag_name(elem) in ('iframe', 'img'):
                src = elem.get('src', '')
                if (
                    not src
                    or src.startswith(('window.', 'javascript'))
                    or parse_url_host(response.urljoin(src)) != parse_url_host(response.url)
                    or self._get_tag_name(elem) == 'img' and self._get_tag_name(self._find_only_child_parent(elem)) not in ('p', 'span')
                ):
                    continue
                score += 8
            else:
                if len(inner_text) >= self.paragraph_min_length:
                    score += min((len(inner_text) / 100, 3))
                score += self._get_punctuations_score(inner_text)

            parent_node = elem.parent
            if not parent_node or self._get_tag_name(parent_node) == 'body':
                key = Candidate(elem, score=score)
                if key not in candidates:
                    candidates.append(key)
            else:
                parent_key = Candidate(self._find_only_child_parent(parent_node))
                try:
                    idx = candidates.index(parent_key)
                    parent_key = candidates[idx]
                except ValueError:
                    parent_key.score = self.gen_candidate_score(parent_node)  # don't score the only child parent.
                    candidates.append(parent_key)
                parent_key.score += score

                grandparent_node = self._find_only_child_parent(parent_key.node.parent)
                if grandparent_node and grandparent_node != parent_key.node:
                    if not grandparent_node.parent or self._get_tag_name(grandparent_node.parent) not in ('td', 'p', 'span'):
                        grandparent_key = Candidate(grandparent_node)
                        try:
                            idx = candidates.index(grandparent_key)
                            grandparent_key = candidates[idx]
                        except ValueError:
                            grandparent_key.score = self.gen_candidate_score(grandparent_node)
                            candidates.append(grandparent_key)
                        if self._get_tag_name(grandparent_node) == 'table':
                            grandparent_key.score += score * 0.4
                            if grandparent_node.parent:
                                key = Candidate(grandparent_node.parent)
                                if key in candidates:
                                    candidates[candidates.index(key)].score += score * 0.3
                        else:
                            grandparent_key.score += score * 0.6  # maybe half.

        # Scale the final candidates score based on link density. Good content should have a
        # relatively small link density (5% or less) and be mostly unaffected by this operation.
        for elem in invalid_elems:
            elem.extract()
        for candidate in candidates:
            candidate.score *= (1 - self._get_link_density(candidate.node))
        return sorted(candidates, key=lambda x: x.score, reverse=True)

    @staticmethod
    def _get_punctuations_score(text):
        return len(re.findall(r'''([，。！？；,.!?;、“”‘’《》%（）'"()])''', text))

    @staticmethod
    def _title_from_meta(root):
        title_tag = root.find('meta', attrs={'name': 'ArticleTitle'})
        if not title_tag:
            site_meta = root.find(lambda x: x.name.lower() == 'meta' and x.get('name', '').lower() == 'sitename')
            sitename = site_meta.get('content') or '' if site_meta else ''
            title_tag = root.find('title')
            if title_tag and sitename:
                if title_tag.get_text().strip() == sitename:
                    title_tag = None
        return (title_tag.get('content', '') or title_tag.get_text()).strip() if title_tag else ""

    @staticmethod
    def _match_title(title):
        for pattern in TITLE_PATTERNS:
            if re.findall(pattern, title):
                return True
        return False

    def _extract_title(self, root, content_node):
        title = ""
        h_tags = root.find_all(lambda x: self._get_tag_name(x) in ('h1', 'h2', 'h3', 'h4', 'h5'))
        page_title = self._title_from_meta(root)
        for h_tag in h_tags:
            lcs = get_longest_common_sub_string(page_title, h_tag.get_text().strip())
            if len(lcs) > len(title):
                title = lcs
        title = title if len(title) > 4 else ''

        if not title:
            source = node = self._find_only_child_parent(content_node.parent) if content_node.parent else content_node
            title_tags = node.find_all(lambda x: self._get_tag_name(x) in ('h1', 'h2', 'h3'))
            if not title_tags or len(title_tags) == 1 and str(node).index(str(title_tags[0])) > len(str(node)) * 0.65:
                title_tags = []
                node = node.parent
                children = node.children if node else []
                for child in children:
                    if not isinstance(child, Tag):
                        continue
                    if child == source:
                        break
                    title_tags.extend(child.find_all('h2') or child.find_all('h3') or child.find_all('h1'))
            if title_tags:
                maybe_titles = [self._format_title(t.get_text()) for t in title_tags[:-1]]
                if self._format_title(page_title) in maybe_titles:
                    title = page_title
                else:
                    node_len = len(str(node))
                    for title_tag in title_tags[::-1]:
                        if str(node).index(str(title_tag)) > node_len * 0.7:
                            continue
                        contents = [each.get_text().strip() for each in title_tag.contents if self._get_text(each)]
                        if contents and len(contents[0]) > 4:
                            title = contents[0]
                            break
            content_text = self._format_title(content_node.get_text())
            if page_title and title and title != page_title and self._format_title(page_title) in content_text:
                # actual title not in h tags.
                try:
                    if content_text.index(self._format_title(page_title)) < content_text.index(self._format_title(title)):
                        title = page_title
                except ValueError:
                    pass
                if title != page_title:
                    if self._match_title(page_title) and not self._match_title(title):
                        title = page_title

        if page_title and (not title or page_title != title and page_title in self._format_title(content_node.get_text())[:50]):
            tmp = re.split(r'[-_|]', page_title)
            if tmp and len(tmp[0]) >= 4:
                title = tmp[0]
            else:
                title = page_title
        if not title and h_tags:
            contents = [each.get_text().strip() for each in h_tags[0].contents if self._get_text(each)]
            if contents:
                title = contents[0]
        return title

    @staticmethod
    def _extract_pubdate(root, content_node):
        pubdate = ""
        meta_tag = root.find_all(
            lambda x: x.name.lower() == 'meta'
            and (
                x.get('name', '').startswith(
                    ('OriginalPublicationDate', 'article_date_original', 'og:time', 'apub:time',
                     'publication_date', 'sailthru.date', 'PublishDate', 'publishdate', 'PubDate',
                     'pubtime', '_pubtime', 'weibo: article:create_at', 'pubdate')
                )
                or x.get('itemprop', '').startswith(('datePublished', 'dateUpdate'))
                or x.get('property', '').startswith(
                    ('PubDate', 'rnews:datePublished', 'article:published_time', 'og:published_time', 'og:release_date')
                )
            )
        )
        if meta_tag:
            pubdate = meta_tag[0].get('content', '').strip()
            if len(pubdate) > 20:
                pubdate = ""

        first_taken = False
        tmp_date = ""
        text = content_node.get_text().strip()
        paragraphs = [p for p in text.split('\n') if len(p) <= 20]
        for p in paragraphs:
            for pattern in DATETIME_PATTERNS:
                result = re.findall(r"(?:发布日期：|发布时间：|印发日期：)\s*" + pattern, p)
                if result:
                    tmp_date = result[0]
                    first_taken = True
                    break
                result = re.findall(pattern, p)
                if len(result) == 1 and len(result[0]) == len(p.strip()):
                    tmp_date = result[0]
                    if p == paragraphs[-1]:
                        first_taken = True
                    break
            if tmp_date:
                break
        if tmp_date and first_taken:
            pubdate = tmp_date
        else:
            pubdate = pubdate or tmp_date
        if not pubdate:
            for pattern in DATETIME_PATTERNS:
                result = re.findall(pattern, text)
                if result:
                    pubdate = result[0]
                    break
        if pubdate:
            pubdate = re.sub(r'[年月]', '-', pubdate)
            pubdate = re.sub(r'[时分]', ':', pubdate).strip()
            pubdate = pubdate.replace('—', '-').replace('/', '-')
            pubdate = pubdate.replace('  ', '').strip(':').strip()
            pubdate = pubdate.replace('日', ' ').replace('秒', ' ')
            pubdate = pubdate.replace('∶', ':')
            try:
                date_obj = datetime.strptime(pubdate, "%b %d, %Y %I:%M:%S %p")
                pubdate = date_obj.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                try:
                    date_obj = datetime.strptime(pubdate, "%Y-%m-%d %H:%M")
                    pubdate = date_obj.strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    try:
                        date_obj = datetime.strptime(pubdate, "%Y-%m-%d %H-%M-")
                        pubdate = date_obj.strftime("%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        pass
        return pubdate

    @staticmethod
    def _extract_author(root):
        result = re.findall(AUTHOR_PATTERN, root.get_text().strip())
        return result[0] if result else ""

    @staticmethod
    def _is_json_capable(response):
        try:
            response.json()
            return True
        except json.JSONDecodeError:
            return False

    def extract(self, response) -> Article:
        if self._is_json_capable(response):
            return Article()
        root = self._clean_root(response.soup('lxml'))
        source = content_node = body = root.find('body') or root
        candidates = self._get_candidates(body, response)

        author = self._extract_author(root)
        if len(candidates) != 0:
            best_candidate = candidates[0]
            candidate = self._finalize_content(best_candidate, candidates=candidates)
            source = candidate.source
            content_node = candidate.node
        title = self._title or self._extract_title(root, source)
        pubdate = self._extract_pubdate(root, source)
        article = Article(content=str(content_node), title=title, source=str(source), publish_date=pubdate, author=author)
        if self.on_extract:
            self.on_extract(root=root, content_node=content_node, article=article)
        return article

    extract_artile = extract

    def _finalize_content(self, best_candidate, candidates, recursive=True):
        # Now that we have the top candidate, look through its siblings for content that might also be related.
        # Things like preambles, content split by ads that we removed, etc.
        best_candidate.finalize_source()
        if not best_candidate.node.parent:
            return best_candidate
        result = Candidate(best_candidate.node.parent)
        result.source = best_candidate.source.parent  # remain unchanged

        contents = [each for each in result.node.contents if isinstance(each, Tag)]
        parent = self._find_only_child_parent(result.node)
        parent_candidate = Candidate(parent)
        parent_candidate.finalize_source()
        if len(contents) > 1 and all([self._get_tag_name(each) in ('h1', 'h2', 'h3') or each is best_candidate.node for each in contents]):
            return best_candidate

        root = best_candidate.node
        while root.parent:
            root = root.parent
        title = self._format_title(self._title or self._title_from_meta(root))
        score = best_candidate.score
        sibling_score_threshold = max([10, score * 0.2])

        positioned = False
        continuous_text = False
        invalid_siblings = []
        for sibling in contents:
            if sibling is best_candidate.node:
                positioned = True  # don't mark tags as invalid between positioned and next valid.
                continue
            if self._get_tag_name(sibling) not in ("b", "p", "span", "h1", "h2", "h3"):
                continuous_text = False
            sibling_key = Candidate(sibling)
            if sibling_key in candidates and candidates[candidates.index(sibling_key)].score >= sibling_score_threshold:
                score += candidates[candidates.index(sibling_key)].score
                if positioned:
                    invalid_siblings.clear()
                if self._get_tag_name(sibling) in ("b", "p", "span", "h1", "h2", "h3"):
                    continuous_text = True
                if all([self._get_tag_name(c) in ("b", "p", "span", "h1", "h2", "h3", "br") for c in sibling.contents if isinstance(c, Tag)]):
                    continuous_text = True
                continue
            elif self._get_tag_name(sibling) in ("b", "p", "span", "h1", "h2", "h3"):
                if continuous_text:
                    if positioned:
                        invalid_siblings.clear()
                    continue
                link_density = self._get_link_density(sibling)
                node_content = sibling.get_text().strip()
                node_length = len(node_content)
                if node_length > 80 and link_density < 0.25:
                    continuous_text = True
                    if positioned:
                        invalid_siblings.clear()
                    continue
                elif node_length < 80 and link_density == 0 and re.search(r'[.。]( |$)', node_content):
                    continuous_text = True
                    if positioned:
                        invalid_siblings.clear()
                    continue
                continuous_text = False
            elif title and title in self._format_title(sibling.get_text()):
                if title == self._format_title(sibling.get_text()):
                    # won't affect extract title.
                    invalid_siblings.append(sibling)
                    continue
                for node in sibling.contents:
                    if not isinstance(node, Tag):
                        continue
                    for each in node.find_all(lambda x: self._get_tag_name(x) in ("img", "iframe", "a")):
                        href = each.get('href') or each.get('src') or ""
                        if not href.startswith(('/', './', '../', 'http')) or href.endswith(('.html', '.htm', '.shtml', '.jsp')):
                            invalid_siblings.append(self._find_only_child_parent(each))
                continue
            elif sibling.find(lambda x: self._get_tag_name(x) in ("img", "iframe", "a")):
                invalid = False
                for each in sibling.find_all(lambda x: self._get_tag_name(x) in ("img", "iframe", "a")):
                    href = each.get('href') or each.get('src') or ""
                    text = each.get('title') or each.get('popover') or each.get_text().strip()
                    ext = self.ext_extractor.extract(href, source_type='url') or self.ext_extractor.extract(text)
                    attrs_text = "".join(each.get('class', [])) + each.get('id', '') + each.get('alt', '')
                    if href.startswith(('javascript', 'window.')) or not ext or re.search(r'qrcode|二维码|首页', attrs_text):
                        invalid = True
                        break
                link_density = self._get_link_density(sibling)
                if not invalid and link_density >= 0.5:
                    if contents.index(sibling) < contents.index(best_candidate.node):
                        invalid = True
                attrs_text = "".join(sibling.get('class', [])) + sibling.get('id', '') + sibling.get('alt', '')
                attrs_text = attrs_text.lower()
                if not invalid and not re.search(r'qrcode|二维码', attrs_text):
                    continue
            invalid_siblings.append(sibling)

        siblings = parent.parent.contents if parent.parent else []
        siblings = [each for each in siblings if isinstance(each, Tag) and each.get_text().strip()]
        title_from_outside = title and title not in self._format_title(result.get_text()) and root.find('body') and title in root.find('body').get_text()
        try_extend = (
            (not title and parent != result.node)
            or not result.node.find_all(lambda x: self._get_tag_name(x) in ("h2", "h3", "h4"))
            or title_from_outside and (not siblings or siblings[-1] is not parent)
        )

        # clean invalid siblings
        for sibling in invalid_siblings:
            non_h_tags = []
            for each in sibling.contents:
                if not isinstance(each, Tag) or self._get_tag_name(each) not in ('h1', 'h2', 'h3', 'h4'):
                    non_h_tags.append(each)
            for non_h_tag in non_h_tags:
                non_h_tag.extract()
            if self._get_tag_name(sibling) in ('h1', 'h2', 'h3', 'h4') or sibling.find(lambda x: self._get_tag_name(x) in ("h1", "h2")):
                if sibling.get_text().strip() != title and contents.index(sibling) < contents.index(best_candidate.node):
                    # avoid extract uncaught titles
                    continue
            sibling.extract()

        depth = 2
        node = parent
        while depth >= 0 and try_extend:
            if node and self._get_tag_name(node) == 'body':
                try_extend = False
                break
            depth -= 1
            node = node.parent
        parent_candidate.score = score
        if try_extend and recursive:
            return self._finalize_content(parent_candidate, candidates, recursive=False)
        elif siblings:
            reserved_nodes = []
            for sibling in siblings[siblings.index(parent) + 1:]:
                if not isinstance(sibling, Tag) or not sibling.find_all('a'):
                    continue
                if all([a.get('href', '').endswith(('.pdf', '.doc', '.docx', '.xlsx', '.xls', '.jpg', '.png')) for a in sibling.find_all('a')]):
                    reserved_nodes.append(sibling)
            if reserved_nodes:
                return self._finalize_content(parent_candidate, candidates, recursive=False)
        return result
