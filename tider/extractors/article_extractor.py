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

AUTHOR_PATTERN = r"(?:责编|责任编辑|作者|编辑|文|原创|撰文|来源)[：|:| |丨|/]\s*([\u4E00-\u9FA5a-zA-Z]{2,20})[^\u4E00-\u9FA5|:|：]"

IGNORED_PARAGRAPHS = (
    '首页', '分享到', '分享：', '分享', '打印此页', '关闭窗口', '打印', '关闭', '保存',
    '【打印本页】', '【关闭窗口】', '【打印页面】', '【关闭页面】', '【TOP】',
    '上一篇：', '下一篇：', '字体：【大中小】', '字体大小：[大中小]', '字号：大中小',
    '【字体：大中小】', '【字体：小中大】', '【字体：大中小】打印', '【字体：大中小】打印分享：',
    '【字体:  大  中  小】', '扫一扫在手机打开当前页', '扫码查看手机版', '是否打开信息无障碍浏览',
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

    __slots__ = ('node', '_path', 'score', 'source', 'source_parent')

    def __init__(self, node: Tag, score=0):
        self.node = node

        self._path = None
        self.score = score
        self.source = deepcopy(node)
        self.source_parent = deepcopy(node.parent) if node.parent else None

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

    def get_text(self, separator: str = "", strip: bool = False,):
        return self.node.get_text(separator=separator, strip=strip)

    def __hash__(self):
        return hash(self.node)

    def __eq__(self, other):
        return self.node == other.node

    def __getattr__(self, name):
        return getattr(self.node, name)


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
        title = title.strip().lower()
        for cn, en in punc_table.items():
            title = title.replace(cn, en)
        return title

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
            if each.name in ("script", "style"):
                invalid_nodes.append(each)
            elif each.name == 'a':
                if not each.get('href') and not each.get('title'):
                    invalid_nodes.append(each)
            elif each.name == 'div' and not each.find_all(recursive=False) and not each.get_text().strip():
                invalid_nodes.append(each)
            elif self._get_text(each) in self.ignored_paragraphs:
                invalid_nodes.append(each)
            elif not self._get_text(each) and each.name in TAGS_CAN_BE_REMOVE_IF_EMPTY and not [c for c in each.children]:
                invalid_nodes.append(each)
            elif each.name == 'b' and not self._get_text(each):
                invalid_nodes.append(each)
            elif each.name in ('h2', 'h3', 'h4'):
                for small in each.find_all('small'):
                    invalid_nodes.append(small)
            elif 'display:none' in each.get('style', ''):
                invalid_nodes.append(each)
            elif each.get('aria-hidden', '').lower() == 'true':
                invalid_nodes.append(each)
        for each in invalid_nodes:
            node = self._find_only_child_parent(each)
            each.extract()
            contents = node.contents
            text = "".join(c.get_text().strip() for c in contents).strip()
            if not text and node.name not in ('p', 'b', 'td', 'h1', 'h2', 'h3', 'h4' 'h5'):
                # don't affect select candidates.
                node.extract()
        return root

    def _clean_node(self, node: Tag) -> Tag:
        for child in node.children:
            if not isinstance(child, Tag):
                continue
            child = self._clean_node(child)
            if not self._get_text(child) and child.name in TAGS_CAN_BE_REMOVE_IF_EMPTY and not [c for c in child.children]:
                child.extract()
        return node

    @staticmethod
    def _find_only_child_parent(node: Tag):
        while (
            node.parent
            and node.parent.name != 'body'
            and len([c for c in node.parent.children if isinstance(c, Tag)]) == 1
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
        return float(len(link_text)) / max(len(text), 1)

    def _tag_weight(self, e):
        multiplier = 1
        hints = " ".join(e.get('class') or []) + (e.get('id') or "")
        if re.search(self._tag_regexes['positives'], hints):
            multiplier += 1
        if re.search(self._tag_regexes['negatives'], hints):
            multiplier -= 1
        return (multiplier - 1) * 25

    def gen_candidate(self, elem):
        score = self._tag_weight(elem)
        name = elem.name.lower()
        if name == 'ucapcontent':
            score += 10
        if name == "div":
            score += 5
        elif name == "blockquote":
            score += 3
        elif name == "form":
            score -= 3
        elif name == "th":
            score -= 5
        return {'score': score, 'elem': elem}

    def _get_candidates(self, body, response):
        candidates = {}
        invalid_elems = []
        for elem in body.find_all(lambda x: x.name in ('p', 'b', 'td', 'span', 'iframe', 'img', 'div')):
            skip_elem = False
            temp = elem.parent
            while temp:
                # td tag may include the whole content.
                if temp.name in ('p', 'b', 'span'):
                    skip_elem = True
                    break
                temp = temp.parent
            if skip_elem:
                continue

            score = 1
            inner_text = elem.get_text().strip()
            if elem.name == 'div':
                if (
                    elem.get('id')
                    or not self._get_text(elem)
                    or any(filter(lambda x: isinstance(x, Tag) and x.name != 'br', elem.contents))
                ):
                    continue
                elem.name = 'p'
            if elem.name in ('p', 'span'):
                if not elem.get_text().strip() and all([not isinstance(each, Tag) or each.name == 'br' for each in elem.contents]):
                    invalid_elems.append(elem)
                    continue
            if elem.name in ('iframe', 'img'):
                src = elem.get('src', '')
                if (
                    not src
                    or src.startswith(('window.', 'javascript'))
                    or parse_url_host(response.urljoin(src)) != parse_url_host(response.url)
                    or elem.name == 'img' and self._find_only_child_parent(elem).name not in ('p', 'span')
                ):
                    continue
                score += 8
            else:
                if len(inner_text) >= self.paragraph_min_length:
                    score += min((len(inner_text) / 100, 3))
                score += self._get_punctuations_score(inner_text)

            parent_node = elem.parent
            if not parent_node:
                key = Candidate(elem)
                if key not in candidates:
                    candidates[key] = {'score': score, 'elem': elem}
                continue

            parent_key = Candidate(self._find_only_child_parent(parent_node))
            if parent_key not in candidates:
                candidates[parent_key] = self.gen_candidate(parent_node)  # don't score the only child parent.
            candidates[parent_key]['score'] += score

            grandparent_node = parent_key.node.parent
            if grandparent_node:
                node = self._find_only_child_parent(grandparent_node)
                if node.name == 'table' and node.parent:
                    key = Candidate(node.parent)
                    if key in candidates:
                        candidates[key]['score'] += score * 0.3
                grandparent_key = Candidate(node)
                if grandparent_key not in candidates:
                    candidates[grandparent_key] = self.gen_candidate(grandparent_node)
                candidates[grandparent_key]['score'] += score * 0.6  # maybe half.

        # Scale the final candidates score based on link density. Good content should have a
        # relatively small link density (5% or less) and be mostly unaffected by this operation.
        for elem in invalid_elems:
            candidates.pop(Candidate(elem), None)
            elem.extract()
        for key, candidate in candidates.items():
            candidate['score'] *= (1 - self._get_link_density(candidate['elem']))
        return candidates

    @staticmethod
    def _get_punctuations_score(text):
        return len(re.findall(r'''([，。！？；,.!?;、“”‘’《》%（）'"()])''', text))

    @staticmethod
    def _title_from_meta(root):
        title_tag = root.find('meta', attrs={'name': 'ArticleTitle'})
        if not title_tag:
            site_meta = root.find(lambda x: x.name == 'meta' and x.get('name', '').lower() == 'sitename')
            sitename = site_meta.get('content') or '' if site_meta else ''
            title_tag = root.find('title')
            if title_tag and sitename:
                if title_tag.get_text().strip() == sitename:
                    title_tag = None
        return (title_tag.get('content', '') or title_tag.get_text()).strip() if title_tag else ""

    def _extract_title(self, root, content_node):
        title = ""
        h_tags = root.find_all(lambda x: x.name in ('h1', 'h2', 'h3', 'h4', 'h5'))
        page_title = self._title_from_meta(root)
        for h_tag in h_tags:
            lcs = get_longest_common_sub_string(page_title, h_tag.get_text().strip())
            if len(lcs) > len(title):
                title = lcs
        title = title if len(title) > 4 else ''

        if not title:
            source = node = self._find_only_child_parent(content_node.parent) if content_node.parent else content_node
            if not node.find_all(lambda x: x.name in ('h1', 'h2', 'h3')):
                node = node.parent or node
            title_tags = []
            for child in node.children:
                if not isinstance(child, Tag):
                    continue
                if child == source:
                    break
                title_tags.extend(child.find_all('h2') or child.find_all('h3') or child.find_all('h1'))
            if title_tags:
                contents = [each.get_text().strip() for each in title_tags[-1].contents if self._get_text(each)]
                if contents and len(contents[0]) > 4:
                    title = contents[0]

        if not title and page_title:
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
            lambda x: x.name == 'meta'
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

    def extract_article(self, response) -> Article:
        if self._is_json_capable(response):
            return Article()
        root = self._clean_root(response.soup('lxml'))
        content_node = body = root.find('body') or root
        candidates = self._get_candidates(body, response)
        sorted_candidates = sorted(candidates.values(), key=lambda x: x['score'], reverse=True)

        author = self._extract_author(root)
        if len(sorted_candidates) != 0:
            best_candidate = sorted_candidates[0]
            content_node = self._finalize_content(candidates, best_candidate)
        title = self._extract_title(root, content_node)
        pubdate = self._extract_pubdate(root, content_node)
        article = Article(content=str(content_node), title=title, publish_date=pubdate, author=author)
        if self.on_extract:
            self.on_extract(root=root, content_node=content_node, article=article)
        return article

    def _finalize_content(self, candidates, best_candidate, recursive=True):
        # Now that we have the top candidate, look through its siblings for content that might also be related.
        # Things like preambles, content split by ads that we removed, etc.
        result = best_candidate['elem'].parent
        if not result:
            return best_candidate['elem']

        contents = [each for each in result.contents if isinstance(each, Tag)]
        parent = self._find_only_child_parent(result)
        if len(contents) > 1 and all([each.name in ('h1', 'h2', 'h3') or each is best_candidate['elem'] for each in contents]):
            return best_candidate['elem']
        root = best_candidate['elem']
        while root.parent:
            root = root.parent
        title = self._title_from_meta(root)
        score = best_candidate['score']
        sibling_score_threshold = max([10, score * 0.2])

        continuous_text = False
        invalid_siblings = []
        for sibling in contents:
            if sibling is best_candidate['elem']:
                continue
            if sibling.name not in ("b", "p", "span", "h1", "h2", "h3"):
                continuous_text = False
            sibling_key = Candidate(sibling)
            if sibling_key in candidates and candidates[sibling_key]['score'] >= sibling_score_threshold:
                score += candidates[sibling_key]['score']
                continue
            elif sibling.name in ("b", "p", "span", "h1", "h2", "h3"):
                if continuous_text:
                    continue
                link_density = self._get_link_density(sibling)
                node_content = sibling.get_text().strip()
                node_length = len(node_content)
                if node_length > 80 and link_density < 0.25:
                    continuous_text = True
                    continue
                elif node_length < 80 and link_density == 0 and re.search(r'[.。]( |$)', node_content):
                    continuous_text = True
                    continue
                continuous_text = False
            elif title and title in sibling.get_text().strip():
                if title == sibling.get_text().strip():
                    # won't affect extract title.
                    invalid_siblings.append(sibling)
                    continue
                for node in sibling.contents:
                    if not isinstance(node, Tag):
                        continue
                    for each in node.find_all(lambda x: x.name in ("img", "iframe", "a")):
                        href = each.get('href') or each.get('src') or ""
                        if not href.startswith(('/', './', '../', 'http')) or href.endswith(('.html', '.htm', '.shtml', '.jsp')):
                            invalid_siblings.append(self._find_only_child_parent(each))
                continue
            elif sibling.find(lambda x: x.name in ("img", "iframe", "a")):
                invalid = False
                for each in sibling.find_all(lambda x: x.name in ("img", "iframe", "a")):
                    href = each.get('href') or each.get('src') or ""
                    text = each.get('title') or each.get('popover') or each.get_text().strip()
                    ext = self.ext_extractor.extract(href, source_type='url') or self.ext_extractor.extract(text)
                    attrs_text = "".join(each.get('class', [])) + each.get('id', '') + each.get('alt', '')
                    if href.startswith(('javascript', 'window.')) or not ext or re.search(r'qrcode|二维码', attrs_text):
                        invalid = True
                        break
                link_density = self._get_link_density(sibling)
                if not invalid and link_density >= 0.5:
                    if contents.index(sibling) < contents.index(best_candidate['elem']):
                        invalid = True
                attrs_text = "".join(sibling.get('class', [])) + sibling.get('id', '') + sibling.get('alt', '')
                attrs_text = attrs_text.lower()
                if not invalid and not re.search(r'qrcode|二维码', attrs_text):
                    continue
            invalid_siblings.append(sibling)

        siblings = parent.parent.contents if parent.parent else []
        siblings = [each for each in siblings if isinstance(each, Tag) and each.get_text().strip()]
        title_from_outside = title and title not in result.get_text() and root.find('body') and title in root.find('body').get_text()
        try_extend = (
            (not title and parent != result)
            or not result.find_all(lambda x: x.name in ("h2", "h3", "h4"))
            or title_from_outside and (not siblings or siblings[-1] is not parent)
        )

        # clean invalid siblings
        for sibling in invalid_siblings:
            non_h_tags = []
            for each in sibling.contents:
                if not isinstance(each, Tag) or each.name not in ('h1', 'h2', 'h3', 'h4'):
                    non_h_tags.append(each)
            for non_h_tag in non_h_tags:
                non_h_tag.extract()
            if sibling.name in ('h1', 'h2', 'h3', 'h4') or sibling.find(lambda x: x.name in ("h1", "h2")):
                if sibling.get_text().strip() != title and contents.index(sibling) < contents.index(best_candidate['elem']):
                    # avoid extract uncaught titles
                    continue
            sibling.extract()

        depth = 2
        node = parent
        while depth >= 0 and try_extend:
            if node and node.name == 'body':
                try_extend = False
                break
            depth -= 1
            node = node.parent
        if try_extend and recursive:
            best_candidate = {'elem': parent, 'score': score}
            return self._finalize_content(candidates, best_candidate, recursive=False)
        elif siblings:
            reserved_nodes = []
            for sibling in siblings[siblings.index(parent) + 1:]:
                if not isinstance(sibling, Tag) or not sibling.find_all('a'):
                    continue
                if all([a.get('href', '').endswith(('.pdf', '.doc', '.docx', '.xlsx', '.xls', '.jpg', '.png')) for a in sibling.find_all('a')]):
                    reserved_nodes.append(sibling)
            if reserved_nodes:
                best_candidate = {'elem': parent, 'score': score}
                return self._finalize_content(candidates, best_candidate, recursive=False)
        return result
