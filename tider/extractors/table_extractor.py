from bs4 import Tag

__all__ = ('HtmlTableExtractor',)


TABLE_TITLES = {
    "序号", "排名", "公司名称", "企业名称", "企业名单",
    "县区", "区域", "所属镇街", "所在区域", "所在辖区",
    "名称", "归口管理", "资金", "备注", "企业类型", "所属区域"
    "权利限制情况及瑕疵情况", "权利限制情况", "拍卖权利限制及瑕疵等情况", "权利限制及瑕疵情况",
    "权证情况", "标的物权证情况", "拍品权证情况", "标的权证情况",
    "拍品所有人", "标的物所有人", "标的所有人",
    "标的物介绍", "拍品介绍", "拍卖成交提供的文件", "提供的文件"
}


class Table:

    __slots__ = ("title", "content", "orientation", )

    def __init__(self, title="", content=None, orientation="vertical"):
        self.title = title
        self.content = content
        self.orientation = orientation

    def is_vertical(self):
        return self.orientation == "vertical"

    @staticmethod
    def dict_to_str(dic):
        string = ""
        for key in dic.keys():
            value = dic[key]
            if isinstance(value, dict):
                value = Table.dict_to_str(value)
                value = "[" + value + "]"
            string += f"{key}：{value}，"
        string = string.rstrip("，")
        return string


class HtmlTableExtractor:

    def __init__(self, titles=None):
        self._titles = set(titles or []) | TABLE_TITLES

    def _get_title_density(self, tr: Tag):
        tds = tr.find_all("td")
        if not tds:
            return 0
        titles = [td for td in tds if self._get_td_text(td) in self._titles]
        return round(len(titles) / len(tds), ndigits=2)

    @staticmethod
    def _get_td_text(td: Tag):
        s = td.get_text().replace("\xa0", "").strip()
        keywords = [" ", " ", "　", " ", "\r", "\n", "\t"]
        for key in keywords:
            s = s.replace(key, "")
        return s.strip()

    def _is_vertical(self, tbody: Tag) -> bool:
        trs = [tr for tr in tbody.find_all("tr")]
        while trs and len(trs[0].find_all("td")) <= 1:
            trs = trs[1:]
        if (
            not trs
            or (trs[0].find_all('strong') and trs[0].find_all('strong') == tbody.find_all('strong'))
            or self._get_title_density(trs[0]) > 0.6
        ):
            return True
        return False

    def extract_tables(self, response):
        tables = []  # {'title': '', 'content': {}}
        soup = response.soup('lxml')
        tbodys = soup.find_all("tbody")
        if not tbodys:
            return tables
        for tbody in tbodys:
            tables.append(self._extract_table(tbody))
        return tables

    def _extract_table(self, tbody: Tag) -> Table:
        if self._is_vertical(tbody):
            return self._extract_vertical_table(tbody)
        return self._extract_horizontal_table(tbody)

    def _extract_horizontal_table(self, tbody: Tag):
        match_dict = {}
        i = 0
        tr_list = [tr for tr in tbody.find_all("tr")]
        while i < len(tr_list):
            tds = tr_list[i].find_all("td")
            if not tds:
                i += 1
                continue
            key = self._get_td_text(tds[0])
            if len(tds) == 1:
                match_dict[key] = ""
            elif len(tds) == 2:
                value = tds[1].get_text().strip()
                match_dict[key] = value
            else:
                rows = tds[0].get("rowspan") or 1
                if not rows:
                    value = tds[-1].get_text().strip()
                    match_dict[key] = value
                    i += 1
                    continue
                tmp_dict = {}
                for s in range((len(tds) - 1) // 2):
                    k, v = self._get_td_text(tds[2 * (s + 1) - 1]), self._get_td_text(tds[2 * (s + 1)])
                    tmp_dict[k] = v
                for r in range(int(rows) - 1):
                    tds = tr_list[i + 1 + r].find_all("td")
                    if not tds:
                        continue
                    k, v = self._get_td_text(tds[0]), self._get_td_text(tds[-1])
                    tmp_dict[k] = v
                match_dict[key] = tmp_dict
                i = i + int(rows) - 1
            i += 1
        return Table(content=match_dict, orientation="horizontal")

    def _extract_vertical_table(self, tbody: Tag):
        trs = [tr for tr in tbody.find_all("tr")]
        result = []
        while trs and len(trs[0].find_all("td")) <= 1:
            if trs[0].find('td'):
                result.append({self._get_td_text(trs[0].find('td')): ""})
            trs = trs[1:]
        if trs:
            head = [self._get_td_text(td) for td in trs[0].find_all('td')]
            for tr in trs[1:]:
                result.append(dict(zip(head, [self._get_td_text(td) for td in tr.find_all('td')])))
        return Table(content=result, orientation="vertical")
