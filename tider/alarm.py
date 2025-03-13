import json
import time
import socket
import warnings
from abc import abstractmethod

from tider.exceptions import DownloadError
from tider.utils.log import get_logger

logger = get_logger(__name__)

BASIC_MD_MESSAGE = """
<font color="#FF0000">**Tider Alarm Message**</font>

> message: <font color="#DC143C">{message}</font>
> alarm_count: {count}
> alarm_time: {alarm_time}
> host: {host}
> pid: {pid}
> spider: <font color="#2b90d9">{spider}</font>
> start_time: {start_time}
"""


class Alarm:
    """
    msg_type: text|markdown|html
    """

    def __init__(self, crawler):
        self.crawler = crawler
        self.stats = crawler.stats
        self.spidername = crawler.spidername
        self.msg_type = crawler.alarm_message_type
        self.msg_formatter = {
            'text': self.text_formatter,
            'markdown': self.markdown_formatter,
        }
        self._topics = {}

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler=crawler)

    def alarm(self, msg,  msg_type=None, extra_infos=None):
        if isinstance(msg, str):
            if msg not in self._topics:
                self._topics[msg] = 0
            self._topics[msg] += 1
        msg_type = msg_type or self.msg_type

        infos = {
            'message': msg,
            'count': self._topics.get(msg) or 1,
            'alarm_time': time.strftime('%Y-%m-%d %H:%M:%S'),
            'host': socket.gethostname(),
            'pid': self.crawler.pid,
            'spider': self.spidername,
            'start_time': self.stats.get_value("time/start_time/format")
        }
        infos.update(dict(extra_infos) if extra_infos else {})

        try:
            message = self.msg_formatter[msg_type](infos)
        except KeyError:
            raise ValueError(f"Not support message type `{msg_type}`")
        self.send_message(message, msg_type, infos)

    def text_formatter(self, infos):
        return json.dumps(infos, ensure_ascii=False, indent=4)

    def markdown_formatter(self, infos):
        message = BASIC_MD_MESSAGE
        message = message.format(**infos)
        reserved_keys = ('message', 'count', 'alarm_time', 'host', 'pid', 'spider', 'start_time')
        for key, value in infos.items():
            if key in reserved_keys:
                continue
            try:
                value = json.dumps(value, ensure_ascii=False, indent=4)
            except json.JSONDecodeError:
                pass
            message += f'> {key}: {value}\n'
        message.strip('\n')
        return message

    @abstractmethod
    def send_message(self, message, msg_type=None, infos=None):
        warnings.warn(message)

    def close(self):
        pass


class WeChatGRobotAlarm(Alarm):

    def __init__(self, crawler, robot_key, mentioned_list=None, mentioned_mobile_list=None):
        super().__init__(crawler=crawler)
        self.crawler = crawler
        self.robot_key = robot_key
        self.mentioned_list = mentioned_list
        self.mentioned_mobile_list = mentioned_mobile_list

        self.msg_formatter.update({'file': self.file_formatter})

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            crawler=crawler,
            robot_key=crawler.settings["WECHAT_ROBOT_KEY"],
            mentioned_list=crawler.settings.getlist("WECHAT_ROBOT_MENTIONED_LIST"),
            mentioned_mobile_list=crawler.settings.getlist("WECHAT_ROBOT_MENTIONED_MOBILE_LIST")
        )

    def file_formatter(self, infos):
        media_id = ""
        content = infos.get('message')
        filename = infos.get('filename') or ""
        url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/upload_media?key={self.robot_key}&type=file"
        files = {
            'media': (filename, content),
        }
        explorer = self.crawler.engine.explorer
        try:
            response = explorer.try_explore(method="POST", url=url, files=files, max_retries=2)
            media_id = response.json()['media_id']
            logger.info(f"Uploaded file to wechat robot: {media_id}")
            response.close()
        except DownloadError as e:
            message = f"Failed to upload file: {e}"
            logger.error(message)
            infos['message'] = message
        return media_id

    def send_message(self, message, msg_type=None, infos=None):
        url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={self.robot_key}"
        if not message:
            # failed to upload file.
            message = self.markdown_formatter(infos)
            msg_type = 'markdown'
        content_key = 'content' if msg_type != 'file' else 'media_id'
        payload = {
            "msgtype": msg_type,
            msg_type: {
                content_key: message,
                "mentioned_list": self.mentioned_list,
                "mentioned_mobile_list": self.mentioned_mobile_list
            }

        }
        explorer = self.crawler.engine.explorer
        try:
            response = explorer.try_explore(method="POST", url=url, data=json.dumps(payload), max_retries=2)
            logger.info(f"Send alarm message by wechat robot successfully: {infos['message']}")
            response.close()
        except DownloadError as e:
            logger.error(f"Unable to send alarm message by wechat robot: {e}")

