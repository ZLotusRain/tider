import json
import time
import socket
import warnings

from tider.exceptions import DownloadError
from tider.utils.log import get_logger

logger = get_logger(__name__)

BASIC_MD_MESSAGE = """
<font color="#FF0000">**Tider Alarm Message**</font>

> message: <font color="#DC143C">{message}</font>
> alarm_time: {alarm_time}
> alarm_count: {count}
> host: {host}
> pid: {pid}
> spider: <font color="#2b90d9">{spider}</font>
> start_time: {start_time}
"""


class Alarm:
    """
    msg_type: text|markdown|html
    """

    def __init__(self, stats, spidername, msg_type='text'):
        self.stats = stats
        self.spidername = spidername
        self.msg_type = msg_type

        self._topics = {}

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            stats=crawler.stats,
            msg_type=crawler.alarm_message_type,
            spidername=crawler.spidername
        )

    def alarm(self, msg,  msg_type=None, extra_infos=None):
        if msg not in self._topics:
            self._topics[msg] = 0
        self._topics[msg] += 1
        msg_type = msg_type or self.msg_type

        infos = {
            'message': msg,
            'alarm_time': time.strftime('%Y-%m-%d %H:%M:%S'),
            'count': self._topics[msg],
            'host': socket.gethostname(),
            'pid': self.stats.get_value('pid'),
            'spider': self.spidername,
            'start_time': self.stats.get_value("time/start_time/format")
        }
        extra_infos = dict(extra_infos) if extra_infos else {}
        if extra_infos:
            infos.update(extra_infos)

        if msg_type == 'text':
            message = json.dumps(infos, ensure_ascii=False)
        elif msg_type == 'markdown':
            message = BASIC_MD_MESSAGE
            message = message.format(**infos)
            for key in extra_infos:
                value = extra_infos[key]
                if not isinstance(value, str):
                    value = json.dumps(value, ensure_ascii=False, indent=4)
                message += f'> {key}: {value}\n'
            message.strip('\n')
        else:
            raise ValueError(f"Not support message type `{msg_type}` right now")
        self.send_message(message, msg_type, infos)

    def send_message(self, message, msg_type=None, infos=None):
        warnings.warn(message)

    def close(self):
        pass


class WeChatGRobotAlarm(Alarm):

    def __init__(self, crawler, stats, spidername, key, mentioned_list=None,
                 mentioned_mobile_list=None, msg_type='text'):
        super().__init__(stats, spidername, msg_type)
        self.crawler = crawler
        self.key = key
        self.mentioned_list = mentioned_list
        self.mentioned_mobile_list = mentioned_mobile_list

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            crawler=crawler,
            stats=crawler.stats,
            msg_type=crawler.settings['ALARM_MESSAGE_TYPE'],
            spidername=crawler.spidername,
            key=crawler.settings["WECHAT_ROBOT_KEY"],
            mentioned_list=crawler.settings.getlist("WECHAT_ROBOT_MENTIONED_LIST"),
            mentioned_mobile_list=crawler.settings.getlist("WECHAT_ROBOT_MENTIONED_MOBILE_LIST")
        )

    def send_message(self, message, msg_type=None, infos=None):
        url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={self.key}"
        payload = {
            "msgtype": msg_type,
            msg_type: {
                "content": message,
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

