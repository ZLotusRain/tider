import json
import time
import socket
import logging
import weakref
import warnings

from tider.network import Request
from tider.utils.spider import get_spider_name

logger = logging.getLogger(__name__)

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

    def __init__(self, tider):
        if isinstance(tider, weakref.ProxyType):
            self.tider = tider
        else:
            self.tider = weakref.proxy(tider)

        self._msg_type = tider.settings.get('ALARM_MESSAGE_TYPE')
        self._topics = {}

    @classmethod
    def from_crawler(cls, tider):
        return cls(tider)

    def alarm(self, msg,  msg_type=None, extra_infos=None):
        if msg not in self._topics:
            self._topics[msg] = 0
        self._topics[msg] += 1
        infos = {
            'message': msg,
            'alarm_time': time.strftime('%Y-%m-%d %H:%M:%S'),
            'count': self._topics[msg],
            'host': socket.gethostname(),
            'pid': self.tider.stats.get_value('pid'),
            'spider': get_spider_name(self.tider.spider),
            'start_time': self.tider.stats.get_value("time/start_time/format")
        }
        extra_infos = dict(extra_infos) if extra_infos else {}
        if extra_infos:
            infos.update(extra_infos)
        msg_type = msg_type or self._msg_type
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

    def __init__(self, tider):
        super().__init__(tider)
        self.key = tider.settings.get("WECHAT_ROBOT_KEY")
        self.mentioned_list = tider.settings.getlist("WECHAT_ROBOT_MENTIONED_LIST")
        self.mentioned_mobile_list = tider.settings.getlist("WECHAT_ROBOT_MENTIONED_MOBILE_LIST")

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
        req = Request(url=url, method='POST', data=json.dumps(payload), timeout=10)
        response = self.tider.engine.explorer.try_explore(req)
        if response.ok:
            logger.info(f"Send alarm message by wechat robot successfully: {infos['message']}")
        else:
            logger.error(f"Unable to send alarm message by wechat robot: {response.reason}")
        response.close()
