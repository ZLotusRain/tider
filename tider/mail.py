"""Mail sending helpers."""

from __future__ import annotations

import smtplib

from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.nonmultipart import MIMENonMultipart
from email.mime.text import MIMEText
from email.utils import formatdate

from typing import (
    IO,
    Any,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from tider.utils.log import get_logger
from tider.utils.misc import arg_to_iter, to_bytes


logger = get_logger(__name__)


# Defined in the email.utils module, but undocumented:
# https://github.com/python/cpython/blob/v3.9.0/Lib/email/utils.py#L42
COMMASPACE = ", "


def _to_bytes_or_none(text: Union[str, bytes, None]) -> Optional[bytes]:
    if text is None:
        return None
    return to_bytes(text)


class MailSender:
    def __init__(
        self,
        smtphost: str = "localhost",
        mailfrom: str = "tider@localhost",
        smtpuser: Optional[str] = None,
        smtppwd: Optional[str] = None,
        smtpport: int = 25,
        debug: bool = False,
    ):
        self.smtphost: str = smtphost
        self.smtpport: int = smtpport
        self.smtpuser: Optional[str] = smtpuser
        self.smtppwd: Optional[str] = smtppwd
        self.mailfrom: str = mailfrom

        self._smtp: Optional[smtplib.SMTP] = None
        self.debug: bool = debug

    @classmethod
    def from_settings(cls, settings):
        return cls(
            smtphost=settings["MAIL_HOST"],
            mailfrom=settings["MAIL_FROM"],
            smtpuser=settings["MAIL_USER"],
            smtppwd=settings["MAIL_PWD"],
            smtpport=settings.getint("MAIL_PORT"),
        )

    @property
    def smtp(self) -> smtplib.SMTP:
        if self._smtp is None:
            self._smtp = smtplib.SMTP_SSL(host=self.smtphost, port=self.smtpport)
            if self.smtpuser:
                self._smtp.ehlo()
                self._smtp.login(user=self.smtpuser, password=self.smtppwd)
        return self._smtp

    def send(
        self,
        to: Union[str, List[str]],
        subject: str,
        body: str,
        cc: Union[str, List[str], None] = None,
        attachs: Sequence[Tuple[str, str, IO[Any]]] = (),
        mimetype: str = "text/plain",
        charset: Optional[str] = None,
    ):
        msg: MIMEBase
        if attachs:
            msg = MIMEMultipart()
        else:
            msg = MIMENonMultipart(*mimetype.split("/", 1))

        to = list(arg_to_iter(to))
        cc = list(arg_to_iter(cc))

        msg["From"] = self.mailfrom
        msg["To"] = COMMASPACE.join(to)
        msg["Date"] = formatdate(localtime=True)
        msg["Subject"] = subject
        rcpts = to[:]
        if cc:
            rcpts.extend(cc)
            msg["Cc"] = COMMASPACE.join(cc)

        if attachs:
            if charset:
                msg.set_charset(charset)
            msg.attach(MIMEText(body, "plain", charset or "us-ascii"))
            for attach_name, mimetype, f in attachs:
                part = MIMEBase(*mimetype.split("/"))
                part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition", "attachment", filename=attach_name
                )
                msg.attach(part)
        else:
            msg.set_payload(body, charset)

        if self.debug:
            logger.debug(
                "Debug mail sent OK: To=%(mailto)s Cc=%(mailcc)s "
                'Subject="%(mailsubject)s" Attachs=%(mailattachs)d',
                {
                    "mailto": to,
                    "mailcc": cc,
                    "mailsubject": subject,
                    "mailattachs": len(attachs),
                },
            )
            return None

        try:
            self.smtp.sendmail(from_addr=self.mailfrom, to_addrs=to, msg=msg.as_string().encode(charset or "utf-8"))
            logger.info(
                "Mail sent OK: To=%(mailto)s Cc=%(mailcc)s "
                'Subject="%(mailsubject)s" Attachs=%(mailattachs)d',
                {
                    "mailto": to,
                    "mailcc": cc,
                    "mailsubject": subject,
                    "mailattachs": len(attachs),
                },
            )
        except smtplib.SMTPException as e:
            logger.error(
                "Unable to send mail: To=%(mailto)s Cc=%(mailcc)s "
                'Subject="%(mailsubject)s" Attachs=%(mailattachs)d'
                "- %(mailerr)s",
                {
                    "mailto": to,
                    "mailcc": cc,
                    "mailsubject": subject,
                    "mailattachs": len(attachs),
                    "mailerr": e,
                },
            )

    def close(self):
        self._smtp.close()
        self._smtp = None
