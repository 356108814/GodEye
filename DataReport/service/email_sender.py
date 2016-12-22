# encoding: utf-8
"""
邮件发送器
@author Yuriseus
@create 2016-7-5 21:57
"""
import os
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email.utils import parseaddr, formataddr

from jinja2 import Template


class EmailSender(object):
    def __init__(self):
        self._from_name = u'艾美数据报表系统'
        self._from = '1489422436@qq.com'
        self._server = 'smtp.qq.com'
        self._user = '1489422436@qq.com'
        self._password = 'zfnbluiccefbbaaa'

    def send_mail(self, to, subject, text, files=None):
        msg = MIMEMultipart()
        msg['From'] = self. _format_addr('%s <%s>' % (self._from_name, self._from))
        msg['Subject'] = subject
        msg['To'] = COMMASPACE.join(to)
        msg['Date'] = formatdate(localtime=True)
        msg.attach(MIMEText(text, 'html', 'utf-8'))

        if files:
            for index, file_path in enumerate(files):
                mime = MIMEBase('application', 'octet-stream')
                mime.set_payload(open(file_path, 'rb').read())
                encoders.encode_base64(mime)
                filename = os.path.basename(file_path)
                mime.add_header('Content-Disposition', 'attachment; filename="%s"' % filename)
                mime.add_header('Content-ID', '<%s>' % index)
                mime.add_header('X-Attachment-Id', '%s' % index)
                msg.attach(mime)

        smtp = smtplib.SMTP(self._server)
        smtp.starttls()    # QQ邮箱必须加密
        smtp.set_debuglevel(1)
        smtp.login(self._user, self._password)
        smtp.sendmail(self._from, to, msg.as_string())
        smtp.close()

    def _format_addr(self, raw_addr):
        """
        格式化邮件地址。格式：name <addr@example.com>
        :param raw_addr:
        :return:
        """
        name, addr = parseaddr(raw_addr)
        return formataddr((name, addr))


emailSender = EmailSender()

if __name__ == '__main__':
    sender = EmailSender()
    tpl = ''
    text = ''
    param = {'title': u'账单异常', 'content': u'下面为异常订单信息：<br>',
             'bills': [{'seqid': 'kkk1'}, {'seqid': 'lll2'}]}
    with open('bill_alarm_email.html', 'r', encoding='utf-8') as f:
        tpl = f.read()
    template = Template(tpl)
    text = template.render(param)    # 注意模板默认编码为Unicode，因此param中的中文必须也为Unicode
    sender.send_mail(['769435570@qq.com'], '测试主题', text, ['J:/meda/workspace/auto_check_money/Yuriseus.jpeg'])
