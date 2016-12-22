# encoding: utf-8
"""
增加多文件日志支持
@author Yuriseus
@create 2016-8-3 19:15
"""
import logging
import logging.handlers
import sys


def _stderr_supports_color():
    color = False
    if hasattr(sys.stderr, 'isatty') and sys.stderr.isatty():
        color = True
    return color


def gen_channel(filename):
    return logging.handlers.TimedRotatingFileHandler(
            filename='logs/%s.log' % filename,
            when='midnight',
            interval=1,
            backupCount=1)


class LogFormatter(logging.Formatter):
    DEFAULT_FORMAT = '%(color)s[%(levelname)1.1s %(asctime)s %(module)s:%(lineno)d]%(end_color)s %(message)s'
    DEFAULT_DATE_FORMAT = '%y%m%d %H:%M:%S'

    PURPLE = '\033[95m'    # 紫色
    BLUE = '\033[94m'      # 蓝色
    GREEN = '\033[92m'     # 绿色
    YELLOW = '\033[93m'    # 黄色
    RED = '\033[91m'       # 红色
    ENDC = '\033[0m'       # 结束符

    DEFAULT_COLORS = {
        logging.DEBUG: BLUE,  # Blue
        logging.INFO: GREEN,  # Green
        logging.WARNING: YELLOW,  # Yellow
        logging.ERROR: RED,  # Red
    }

    def __init__(self, color=True, fmt=DEFAULT_FORMAT,
                 datefmt=DEFAULT_DATE_FORMAT, colors=DEFAULT_COLORS):
        super().__init__(fmt, datefmt=datefmt)
        self._fmt = fmt

        self._colors = {}
        if color and _stderr_supports_color():
            for levelno, color_name in colors.items():
                self._colors[levelno] = color_name

    def format(self, record):
        record.message = record.getMessage()
        record.asctime = self.formatTime(record, self.datefmt)
        if record.levelno in self._colors:
            record.color = self._colors[record.levelno]
            record.end_color = LogFormatter.ENDC
        else:
            record.color = record.end_color = ''

        formatted = self._fmt % record.__dict__

        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            lines = [formatted.rstrip()]
            lines.extend(ln for ln in record.exc_text.split('\n'))
            formatted = '\n'.join(lines)
        return formatted.replace("\n", "\n    ")


class Logger(object):

    def __init__(self):
        self._loggers = {}

    def get_logger(self, name):
        if not name:
            name = 'ETL'
        if name not in self._loggers:
            temp_logger = logging.getLogger(name)
            temp_logger.setLevel('DEBUG')
            handler = gen_channel(name)
            handler.setFormatter(LogFormatter(color=True))
            temp_logger.addHandler(handler)
            if True:    # 调试模式默认也输出到控制台
                console_handler = logging.StreamHandler()
                console_handler.setFormatter(LogFormatter(color=False))
                temp_logger.addHandler(console_handler)
            self._loggers[name] = temp_logger
        return self._loggers[name]

    def debug(self, msg, name=None):
        current_logger = self.get_logger(name)
        current_logger.debug(msg)

    def info(self, msg, name=None):
        """
        记录日志
        :param msg:
        :param name: 日志文件名称，如不存在，则创建
        :return:
        """
        current_logger = self.get_logger(name)
        current_logger.info(msg)

    def warning(self, msg, name=None):
        current_logger = self.get_logger(name)
        current_logger.warning(msg)

    def error(self, msg, name=None):
        current_logger = self.get_logger(name)
        current_logger.error(msg)


logger = Logger()

if __name__ == '__main__':
    logger.debug('debug')
    logger.info('info')
    logger.warning('warning')
    logger.error('error')
