# encoding: utf-8
"""
文件消费者
@author Yuriseus
@create 2016-8-23 13:50
"""
import os

from util.file import FileUtil

from config import settings
from consumer.base import BaseConsumer, State
from util.log import logger


class FileConsumer(BaseConsumer):
    def __init__(self, dir_path):
        super(BaseConsumer).__init__()
        self._dir_path = dir_path
        self._files = []

    def check_process(self):
        """
        主循环每秒检测
        :return:
        """
        if self.state == State.STARTED:
            if not self._files:    # 队列已处理完成，重新读取目录新文件
                self._files = FileUtil.get_files(self._dir_path, ['.COMPLETE', '.gz'], self.is_filter_file)
                self._files.sort(key=self.get_file_name)
            self.process_files()

    def is_filter_file(self, file_path):
        """
        是否过滤掉该文件
        :param file_path:
        :return:
        """
        return False

    def get_file_name(self, file_path):
        return os.path.basename(file_path)

    def consume(self, line_data):
        print(line_data)

    def process_files(self):
        while self._files:
            if not settings.GLOBAL_RUNNING:
                break
            if self.state == State.STARTED:    # 若处理过程中暂停会停止，正在处理的文件不受影响，继续处理完成
                file_path = self._files.pop(0)
                logger.info('start:%s' % file_path)
                with open(file_path, encoding='utf-8') as f:
                    lineno = 0
                    while True:
                        try:
                            line = f.readline()
                            if not line:
                                break
                            self.consume(line)
                            lineno += 1
                            print(lineno)
                        except UnicodeDecodeError as e:
                            logger.warning('UnicodeDecodeError:%s, filepath:%s, lineno:%s' % (str(e), file_path, lineno+1))
                            # raise e
                        except Exception as e:
                            logger.warning('error:%s, filepath:%s, lineno:%s' % (str(e), file_path, lineno+1))
                            raise

                # 处理完成。重命名文件，从列表中移除
                complete_file_path = file_path + '.COMPLETE'
                os.rename(file_path, complete_file_path)

    def handle_start(self):
        pass

    def handle_pause(self):
        pass

    def handle_stop(self):
        pass


if __name__ == '__main__':
    dir_path = 'G:\\GitHub\\study\\test'
    consumer = FileConsumer(dir_path)
    consumer.start()
