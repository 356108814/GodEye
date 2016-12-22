# encoding: utf-8

import threading

from config import settings
from consumer.base import BaseConsumer
from kafka import KafkaConsumer
from util.log import logger


class Kafka1Consumer(BaseConsumer):
    """
    消费者基类，只针对kafka
    """

    def __init__(self, topics, group_id, bootstrap_servers):
        """
        初始化。默认消费最近的数据，offset自动提交
        :param topics: 消息类别
        :param group_id: 消费组
        :param bootstrap_servers: 服务器列表
        :return:
        """
        # auto_offset_reset='earliest'
        # enable_auto_commit=False，默认为True，自动保存offset
        super(Kafka1Consumer).__init__()
        if settings.DEBUG:
            auto_offset_reset = 'earliest'
            enable_auto_commit = False
        else:
            auto_offset_reset = 'latest'
            enable_auto_commit = True
        self.kafka_consumer = KafkaConsumer(topics, group_id=group_id, bootstrap_servers=bootstrap_servers,
                                            auto_offset_reset=auto_offset_reset, enable_auto_commit=enable_auto_commit)
        self._handle_thread = None

    def handle_thread(self):
        try:
            for message in self.kafka_consumer:
                # message value and key are raw bytes -- decode if necessary!
                # e.g., for unicode: `message.value.decode('utf-8')`
                topic = message.topic
                partition = message.partition
                offset = message.offset
                key = message.value.decode('utf-8')
                value = message.value.decode('utf-8')
                # print("%s:%d:%d: key=%s value=%s" % (topic, partition, offset, key, value))
                if value:
                    self.consume(value)
        except Exception as e:
            if settings.DEBUG:
                raise e
            logger.error('Consumer.handle_thread:%s' % str(e))

    def consume(self, line_data):
        print("value:%s" % line_data)

    def handle_start(self):
        topics = self.kafka_consumer.topics()
        print('topics:%s' % topics)
        self._handle_thread = threading.Thread(target=self.handle_thread)    # 必须放在子线程中，不然阻塞server主循环
        self._handle_thread.start()

    def handle_stop(self):
        self.kafka_consumer.close()
        self.kafka_consumer = None
        self._handle_thread = None


if __name__ == '__main__':
    consumer = Kafka1Consumer('minik_weixin_user_action', 'python-etl-group', ['172.16.3.222:9092'])
    consumer.start()
