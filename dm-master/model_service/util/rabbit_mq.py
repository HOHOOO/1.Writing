# coding=utf-8
import pika
import pika_pool
from tornado import gen
from tornado.log import gen_log

from base.config import Config

CONNECTION_POOL = None


class InitConnectionPool(object):
    def __init__(self):
        global CONNECTION_POOL
        if not CONNECTION_POOL:
            params = pika.URLParameters(Config["rabbitmq.url"])

            CONNECTION_POOL = pika_pool.QueuedPool(
                create=lambda: pika.BlockingConnection(parameters=params),
                max_size=10,
                max_overflow=10,
                timeout=10,
                recycle=3600,
                stale=45,
            )
            gen_log.debug("init RABBIT_MQ_CONNECTION_POOL: %s", CONNECTION_POOL)


class RabbitMQPoolClient(InitConnectionPool):

    def get_rabbit_mq_pool(self):
        return CONNECTION_POOL

    @gen.coroutine
    def get_rabbit_mq_connect(self):
        connection = CONNECTION_POOL.Connection
        raise gen.Return(connection)

    @gen.coroutine
    def get_rabbit_mq_pool_cor(self):
        raise gen.Return(CONNECTION_POOL)


class RabbitMQClient(object):
    @gen.coroutine
    def get_rabbit_mq_connect(self):
        params = pika.URLParameters(Config["rabbitmq.url"])
        connection = pika.BlockingConnection(params)
        raise gen.Return(connection)
