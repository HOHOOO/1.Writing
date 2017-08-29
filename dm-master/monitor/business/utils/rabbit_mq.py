# -*- coding: utf-8 -*-
import pika
import pika_pool
from base.config import Config, load

pool = None


def create_rabbit_mq_connect(host, user, password, port, vHost):
	credentials = pika.PlainCredentials(user, password)
	parameters = pika.ConnectionParameters(host=host, port=port, virtual_host=vHost, credentials=credentials)

	connection = pika.BlockingConnection(parameters)
	return connection


def init_rabbit_mq_pool():
	global pool
	params = pika.URLParameters(Config["rabbit_mq.url"])

	pool = pika_pool.QueuedPool(
        create=lambda: pika.BlockingConnection(parameters=params),
        max_size=int(Config["rabbit_mq.max_size"]),
        max_overflow=int(Config["rabbit_mq.max_overflow"]),
        timeout=int(Config["rabbit_mq.timeout"]),
        recycle=int(Config["rabbit_mq.recycle"]),
        stale=int(Config["rabbit_mq.stale"])
    )


def send_msg(msg):
	if not msg:
		return
	global pool
	
	if not pool:
		init_rabbit_mq_pool()

	with pool.acquire() as cxn:
		cxn.channel.basic_publish(
            body=msg,
            exchange='',
            routing_key=Config["rabbit_mq.queue"],
            properties=pika.BasicProperties(
                content_encoding='utf-8',
                delivery_mode=2,
            )
        )


if __name__ == "__main__":
	import json
	import sys
	config_path = sys.argv[1]
	load(config_path)
	d = {
		u"action": u"business_monitor",
		u"device_id": u"hy4K6471h/LVi3ng1VjYa6RWzp511qG9iz1AyKCT8JzFnWwHJ7hiwg==",
		u"smzdm_id": u"dddd"
	}
	
	send_msg(json.dumps(d))
