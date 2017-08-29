# -*- coding: utf-8 -*-
import time
import sys
import urllib
import json
from biz.monitor_article import MonitorArticleList
from utils.dingding import dingding_alarm_msg


from utils.rabbit_mq import create_rabbit_mq_connect
from base.config import load, Config

import logging.config
logging.config.fileConfig("logger.conf")

logger = logging.getLogger("monitor")


def check_user_article_list(raw):
	if not raw:
		return
	json_raw = json.loads(raw)
	
	action = json_raw.get("action", "")
	logger.info("action: %s", action)
	if action and action == "business_monitor":
		device_id = json_raw.get("device_id", "")
		smzdm_id = json_raw.get("smzdm_id", "")
		logger.info("device_id: %s", device_id)
		if device_id:
			ma = MonitorArticleList(device_id, smzdm_id)
			err_msg = ma.check_duplicate_or_sequence_article()
			if err_msg:
				dingding_alarm_msg(err_msg)


def callback(ch, method, properties, body):
	raw = urllib.unquote(body)
	logger.info("Received msg: %s", raw)
	check_user_article_list(raw)
	time.sleep(body.count('.'))
	# 对message进行确认
	ch.basic_ack(delivery_tag=method.delivery_tag)


def consumer(conn):
	logger.info("=====start=====")
	channel = conn.channel()
	channel.queue_declare(queue=Config["rabbit_mq.queue"], durable=True)
	channel.basic_qos(prefetch_count=1)
	channel.basic_consume(callback, queue=Config["rabbit_mq.queue"])
	channel.start_consuming()
	logger.info("=====end=====")

if __name__ == "__main__":
	config_path = sys.argv[1]
	load(config_path)
	connection = create_rabbit_mq_connect(Config["rabbit_mq.host"],
	                                      Config["rabbit_mq.user"],
	                                      Config["rabbit_mq.password"],
	                                      int(Config["rabbit_mq.port"]),
	                                      Config["rabbit_mq.vhost"])
	consumer(connection)
