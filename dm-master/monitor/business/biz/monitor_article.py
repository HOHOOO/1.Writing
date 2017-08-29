# -*- coding: utf-8 -*-
import logging.config
import json
from utils.redis_client import RedisClient
from comm.consts import *


logger = logging.getLogger("monitor")


class MonitorArticleList(object):
	def __init__(self, device_id, smzdm_id):
		self._device_id = device_id
		self._smzdm_id = smzdm_id
	
	def get_article_list_from_redis(self):
		key = HISTORY_B_KEY % self._device_id
		redis_client = RedisClient().get_redis_simple_client()
		article_list = redis_client.lrange(key, 0, -1)
		logger.debug(article_list)
		return article_list

	def check_duplicate_or_sequence_article(self):
		"""
		desc: 监测用户列表是否有重复; 非好加内容是否连续
		:return: err_msg
		"""
		err_msg = ''
		err_msg_list = []

		article_list = self.get_article_list_from_redis()
		duplicate_dict = {}
		sequence_list = []
		sequence_channel_list = []
		sequence_article_list = []
		for item in article_list:
			d = json.loads(item)
			article_id = d.get("article_id", None)
			channel_id = d.get("channel", None)

			if not article_id or not channel_id:
				continue

			duplicate_key = "%s:%s" % (article_id, channel_id)
			if duplicate_key in duplicate_dict.keys():
				duplicate_dict[duplicate_key] += 1
			else:
				duplicate_dict[duplicate_key] = 1
			
			# 非好价连续
			if channel_id != 3:
				sequence_channel_list.append(channel_id)
				sequence_article_list.append(article_id)
			if len(sequence_channel_list) == 3 and len(set(sequence_channel_list)) == 1:
				sequence_list.append(zip(sequence_article_list, sequence_channel_list))
				sequence_channel_list = []
				sequence_article_list = []
			else:
				sequence_channel_list = sequence_channel_list[1:]
				sequence_article_list = sequence_article_list[1:]
			
		duplicate_list = [(k, v) for (k, v) in duplicate_dict.items() if v > 1]
		if duplicate_list:
			err_msg_list.append("device_id: %s duplicate article list: %s" % (self._device_id, duplicate_list))
		
		if sequence_list:
			err_msg_list.append("device_id: %s, sequence article list: %s" % (self._device_id, sequence_list))
		logger.info("err_msg: %s", err_msg_list)
		if err_msg_list:
			err_msg = ";".join(err_msg_list)
		return err_msg
