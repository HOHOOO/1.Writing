# -*- coding: utf-8 -*-
import os
import logging
from datetime import datetime, timedelta

from channel.comm import save
from util.consts import *
from util.mysql import MysqlClient
from util.utils import get_start_end_time

logger = logging.getLogger(__name__)

target_table = "topic_rec"


def wiki(start_time=None, end_time=None):
	"""
	desc: 计算同步的开始，结束时间。 若输入开始和结束时间则为输入时间; 否则默认开始时间为6小时前，结束时间为当前时间
	:param start_time:
	:param end_time:
	:return:
	"""
	# 千人千面所用的wiki的数据同步直接从首页编辑精选数据库同步文章id和channel_id，不需要从wiki库中同步品牌，品类，标签等信息
	#

	start_time, end_time = get_start_end_time(WK_SYNC_LAST_TIME_PATH, start_time, end_time)

	if start_time == end_time:
		logger.info("wiki start time equal end time return.")
		return
	limit = ROW_COUNT
	offset = 0
	# 新增数据同步
	insert(start_time, end_time, limit, offset)
	# 更新数据同步
	update(start_time, end_time, limit, offset)


def insert(start_time, end_time, limit, offset):
	"""
	desc: 百科/好物新增数据同步
	:param start_time:
	:param end_time:
	:param limit:
	:param offset:
	:return:
	"""
	incr_comm_sql = """select id, title, state, publish_date, review_num, collect_num, edit_date
						from topic
						where publish_date >= '{start_time}'
						and publish_date < '{end_time}' limit {l} offset {o}"""
	incr_sql = incr_comm_sql.format(start_time=start_time, end_time=end_time, l=limit, o=offset)

	selectMysqlClient = MysqlClient()
	result = selectMysqlClient.getMany(incr_sql, ROW_COUNT, None, False)

	while result:
		logger.info("update result num: %s" % len(result))
		offset += limit
		value_list = list(result)
		logger.info("values_list: %s", value_list)
		# 写入数据
		insert_sql = """
		insert into {table}
		(id, title, state, publish_date, review_num, collect_num, edit_date)
		values
		(%s, %s, %s, %s, %s, %s, %s)
		""".format(table=target_table)

		masterMysqlClient = MysqlClient(mode="master")
		try:
			res = masterMysqlClient.insertMany(insert_sql, value_list)
			logger.info("save msg count: %s", res)
		except Exception as e:
			masterMysqlClient.close()
			masterMysqlClient = MysqlClient(mode="master")
			logger.warn("save insert error: %s, try one by one insert", str(e))
			try:
				for d in value_list:
					res = masterMysqlClient.insertMany(insert_sql, [d], False)
					logger.info("save one by one(%s) res: %s", d, res)
				masterMysqlClient.close()
			except Exception as e:
				masterMysqlClient.close()
				logger.warn("save one by one err: %s", str(e))

		new_sql = incr_comm_sql.format(start_time=start_time, end_time=end_time, l=limit, o=offset)
		result = selectMysqlClient.getMany(new_sql, ROW_COUNT, None, False)

	selectMysqlClient.close()

def update(start_time, end_time, limit, offset):
	"""
	desc: 百科/好物变化数据同步
	:param start_time:
	:param end_time:
	:param limit:
	:param offset:
	:return:
	"""

	selectMysqlClient = MysqlClient()
	up_comm = """select id, title, state, publish_date,review_num, collect_num, edit_date
				 from topic
				 where edit_date >= '{start_time}'
				 and edit_date < '{end_time}' limit {l} offset {o}"""
	up_sql = up_comm.format(start_time=start_time, end_time=end_time, l=limit, o=offset)

	logger.info("up_sql: %s", up_sql)
	result = selectMysqlClient.getMany(up_sql, ROW_COUNT, None, False)
	while result:
		logger.info("update result num: %s" % len(result))
		offset += limit
		for (id, title, state, publish_date, review_num, collect_num, edit_date) in result:
			logger.info(
				"update id=%s, title=%s, state=%s, publish_date=%s, review_num=%s, collect_num=%s, edit_date=%s",
				id, title, state, publish_date, review_num, collect_num, edit_date)

			update_sql = u"""
			update {table}
			set title='{title}', state={state}, publish_date='{publish_date}',
			review_num={review_num}, collect_num={collect_num}, edit_date='{edit_date}'
			where id={id}
			""".format(table=target_table, title=title, state=state, publish_date=publish_date, review_num=review_num, collect_num=collect_num, edit_date=edit_date, id=id)
			try:
				MysqlClient(mode="master").update(update_sql)
			except Exception as e:
				logger.warn("update_sql: %s, err: %s", update_sql, str(e))

		up_sql = up_comm.format(start_time=start_time, end_time=end_time, l=limit, o=offset)
		result = selectMysqlClient.getMany(up_sql, ROW_COUNT, None, False)

	selectMysqlClient.close()
