# -*- coding: utf-8 -*-
import logging
from util.mysql import MysqlClient
from base.config import Config

logger = logging.getLogger(__name__)


def save(value_list):
	# 将数据写入同步表中
	tbl = Config["master.recommend_tbl"]
	if value_list:
		sync_sql = u"""insert into {tbl} (article_id, channel_id, article_channel, channel, level1_ids, level2_ids, level3_ids,
										level4_ids, tag_ids, brand_ids, sync_home, is_top, machine_report, publish_time,
										sync_home_time, sync_time, status, title, comment_count, collection_count,
										praise, sum_collect_comment, mall, brand, digital_price, worthy, unworthy, mall_id)
										VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
										%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""".format(tbl=tbl)
		
		masterMysqlClient = MysqlClient(mode="master")
		res = 0
		try:
			res = masterMysqlClient.insertMany(sync_sql, value_list)
			logger.info("save msg count: %s", res)
		except Exception as e:
			masterMysqlClient.close()
			masterMysqlClient = MysqlClient(mode="master")
			logger.warn("save insert error: %s, try one by one insert", str(e))
			try:
				for d in value_list:
					res = masterMysqlClient.insertMany(sync_sql, [d], False)
					logger.info("save one by one(%s) res: %s", d, res)
				masterMysqlClient.close()
			except Exception as e:
				masterMysqlClient.close()
				logger.warn("save one by one err: %s", str(e))
			
		return res


def get_all_values_by_one_field(sql=None):
	"""
	desc: 将sql中单个字段的所有记录的值拼接起来并返回
	:param sql:
	:return:
	"""
	field_list = []
	if sql:
		mysqlClient = MysqlClient()
		result = mysqlClient.getAll(sql)
		if result:
			for (fid,) in result:
				field_list.append(str(fid))
	f_ids = ','.join(set(field_list)) if field_list else ''
	return f_ids


def get_all_values_by_four_field(sql=None):
	"""
	desc: 将sql中四个字段的所有记录的值拼接起来并返回
	:param sql:
	:return:
	"""
	id1_list = []
	id2_list = []
	id3_list = []
	id4_list = []
	
	if sql:
		mysqlClient = MysqlClient()
		result = mysqlClient.getAll(sql)
		if result:
			for (l1, l2, l3, l4) in result:
				id1_list.append(str(l1))
				id2_list.append(str(l2))
				id3_list.append(str(l3))
				id4_list.append(str(l4))
	f_id1s = ','.join(set(id1_list)) if id1_list else ''
	f_id2s = ','.join(set(id2_list)) if id2_list else ''
	f_id3s = ','.join(set(id3_list)) if id3_list else ''
	f_id4s = ','.join(set(id4_list)) if id4_list else ''
	
	return f_id1s, f_id2s, f_id3s, f_id4s
