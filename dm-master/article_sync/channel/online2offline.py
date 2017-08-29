# -*- coding: utf-8 -*-
import logging
from util.consts import *
from util.utils import get_start_end_time
from util.mysql import MysqlClient
from channel.comm import save
from base.config import Config

logger = logging.getLogger(__name__)


def online2offline(start_time=None, end_time=None):
	"""
		desc: 推荐系统上线的数据同步到测试环境
		:param start_time:
		:param end_time:
		:return:
	"""
	start_time, end_time = get_start_end_time(ON2OFF_LAST_TIME_PATH, start_time, end_time)
	
	if start_time == end_time:
		logger.info("pingce start time equal end time return.")
		return
	limit = ROW_COUNT
	offset = 0
	# 新增数据同步
	insert(start_time, end_time, limit, offset)
	
	# 更新数据同步
	update(start_time, end_time, limit, offset)


def insert(start_time, end_time, limit, offset):
	"""
	desc: 推荐系统数据同步---新增
	:param start_time:
	:param end_time:
	:param limit:
	:param offset:
	:return:
	"""
	incr_comm_sql = """select article_id, channel_id, article_channel, channel, level1_ids, level2_ids, level3_ids,
										level4_ids, tag_ids, brand_ids, sync_home, is_top, machine_report, publish_time,
										sync_home_time, sync_time, status, title, comment_count, collection_count,
										praise, sum_collect_comment, mall, brand, digital_price, worthy, unworthy, mall_id
						from home_article_online where sync_time >= '{start_time}'
						and sync_time < '{end_time}' limit {l} offset {o}"""
	incr_sql = incr_comm_sql.format(start_time=start_time, end_time=end_time, l=limit, o=offset)
	
	selectMysqlClient = MysqlClient()
	result = selectMysqlClient.getMany(incr_sql, ROW_COUNT, None, False)
	
	while result:
		offset += limit
		save(result)
		
		new_sql = incr_comm_sql.format(start_time=start_time, end_time=end_time, l=limit, o=offset)
		result = selectMysqlClient.getMany(new_sql, ROW_COUNT, None, False)
	
	selectMysqlClient.close()


def update(start_time, end_time, limit, offset):
	"""
	desc: 推荐系统数据同步---更新
	:param start_time:
	:param end_time:
	:param limit:
	:param offset:
	:return:
	"""
	tbl = Config["master.recommend_tbl"]
	selectMysqlClient = MysqlClient()
	up_comm = """select article_id, channel_id, article_channel, channel, level1_ids, level2_ids, level3_ids,
										level4_ids, tag_ids, brand_ids, sync_home, is_top, machine_report, publish_time,
										sync_home_time, sync_time, status, title, comment_count, collection_count,
										praise, sum_collect_comment, mall, brand, digital_price, worthy, unworthy, mall_id
						from home_article_online
					where auto_updatetime >= '{start_time}' and auto_updatetime  < '{end_time}'
					 limit {l} offset {o}"""
	up_sql = up_comm.format(start_time=start_time, end_time=end_time, l=limit, o=offset)
	logger.info("up_sql: %s", up_sql)
	result = selectMysqlClient.getMany(up_sql, ROW_COUNT, None, False)
	while result:
		offset += limit
		for (t_article_id,
				t_channel_id,
				t_article_channel,
				t_channel,
				t_level1_ids,
				t_level2_ids,
				t_level3_ids,
				t_level4_ids,
				t_tag_ids,
				t_brand_ids,
				t_sync_home,
				t_is_top,
				t_machine_report,
				t_publish_time,
				t_sync_home_time,
				t_sync_time,
				t_status,
				t_title,
				t_comment_count,
				t_collection_count,
				t_praise,
				t_sum_collect_comment,
				t_mall,
				t_brand,
				t_digital_price,
				t_worthy,
				t_unworthy,
				t_mall_id) in result:
			logger.info("t_article_id=%s,t_channel_id=%s,t_article_channel=%s,t_channel=%s,t_level1_ids=%s,"
			            "t_level2_ids=%s,t_level3_ids=%s,t_level4_ids=%s,t_tag_ids=%s,t_brand_ids=%s,t_sync_home=%s,"
			            "t_is_top=%s,t_machine_report=%s,t_publish_time=%s,t_sync_home_time=%s,t_sync_time=%s,"
			            "t_status=%s,t_title=%s,t_comment_count=%s,t_collection_count=%s,t_praise=%s,"
			            "t_sum_collect_comment=%s,t_mall=%s,t_brand=%s,t_digital_price=%s,t_worthy=%s,t_unworthy=%s,"
			            "t_mall_id=%s", t_article_id, t_channel_id,t_article_channel,t_channel,t_level1_ids,
			            t_level2_ids,t_level3_ids,t_level4_ids,t_tag_ids,t_brand_ids,t_sync_home,t_is_top,
			            t_machine_report,t_publish_time,t_sync_home_time,t_sync_time,t_status,t_title,t_comment_count,
			            t_collection_count,t_praise,t_sum_collect_comment,t_mall,t_brand,t_digital_price,t_worthy,
			            t_unworthy,t_mall_id)
			
			
			article_up_sql = u"""update {tbl} set channel_id='{channel_id}',
														article_channel='{article_channel}',
														level1_ids='{level1_ids}',
														level2_ids='{level2_ids}',
														level3_ids='{level3_ids}',
														level4_ids='{level4_ids}',
														tag_ids='{tag_ids}',
														brand_ids='{brand_ids}',
														sync_home='{sync_home}',
														is_top='{is_top}',
														machine_report='{machine_report}',
														publish_time='{publish_time}',
														sync_home_time='{sync_home_time}',
														sync_time='{sync_time}',
														status='{status}',
														title='{title}',
														comment_count='{comment_count}',
														collection_count='{collection_count}',
														praise='{praise}',
														sum_collect_comment='{sum_collect_comment}',
														mall='{mall}',
														brand='{brand}',
														digital_price='{digital_price}',
														worthy='{worthy}',
														unworthy='{unworthy}',
														mall_id='{mall_id}'
											   where article_id = {aid}
											  and channel='{c}'""".format(tbl=tbl, channel_id=t_channel_id,
																					article_channel=t_article_channel,
																					level1_ids=t_level1_ids,
																					level2_ids=t_level2_ids,
																					level3_ids=t_level3_ids,
																					level4_ids=t_level4_ids,
																					tag_ids=t_tag_ids,
																					brand_ids=t_brand_ids,
																					sync_home=t_sync_home,
																					is_top=t_is_top,
																					machine_report=t_machine_report,
																					publish_time=t_publish_time,
																					sync_home_time=t_sync_home_time,
																					sync_time=t_sync_time,
																					status=t_status,
																					title=t_title,
																					comment_count=t_comment_count,
																					collection_count=t_collection_count,
																					praise=t_praise,
																					sum_collect_comment=t_sum_collect_comment,
																					mall=t_mall,
																					brand=t_brand,
																					digital_price=t_digital_price,
																					worthy=t_worthy,
																					unworthy=t_unworthy,
																					mall_id=t_mall_id,
			                                                                aid=t_article_id, c=t_channel)
			try:
				MysqlClient(mode="master").update(article_up_sql)
			except Exception as e:
				logger.warn("home_article_up_sql: %s, err: %s", article_up_sql, str(e))
		
		up_sql = up_comm.format(start_time=start_time, end_time=end_time, l=limit, o=offset)
		result = selectMysqlClient.getMany(up_sql, ROW_COUNT, None, False)
	
	selectMysqlClient.close()
