# -*- coding: utf-8 -*-
import logging
from datetime import datetime
from util.consts import *
from util.utils import get_start_end_time
from util.mysql import MysqlClient
from channel.comm import save, get_all_values_by_one_field
from base.config import Config

logger = logging.getLogger(__name__)


def xinruipinpai(start_time=None, end_time=None):
	"""
		desc: 计算同步的开始，结束时间。 若输入开始和结束时间则为输入时间; 否则默认开始时间为6小时前，结束时间为当前时间
		:param start_time:
		:param end_time:
		:return:
	"""
	start_time, end_time = get_start_end_time(XR_SYNC_LAST_TIME_PATH, start_time, end_time)
	
	if start_time == end_time:
		logger.info("zixun start time equal end time return.")
		return
	limit = ROW_COUNT
	offset = 0
	# 新增数据同步
	insert(start_time, end_time, limit, offset)
	
	# 更新数据同步
	update(start_time, end_time, limit, offset)


def insert(start_time, end_time, limit, offset):
	"""
	desc: 新锐品牌新增数据同步
	:param start_time:
	:param end_time:
	:param limit:
	:param offset:
	:return:
	"""
	incr_comm_sql = """select id, pub_time,push_home,editdate,title,review_num,collect_num from brand_special where pub_time >= '{start_time}'
						and pub_time < '{end_time}' and status = 1 limit {l} offset {o}"""
	incr_sql = incr_comm_sql.format(start_time=start_time, end_time=end_time, l=limit, o=offset)
	
	selectMysqlClient = MysqlClient()
	result = selectMysqlClient.getMany(incr_sql, ROW_COUNT, None, False)
	
	while result:
		offset += limit
		value_list = []
		
		for (t_article_id, t_publish_time, t_is_top, t_sync_home_time, t_title, t_comment_count, t_collection_count) in result:
			machine_report = 0
			sync_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			status = 0
			logger.info("article_id=%s, publish_time=%s", t_article_id, t_publish_time)
			
			# 新锐品牌没有品类和标签
			level1_ids = ''
			level2_ids = ''
			level3_ids = ''
			level4_ids = ''
			tag_ids = ''
			
			t_praise = 0
			t_sum_collect_comment = 0
			t_mall = ''
			t_brand = ''
			t_digital_price = ''
			t_worthy = 0
			t_unworthy = 0
			t_mall_id = 0
			t_title = t_title if t_title else ''
			t_comment_count = t_comment_count if t_comment_count else 0
			t_collection_count = t_collection_count if t_collection_count else 0
			
			# 一个文章有多个品牌的拼接
			brand_sql = """select brand_id from brand_special_item where item_id = {aid}""".format(
				aid=t_article_id)
			brand_ids = get_all_values_by_one_field(brand_sql)
			
			value_list.append((t_article_id, XINRUI_CHANNEL_ID, "%s:%s" % (t_article_id, XINRUI_CHANNEL_ID),
			                   XINRUI_CHANNEL, level1_ids, level2_ids, level3_ids, level4_ids,
			                   tag_ids, brand_ids,
			                   # t_is_top, t_is_top, machine_report,
			                   0, 0, machine_report,
			                   t_publish_time if t_publish_time else DEFAULT_TIME,
			                   sync_time,
			                   sync_time,
			                   status,
			                   t_title, t_comment_count, t_collection_count, t_praise, t_sum_collect_comment, t_mall,
			                   t_brand, t_digital_price, t_worthy, t_unworthy, t_mall_id
			                   )
			                  )
		
		logger.info("values_list: %s", value_list)
		# 写入数据
		save(value_list)
		
		new_sql = incr_comm_sql.format(start_time=start_time, end_time=end_time, l=limit, o=offset)
		result = selectMysqlClient.getMany(new_sql, ROW_COUNT, None, False)
	
	selectMysqlClient.close()


def update(start_time, end_time, limit, offset):
	"""
	desc: 新锐品牌变化数据同步， 新锐品牌只同步首页的数据，非首页的数据不同步
	:param start_time:
	:param end_time:
	:param limit:
	:param offset:
	:return:
	"""
	tbl = Config["master.recommend_tbl"]
	selectMysqlClient = MysqlClient()
	up_comm = """select id, push_home,editdate,title,review_num,collect_num from brand_special
					where editdate >= '{start_time}' and editdate  < '{end_time}'
					 limit {l} offset {o}"""
	up_sql = up_comm.format(start_time=start_time, end_time=end_time, l=limit, o=offset)
	logger.info("up_sql: %s", up_sql)
	result = selectMysqlClient.getMany(up_sql, ROW_COUNT, None, False)
	while result:
		offset += limit
		for (t_article_id, t_sync_home, t_sync_home_time, t_title, t_comment_count, t_collection_count) in result:
			logger.info("update t_article_id=%s, t_sync_home=%s, t_sync_home_time=%s",
			            t_article_id, t_sync_home, t_sync_home_time)
			
			# 新锐品牌没有品类和标签
	
			# 一个文章有多个品牌的拼接
			brand_sql = """select brand_id from brand_special_item where item_id = {aid}""".format(
					aid=t_article_id)
			brand_ids = get_all_values_by_one_field(brand_sql)
			
			t_title = t_title if t_title else ''
			t_comment_count = t_comment_count if t_comment_count else 0
			t_collection_count = t_collection_count if t_collection_count else 0
			
			# if brand_ids:
			home_article_up_sql = u"""update {tbl} set brand_ids = '{bi}',
                                            title='{title}',
											comment_count={comment_count},
											collection_count={collection_count}
                                            where article_id = {aid}
											  and channel='{c}'""".format(tbl=tbl, bi=brand_ids,
			                                                              title=t_title,
			                                                              comment_count=t_comment_count,
			                                                              collection_count=t_collection_count,
			                                                              aid=t_article_id,
			                                                              c=XINRUI_CHANNEL)
			
			try:
				MysqlClient(mode="master").update(home_article_up_sql)
			except Exception as e:
				logger.warn("home_article_up_sql: %s, err: %s", home_article_up_sql, str(e))
		
		up_sql = up_comm.format(start_time=start_time, end_time=end_time, l=limit, o=offset)
		result = selectMysqlClient.getMany(up_sql, ROW_COUNT, None, False)
	
	selectMysqlClient.close()
