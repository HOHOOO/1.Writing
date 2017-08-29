# -*- coding: utf-8 -*-
import logging
from datetime import datetime
from util.consts import *
from util.utils import get_start_end_time
from util.mysql import MysqlClient
from channel.comm import save, get_all_values_by_four_field, get_all_values_by_one_field
from base.config import Config

logger = logging.getLogger(__name__)


def yuanchuang(start_time=None, end_time=None):
	"""
		desc: 计算同步的开始，结束时间。 若输入开始和结束时间则为输入时间; 否则默认开始时间为6小时前，结束时间为当前时间
		:param start_time:
		:param end_time:
		:return:
	"""
	start_time, end_time = get_start_end_time(YC_SYNC_LAST_TIME_PATH, start_time, end_time)
	
	if start_time == end_time:
		logger.info("yuanchuang start time equal end time return.")
		return
	
	limit = ROW_COUNT
	offset = 0
	# 新增数据同步
	insert(start_time, end_time, limit, offset)
	
	# 更新数据同步
	update(start_time, end_time, limit, offset)
	

def insert(start_time, end_time, limit, offset):
	"""
	desc: 原创新增数据同步
	:param start_time:
	:param end_time:
	:param limit:
	:param offset:
	:return:
	"""
	incr_comm_sql = """select id, publishtime, title, comment_count, brand, mall  from yuanchuang where type in (3,8) and publishtime >= '{start_time}'
 							and publishtime < '{end_time}' and is_delete = 0 limit {l} offset {o}"""
	incr_sql = incr_comm_sql.format(start_time=start_time, end_time=end_time, l=limit, o=offset)
	
	selectMysqlClient = MysqlClient()
	result = selectMysqlClient.getMany(incr_sql, ROW_COUNT, None, False)
	
	while result:
		offset += limit
		value_list = []
		
		for (t_article_id, t_publish_time, t_title, t_comment_count, t_brand, t_mall) in result:
			sync_home = 0
			is_top = 0
			machine_report = 0
			publish_time = t_publish_time
			sync_home_time = ''
			sync_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			status = 0
			t_title = t_title if t_title else ''
			t_comment_count = t_comment_count if t_comment_count else 0
			t_brand = t_brand if t_brand else ''
			t_mall = t_mall if t_mall else ''
			t_collection_count = 0
			t_praise = 0
			t_sum_collect_comment = 0
			t_digital_price = ''
			t_worthy = 0
			t_unworthy = 0
			t_mall_id = 0
			logger.info("article_id=%s, publish_time=%s", t_article_id, publish_time)
			
			# 一个文章有多个类别的拼接
			cate_sql = """select level_first,level_second,level_third,level_four from yuanchuang_category_relation
 								where yc_id = {aid}""".format(aid=t_article_id)
			(level1_ids, level2_ids, level3_ids, level4_ids) = get_all_values_by_four_field(cate_sql)
			
			# 一个文章有多个品牌的拼接
			brand_sql = """select brand_id from yuanchuang_brand_item where blog_id={aid}""".format(aid=t_article_id)
			brand_ids = get_all_values_by_one_field(brand_sql)
			
			# 一个文章有多个标签id
			tag_sql = """select tag_id from yuanchuang_tag_item where blog_id = {aid}""".format(aid=t_article_id)
			tag_ids = get_all_values_by_one_field(tag_sql)
			
			# 查看是否为编辑同步到首页
			# home_sql = """select set_auto_sync,is_write_post_time,is_home_top from yuanchuang_extend where id={aid}""".format(
			# 		aid=t_article_id)
			# home_result = selectMysqlClient.getAll(home_sql, None, False)
			# if home_result:
			# 	sync_home = home_result[0][0]
			# 	sync_home_time = home_result[0][1]
			# 	is_top = home_result[0][2]
			
			value_list.append((t_article_id, YUANCHUANG_CHANNEL_ID, "%s:%s" % (t_article_id, YUANCHUANG_CHANNEL_ID),
			                   YUANCHUANG_CHANNEL, level1_ids, level2_ids, level3_ids, level4_ids,
			                   tag_ids, brand_ids,
			                   sync_home, is_top, machine_report,
			                   publish_time if publish_time else DEFAULT_TIME,
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
	desc: 原创变化数据同步
	:param start_time:
	:param end_time:
	:param limit:
	:param offset:
	:return:
	"""
	tbl = Config["master.recommend_tbl"]
	selectMysqlClient = MysqlClient()
	up_comm = """select id,set_auto_sync,is_write_post_time,is_home_top from yuanchuang_extend
					where updateline >= '{start_time}' and updateline  < '{end_time}' limit {l} offset {o}"""
	up_sql = up_comm.format(start_time=start_time, end_time=end_time, l=limit, o=offset)
	logger.info("up_sql: %s", up_sql)
	result = selectMysqlClient.getMany(up_sql, ROW_COUNT, None, False)
	while result:
		offset += limit
		for (t_article_id, t_sync_home, t_sync_home_time, t_is_home_top) in result:
			logger.info("update t_article_id=%s, t_sync_home=%s, t_sync_home_time=%s, t_is_home_top=%s",
			            t_article_id, t_sync_home, t_sync_home_time, t_is_home_top)
			
			sync_home = t_sync_home
			is_top = t_is_home_top
			sync_home_time = t_sync_home_time if t_sync_home_time else DEFAULT_TIME
			
			# brand_id
			brand_sql = """select brand_id from yuanchuang_brand_item where blog_id={aid}""".format(aid=t_article_id)
			brand_ids = get_all_values_by_one_field(brand_sql)
			
			# 四级品类
			cate_sql = """select level_first,level_second,level_third,level_four from yuanchuang_category_relation
			 								where yc_id = {aid}""".format(aid=t_article_id)
			(level1_ids, level2_ids, level3_ids, level4_ids) = get_all_values_by_four_field(cate_sql)
			
			# 标签
			tag_sql = """select tag_id from yuanchuang_tag_item where blog_id = {aid}""".format(aid=t_article_id)
			tag_ids = get_all_values_by_one_field(tag_sql)
			
			# 更新其它信息
			t_title = ''
			t_comment_count = 0
			t_brand = ''
			t_mall = ''
			yh_sql = """select title, comment_count, brand, mall  from yuanchuang where type in (3,8) and is_delete = 0 and id={aid}""".format(aid=t_article_id)
			yh_data = selectMysqlClient.getAll(yh_sql, None, False)
			if yh_data:
				t_title = yh_data[0][0] if yh_data[0][0] else t_title
				t_comment_count = yh_data[0][1] if yh_data[0][1] else t_comment_count
				t_brand = yh_data[0][2] if yh_data[0][2] else t_brand
				t_mall = yh_data[0][3] if yh_data[0][3] else t_mall
			# if level1_ids or level2_ids or level3_ids or level4_ids or tag_ids or brand_ids:
			home_article_up_sql = u"""update {tbl} set level1_ids='{l1}', level2_ids='{l2}',  level3_ids = '{l3}',
											  level4_ids = '{l4}', tag_ids = '{ti}', brand_ids = '{bi}',
											  title='{title}',
												comment_count={comment_count},
												brand='{brand}',
												mall='{mall}'
											  where article_id = {aid}
											  and channel='{c}'""".format(tbl=tbl, l1=level1_ids, l2=level2_ids, l3=level3_ids,
			                                l4=level4_ids, ti=tag_ids, bi=brand_ids,
                                              title=t_title if t_title else '',
                                              comment_count=t_comment_count if t_comment_count else 0,
                                              brand=t_brand if t_brand else '',
                                              mall=t_mall if t_mall else '',
                                              aid=t_article_id, c=YUANCHUANG_CHANNEL)
			try:
				MysqlClient(mode="master").update(home_article_up_sql)
			except Exception as e:
				logger.warn("home_article_up_sql: %s, err: %s", home_article_up_sql, str(e))
		
		up_sql = up_comm.format(start_time=start_time, end_time=end_time, l=limit, o=offset)
		result = selectMysqlClient.getMany(up_sql, ROW_COUNT, None, False)
	
	selectMysqlClient.close()
