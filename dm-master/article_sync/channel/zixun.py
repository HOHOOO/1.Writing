# -*- coding: utf-8 -*-
import logging
from datetime import datetime
from util.consts import *
from util.utils import get_start_end_time
from util.mysql import MysqlClient
from channel.comm import save, get_all_values_by_four_field, get_all_values_by_one_field
from base.config import Config

logger = logging.getLogger(__name__)


def zixun(start_time=None, end_time=None):
	"""
		desc: 计算同步的开始，结束时间。 若输入开始和结束时间则为输入时间; 否则默认开始时间为6小时前，结束时间为当前时间
		:param start_time:
		:param end_time:
		:return:
	"""
	start_time, end_time = get_start_end_time(ZX_SYNC_LAST_TIME_PATH, start_time, end_time)
	
	if start_time == end_time:
		logger.info("zixun start time equal end time return.")
		return
	limit = ROW_COUNT
	offset = 0
	# 新增数据同步
	insert(start_time, end_time, limit, offset)
	
	# 更新数据同步
	update(start_time, end_time, limit, offset)
	
	# 更新运营位的数据
	update_oper(start_time)


def insert(start_time, end_time, limit, offset):
	"""
	desc: 资讯新增数据同步
	:param start_time:
	:param end_time:
	:param limit:
	:param offset:
	:return:
	"""
	incr_comm_sql = """select id, news_pub_date, news_title,news_brand,news_mall,worthy,unworthy from smzdm_news where news_pub_date >= '{start_time}'
						and news_pub_date < '{end_time}' and news_status = 1 limit {l} offset {o}"""
	incr_sql = incr_comm_sql.format(start_time=start_time, end_time=end_time, l=limit, o=offset)
	
	selectMysqlClient = MysqlClient()
	result = selectMysqlClient.getMany(incr_sql, ROW_COUNT, None, False)
	
	while result:
		offset += limit
		value_list = []
		
		for (t_article_id, t_publish_time, t_title, t_brand, t_mall, t_worthy, t_unworthy) in result:
			sync_home = 0
			is_top = 0
			machine_report = 0
			publish_time = t_publish_time
			sync_home_time = ''
			sync_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			status = 0
			logger.info("article_id=%s, publish_time=%s", t_article_id, publish_time)
			
			# 品类，未上线，暂时不处理
			cate_sql = None
			(level1_ids, level2_ids, level3_ids, level4_ids) = get_all_values_by_four_field(cate_sql)
			
			# 一个文章有多个品牌的拼接
			brand_sql = """select brand_id from smzdm_brand_item where blog_id={aid} and type=6""".format(aid=t_article_id)
			brand_ids = get_all_values_by_one_field(brand_sql)
			
			# 一个文章有多个标签id
			tag_sql = """select tag_id from smzdm_tag_type_item where blog_id={aid} and type = 6""".format(aid=t_article_id)
			tag_ids = get_all_values_by_one_field(tag_sql)
			
			# 查看是否为编辑同步到首页
			# home_sql = """select is_write_post,set_auto_sync,is_write_post_time,is_home_top from smzdm_news where id={aid}""".format(
			# 		aid=t_article_id)
			# home_result = selectMysqlClient.getAll(home_sql, None, False)
			# if home_result:
			# 	if home_result[0][0] > 0:   # 立即同步主站标识
			# 		sync_home = home_result[0][0]
			# 	elif home_result[0][1] > 0:     # 自动同步主站标识
			# 		sync_home = home_result[0][1]
			# 	sync_home_time = home_result[0][2]
			# 	is_top = home_result[0][3]
			#
			t_comment_count = 0
			t_collection_count = 0
			t_praise = 0
			t_sum_collect_comment = 0
			t_digital_price = ''
			t_mall_id = 0
			t_title = t_title if t_title else ''
			t_brand = t_brand if t_brand else ''
			t_mall = t_mall if t_mall else ''
			t_worthy = t_worthy if t_worthy else 0
			t_unworthy = t_unworthy if t_unworthy else 0
			
			value_list.append((t_article_id, ZIXUN_CHANNEL_ID, "%s:%s" % (t_article_id, ZIXUN_CHANNEL_ID),
			                   ZIXUN_CHANNEL, level1_ids, level2_ids, level3_ids, level4_ids,
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
	desc: 资讯变化数据同步， 资讯只同步首页的数据，非首页的数据不同步
	:param start_time:
	:param end_time:
	:param limit:
	:param offset:
	:return:
	"""
	tbl = Config["master.recommend_tbl"]
	selectMysqlClient = MysqlClient()
	up_comm = """select id,is_write_post,set_auto_sync,is_write_post_time,is_home_top,news_title,news_brand,news_mall,worthy,unworthy  from smzdm_news
					where is_write_post_time >= '{start_time}' and is_write_post_time  < '{end_time}'
					 limit {l} offset {o}"""
	up_sql = up_comm.format(start_time=start_time, end_time=end_time, l=limit, o=offset)
	logger.info("up_sql: %s", up_sql)
	result = selectMysqlClient.getMany(up_sql, ROW_COUNT, None, False)
	while result:
		offset += limit
		for (t_article_id, t_write_post, t_auto_sync, t_post_time, t_is_home_top, t_title, t_brand, t_mall, t_worthy, t_unworthy) in result:
			logger.info("update t_article_id=%s, t_write_post=%s, t_auto_sync=%s, t_post_time=%s, t_is_home_top= %s",
			            t_article_id, t_write_post, t_auto_sync, t_post_time, t_is_home_top)
			
			sync_home = 0
			if t_write_post > 0:
				sync_home = t_write_post
			elif t_auto_sync > 0:
				sync_home = t_auto_sync
				
			is_top = t_is_home_top
			sync_home_time = t_post_time if t_post_time else DEFAULT_TIME
			
			# brand_id
			brand_sql = """select brand_id from smzdm_brand_item where blog_id={aid} and type=6""".format(
				aid=t_article_id)
			brand_ids = get_all_values_by_one_field(brand_sql)
			
			# 四级品类，未上线，暂时不处理
			cate_sql = None
			(level1_ids, level2_ids, level3_ids, level4_ids) = get_all_values_by_four_field(cate_sql)
			
			# 标签
			tag_sql = """select tag_id from smzdm_tag_type_item where blog_id={aid} and type = 6""".format(
				aid=t_article_id)
			tag_ids = get_all_values_by_one_field(tag_sql)
			
			# if level1_ids or level2_ids or level3_ids or level4_ids or tag_ids or brand_ids or sync_home or is_top or sync_home_time:
			# 	home_article_up_sql = """update home_article set level1_ids='{l1}', level2_ids='{l2}',  level3_ids = '{l3}',
			# 									  level4_ids = '{l4}', tag_ids = '{ti}', brand_ids = '{bi}', sync_home = '{sh}',
			# 									  is_top = '{it}', sync_home_time= '{sht}' where article_id = {aid}
			# 									  and channel='{c}'""".format(l1=level1_ids, l2=level2_ids,
			# 	                                                              l3=level3_ids,
			# 	                                                              l4=level4_ids, ti=tag_ids, bi=brand_ids,
			# 	                                                              sh=sync_home, it=is_top,
			# 	                                                              sht=sync_home_time, aid=t_article_id,
			# 	                                                              c=ZIXUN_CHANNEL)
			#
			# 	MysqlClient(mode="master").update(home_article_up_sql)
			
			# if level1_ids or level2_ids or level3_ids or level4_ids or tag_ids or brand_ids:
			home_article_up_sql = u"""update {tbl} set level1_ids='{l1}', level2_ids='{l2}',  level3_ids = '{l3}',
											  level4_ids = '{l4}', tag_ids = '{ti}', brand_ids = '{bi}',
											   title='{title}',
												brand='{brand}',
												mall='{mall}',
												worthy={worthy},
												unworthy={unworthy},
											   where article_id = {aid}
											  and channel='{c}'""".format(tbl=tbl, l1=level1_ids, l2=level2_ids,
			                                                              l3=level3_ids,
			                                                              l4=level4_ids, ti=tag_ids, bi=brand_ids,
			                                                              title=t_title if t_title else '',
			                                                              brand=t_brand if t_brand else '',
			                                                              mall=t_mall if t_mall else '',
			                                                              orthy=t_worthy if t_worthy else 0,
			                                                              nworthy=t_unworthy if t_unworthy else 0,
			                                                              aid=t_article_id, c=ZIXUN_CHANNEL)
			try:
				MysqlClient(mode="master").update(home_article_up_sql)
			except Exception as e:
				logger.warn("home_article_up_sql: %s, err: %s", home_article_up_sql, str(e))
		
		up_sql = up_comm.format(start_time=start_time, end_time=end_time, l=limit, o=offset)
		result = selectMysqlClient.getMany(up_sql, ROW_COUNT, None, False)
	
	selectMysqlClient.close()
	
	
def update_oper(start_time):
	"""
	desc: 更新运营位的数据信息，
	:return:
	"""
	
	tbl = Config["master.recommend_tbl"]
	selectMysqlClient = MysqlClient()
	up_comm = """select article_id, article_channel from zdmdb_client where article_channel in (3,6,7,11,14,31,38,48)
					and '{start_time}' between start_date and end_date"""
	up_sql = up_comm.format(start_time=start_time)
	logger.info("update_oper: %s", up_sql)
	result = selectMysqlClient.getAll(up_sql)
	if result:
		for (t_article_id, t_article_channel) in result:
			logger.info("update t_article_id=%s, t_article_channel=%s", t_article_id, t_article_channel)
			home_article_up_sql = u"""update {tbl} set status=1
												   where article_id = {aid}
												  and channel_id={c}""".format(tbl=tbl, aid=t_article_id, c=t_article_channel)
			try:
				MysqlClient(mode="master").update(home_article_up_sql)
			except Exception as e:
				logger.warn("home_article_up_sql: %s, err: %s", home_article_up_sql, str(e))
