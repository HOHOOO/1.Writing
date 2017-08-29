# -*- coding: utf-8 -*-
import logging
from datetime import datetime
from util.consts import *
from util.utils import get_start_end_time
from util.mysql import MysqlClient
from channel.comm import save, get_all_values_by_one_field
from base.config import Config

logger = logging.getLogger(__name__)


def shipin(start_time=None, end_time=None):
	"""
		desc: 计算同步的开始，结束时间。 若输入开始和结束时间则为输入时间; 否则默认开始时间为6小时前，结束时间为当前时间
		:param start_time:
		:param end_time:
		:return:
	"""
	start_time, end_time = get_start_end_time(SP_SYNC_LAST_TIME_PATH, start_time, end_time)
	
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
	desc: 视频新增数据同步
	:param start_time:
	:param end_time:
	:param limit:
	:param offset:
	:return:
	"""
	incr_comm_sql = """select id, publish_date, title, comment_count, collection_count, worthy, unworthy, sum_collect_comment from v_video where publish_date >= '{start_time}'
						and publish_date < '{end_time}' and status = 3 and is_delete = 0 limit {l} offset {o}"""
	incr_sql = incr_comm_sql.format(start_time=start_time, end_time=end_time, l=limit, o=offset)
	
	logger.info("incr_sql: %s", incr_sql)
	selectMysqlClient = MysqlClient()
	result = selectMysqlClient.getMany(incr_sql, ROW_COUNT, None, False)
	
	while result:
		offset += limit
		value_list = []
		
		for (t_article_id, t_publish_time, t_title, t_comment_count, t_collection_count, t_worthy, t_unworthy, t_sum_collect_comment) in result:
			sync_home = 0
			is_top = 0
			machine_report = 0
			publish_time = t_publish_time
			sync_home_time = ''
			sync_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			status = 0
			logger.info("article_id=%s, publish_time=%s", t_article_id, publish_time)
			
			# 视频有自己的二级分类，暂时用不到，不处理
			level1_ids = ''
			level2_ids = ''
			level3_ids = ''
			level4_ids = ''
			t_praise = 0
			t_mall = ''
			t_brand = ''
			t_digital_price = ''
			t_mall_id = 0
			t_title = t_title if t_title else ''
			t_comment_count = t_comment_count if t_comment_count else 0
			t_collection_count = t_collection_count if t_collection_count else 0
			t_worthy = t_worthy if t_worthy else 0
			t_unworthy = t_unworthy if t_unworthy else 0
			t_sum_collect_comment = t_sum_collect_comment if t_sum_collect_comment else 0
			
			# 一个文章有多个品牌的拼接
			brand_sql = """select brand_id from v_brand_item where video_id={aid}""".format(aid=t_article_id)
			brand_ids = get_all_values_by_one_field(brand_sql)
			
			# 一个文章有多个标签id
			tag_sql = """select tag_id from v_tag_item where video_id={aid}""".format(aid=t_article_id)
			tag_ids = get_all_values_by_one_field(tag_sql)
			
			# 查看是否为编辑同步到首页
			# 此部分只同步数据，同步的首页的状态在home.py中
			# home_sql = """select set_auto_sync,is_write_post_time,is_home_top from v_video_extend where video_id={aid}""".format(
			# 		aid=t_article_id)
			# home_result = selectMysqlClient.getAll(home_sql, None, False)
			# if home_result:
			# 	sync_home = home_result[0][0]
			# 	sync_home_time = home_result[0][1]
			# 	is_top = home_result[0][2]
			
			value_list.append((t_article_id, SHIPIN_CHANNEL_ID, "%s:%s" % (t_article_id, SHIPIN_CHANNEL_ID),
			                   SHIPIN_CHANNEL, level1_ids, level2_ids, level3_ids, level4_ids,
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
	desc: 视频变化数据同步， 视频只同步首页的数据，非首页的数据不同步
	:param start_time:
	:param end_time:
	:param limit:
	:param offset:
	:return:
	"""
	tbl = Config["master.recommend_tbl"]
	selectMysqlClient = MysqlClient()
	up_comm = """select id,title, comment_count, collection_count, worthy, unworthy, sum_collect_comment from v_video where modification_date >= '{start_time}'
					and modification_date  < '{end_time}' and status = 3 and is_delete = 0
					 limit {l} offset {o}"""
	up_sql = up_comm.format(start_time=start_time, end_time=end_time, l=limit, o=offset)
	logger.info("up_sql: %s", up_sql)
	result = selectMysqlClient.getMany(up_sql, ROW_COUNT, None, False)
	while result:
		offset += limit
		for (t_article_id, t_title, t_comment_count, t_collection_count, t_worthy, t_unworthy, t_sum_collect_comment) in result:
			logger.info("update t_article_id=%s", t_article_id)
			
			sync_home = 0
			
			is_top = 0
			sync_home_time = DEFAULT_TIME
			
			# 视频有自己的二级分类，暂时用不到，不处理
			level1_ids = ''
			level2_ids = ''
			level3_ids = ''
			level4_ids = ''
			
			# brand_id
			brand_sql = """select brand_id from v_brand_item where video_id={aid}""".format(
					aid=t_article_id)
			brand_ids = get_all_values_by_one_field(brand_sql)
			
			# 标签
			tag_sql = """select tag_id from v_tag_item where video_id={aid}""".format(
					aid=t_article_id)
			tag_ids = get_all_values_by_one_field(tag_sql)
			
			# 查看是否为编辑同步到首页
			# 这里只更新标签，品牌，品类属性，同步到首页的状态放到了home.py中
			# home_sql = """select set_auto_sync,is_write_post_time,is_home_top from v_video_extend where video_id={aid}""".format(
			# 		aid=t_article_id)
			# home_result = selectMysqlClient.getAll(home_sql, None, False)
			# if home_result:
			# 	sync_home = home_result[0][0]
			# 	sync_home_time = home_result[0][1]
			# 	is_top = home_result[0][2]
			
			# if level1_ids or level2_ids or level3_ids or level4_ids or tag_ids or brand_ids or sync_home or is_top or sync_home_time:
			# 	home_article_up_sql = """update home_article set level1_ids='{l1}', level2_ids='{l2}',  level3_ids = '{l3}',
			# 									  level4_ids = '{l4}', tag_ids = '{ti}', brand_ids = '{bi}', sync_home = '{sh}',
			# 									  is_top = '{it}', sync_home_time= '{sht}' where article_id = {aid}
			# 									  and channel='{c}'""".format(l1=level1_ids, l2=level2_ids,
			# 	                                                              l3=level3_ids,
			# 	                                                              l4=level4_ids, ti=tag_ids, bi=brand_ids,
			# 	                                                              sh=sync_home, it=is_top,
			# 	                                                              sht=sync_home_time, aid=t_article_id,
			# 	                                                              c=SHIPIN_CHANNEL)
			#
			# 	MysqlClient(mode="master").update(home_article_up_sql)
			
			home_article_up_sql = u"""update {tbl} set level1_ids='{l1}', level2_ids='{l2}',  level3_ids = '{l3}',
											  level4_ids = '{l4}', tag_ids = '{ti}', brand_ids = '{bi}',
											   title='{title}',
												comment_count={comment_count},
												collection_count={collection_count},
												worthy={worthy},
												unworthy={unworthy},
												sum_collect_comment={sum_collect_comment}
											   where article_id = {aid}
											  and channel='{c}'""".format(tbl=tbl, l1=level1_ids, l2=level2_ids,
			                                                              l3=level3_ids,
			                                                              l4=level4_ids, ti=tag_ids, bi=brand_ids,
			                                                              title=t_title if t_title else '',
			                                                              comment_count=t_comment_count if t_comment_count else 0,
			                                                              collection_count=t_collection_count if t_collection_count else 0,
			                                                              worthy=t_worthy if t_worthy else 0,
			                                                              unworthy=t_unworthy if t_unworthy else 0,
			                                                              sum_collect_comment=t_sum_collect_comment if t_sum_collect_comment else 0,
			                                                              aid=t_article_id, c=SHIPIN_CHANNEL)
			
			try:
				MysqlClient(mode="master").update(home_article_up_sql)
			except Exception as e:
				logger.warn("home_article_up_sql: %s, err: %s", home_article_up_sql, str(e))
		
		up_sql = up_comm.format(start_time=start_time, end_time=end_time, l=limit, o=offset)
		result = selectMysqlClient.getMany(up_sql, ROW_COUNT, None, False)
	
	selectMysqlClient.close()
