# -*- coding: utf-8 -*-
import logging
from datetime import datetime
from util.consts import *
from util.mysql import MysqlClient
from util.utils import get_start_end_time
from channel.comm import save, get_all_values_by_one_field, get_all_values_by_four_field
from base.config import Config

logger = logging.getLogger(__name__)


def youhui(start_time=None, end_time=None):
	"""
		desc: 计算同步的开始，结束时间。 若输入开始和结束时间则为输入时间; 否则默认开始时间为6小时前，结束时间为当前时间
		:param start_time:
		:param end_time:
		:return:
	"""
	start_time, end_time = get_start_end_time(YH_SYNC_LAST_TIME_PATH, start_time, end_time)
	
	if start_time == end_time:
		logger.info("youhui start time equal end time return.")
		return
	
	limit = ROW_COUNT
	offset = 0
	# 新增数据同步
	insert(start_time, end_time, limit, offset)
	
	# 更新数据同步
	update(start_time, end_time, limit, offset)


def insert(start_time, end_time, limit, offset):
	# 新增
	new_comm = """select id,choiceness_date,brand_id,channel, comment_count, collection_count, praise,
						sum_collect_comment, mall, brand, digital_price, worthy, unworthy, mall_id
						from youhui where yh_status = 1 and channel in (1, 5)
						and choiceness_date >= '{start_time}'
						and choiceness_date  < '{end_time}'
	                  limit {l} offset {o} """
	
	new_sql = new_comm.format(start_time=start_time, end_time=end_time, l=limit, o=offset)
	
	logger.info("youhui new_sql is: %s", new_sql)
	
	slaveMysqlClient = MysqlClient()
	new_result = slaveMysqlClient.getMany(new_sql, ROW_COUNT, None, False)
	
	while new_result:
		offset += limit
		value_list = []
		
		for (t_article_id, t_publish_time, t_brand_id, t_channel, t_comment_count, t_collection_count, t_praise, t_sum_collect_comment, t_mall, t_brand, t_digital_price, t_worthy, t_unworthy, t_mall_id) in new_result:
			brand_ids = t_brand_id
			sync_home = 0
			is_top = 0
			editor_report = 0
			publish_time = t_publish_time
			sync_home_time = ''
			title = ''
			sync_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			status = 0
			logger.info("article_id=%s, publish_time=%s, brand_id=%s, channel=%s", t_article_id, publish_time, t_brand_id,
			            t_channel)
			
			# 一个文章有多个类别的拼接
			cate_sql = """select top_category_id,second_category_id,third_category_id,fourth_category_id
								from youhui_category where youhui_id={aid}""".format(aid=t_article_id)
			(level1_ids, level2_ids, level3_ids, level4_ids) = get_all_values_by_four_field(cate_sql)
			
			# 一个文章有多个标签的拼接
			tag_sql = """select tag_id from youhui_tag_type_item where article_id={aid} and channel_id = 1""".format(aid=t_article_id)
			tag_ids = get_all_values_by_one_field(tag_sql)
			
			# 查看是否为编辑同步到首页
			home_sql = """select sync_home,sync_home_time,is_home_top,stock_status, title from youhui_extend where id ={article_id}""".format(
				article_id=t_article_id)
			home_result = slaveMysqlClient.getAll(home_sql, None, False)
			if home_result:
				# sync_home = home_result[0][0]
				# sync_home_time = home_result[0][1]
				# is_top = home_result[0][2]
				editor_report = EDITOR_STOCK_STATUS_MAP.get(int(home_result[0][3]), 0)
				title = home_result[0][4]
			
			value_list.append((t_article_id, YOUHUI_CHANNEL_ID, "%s:%s" % (t_article_id, YOUHUI_CHANNEL_ID),
			                   YOUHUI_CHANNEL, level1_ids, level2_ids, level3_ids, level4_ids,
			                   tag_ids, str(brand_ids),
			                   sync_home, is_top, editor_report,
			                   publish_time if publish_time else DEFAULT_TIME,
			                   sync_time,
			                   sync_time,
			                   status,
			                   title, t_comment_count, t_collection_count, t_praise, t_sum_collect_comment, t_mall,
			                   t_brand, t_digital_price, t_worthy, t_unworthy, t_mall_id
			                   )
			                  )
		
		logger.info("values_list: %s", value_list)
		
		# 写入数据
		save(value_list)
		
		new_sql = new_comm.format(start_time=start_time, end_time=end_time, l=limit, o=offset)
		new_result = slaveMysqlClient.getMany(new_sql, ROW_COUNT, None, False)
	
	slaveMysqlClient.close()


def update(start_time, end_time, limit, offset):
	tbl = Config["master.recommend_tbl"]
	selectMysqlClient = MysqlClient()
	
	up_comm = """select id, brand_id, comment_count, collection_count, praise,
						sum_collect_comment, mall, brand, digital_price, worthy, unworthy, mall_id
						 from youhui where update_timestamp >= '{start_time}'
							and update_timestamp  < '{end_time}' limit {l} offset {o}"""
	
	up_sql = up_comm.format(start_time=start_time, end_time=end_time, l=limit, o=offset)
	logger.info("up_sql: %s", up_sql)
	up_result = selectMysqlClient.getMany(up_sql, ROW_COUNT, None, False)
	while up_result:
		offset += limit
		# t_brand_id: 品牌id
		for (t_article_id, t_brand_id, t_comment_count, t_collection_count, t_praise, t_sum_collect_comment, t_mall, t_brand, t_digital_price, t_worthy, t_unworthy, t_mall_id) in up_result:
			logger.info("t_article_id=%s, t_brand_id=%s, t_comment_count=%s, t_collection_count=%s, t_praise=%s, t_sum_collect_comment=%s, t_mall=%s, t_brand=%s, t_digital_price=%s, t_worthy=%s, t_unworthy=%s, t_mall_id=%s",
				t_article_id, t_brand_id, t_comment_count, t_collection_count, t_praise, t_sum_collect_comment, t_mall,
				t_brand, t_digital_price, t_worthy, t_unworthy, t_mall_id)
			# 查看是否为编辑同步到首页
			# sync_home = 0
			# is_top = 0
			# sync_home_time = DEFAULT_TIME
			editor_report = 0
			title = ''
			home_sql = """select sync_home,sync_home_time,is_home_top,stock_status, title from youhui_extend where id ={article_id}""".format(
				article_id=t_article_id)
			home_result = selectMysqlClient.getAll(home_sql, None, False)
			if home_result:
				# sync_home = home_result[0][0]
				# sync_home_time = home_result[0][1]
				# is_top = home_result[0][2]
				editor_report = EDITOR_STOCK_STATUS_MAP.get(int(home_result[0][3]), 0)
				title = home_result[0][4]
				
			logger.info("update t_article_id=%s, editor_report=%s", t_article_id, editor_report)
			
			# 四级品类
			cate_sql = """select top_category_id,second_category_id,third_category_id,fourth_category_id
											from youhui_category where youhui_id={aid}""".format(aid=t_article_id)
			(level1_ids, level2_ids, level3_ids, level4_ids) = get_all_values_by_four_field(cate_sql)
			
			# 标签
			tag_sql = """select tag_id from youhui_tag_type_item where article_id={aid} and channel_id = 1""".format(
				aid=t_article_id)
			tag_ids = get_all_values_by_one_field(tag_sql)
			
			# if level1_ids or level2_ids or level3_ids or level4_ids or tag_ids or t_brand_id or editor_report:
			home_article_up_sql = u"""update {tbl} set level1_ids='{l1}', level2_ids='{l2}',  level3_ids = '{l3}',
										  level4_ids = '{l4}', tag_ids = '{ti}', brand_ids = '{bi}',
										  machine_report='{mr}',
										   title='{title}',
											comment_count={comment_count},
											collection_count={collection_count},
											praise={praise},
											sum_collect_comment={sum_collect_comment},
											mall='{mall}',
											brand='{brand}',
											digital_price='{digital_price}',
											worthy={worthy},
											unworthy={unworthy},
											mall_id={mall_id}
										   where article_id = {aid}
											  and channel='{c}'""".format(tbl=tbl, l1=level1_ids, l2=level2_ids,
			                                l3=level3_ids, l4=level4_ids, ti=tag_ids, bi=t_brand_id,
			                                mr=editor_report,
                                          title=title if title else '',
                                          comment_count=t_comment_count if t_comment_count else 0,
                                          collection_count=t_collection_count if t_collection_count else 0,
                                          praise=t_praise if t_praise else 0,
                                          sum_collect_comment=t_sum_collect_comment if t_sum_collect_comment else 0,
                                          mall=t_mall if t_mall else '',
                                          brand=t_brand if t_brand else '',
                                          digital_price=t_digital_price if t_digital_price else '',
                                          worthy=t_worthy if t_worthy else 0,
                                          unworthy=t_unworthy if t_unworthy else 0,
                                          mall_id=t_mall_id if t_mall_id else 0,
			                              aid=t_article_id, c=YOUHUI_CHANNEL)
			
			try:
				MysqlClient(mode="master").update(home_article_up_sql)
			except Exception as e:
				logger.warn("home_article_up_sql: %s, err: %s", home_article_up_sql, str(e))
			
		up_sql = up_comm.format(start_time=start_time, end_time=end_time, l=limit, o=offset)
		up_result = selectMysqlClient.getMany(up_sql, ROW_COUNT, None, False)
	
	selectMysqlClient.close()
