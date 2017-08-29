# -*- coding: utf-8 -*-
import logging
import time
from datetime import datetime, timedelta
from util.consts import *
from util.mysql import MysqlClient
from base.config import Config
from channel.comm import save

logger = logging.getLogger(__name__)


def home(start_time=None):
	"""
		desc: 从首页编辑精选库同步同步首页文章; 总共不超过300篇文章，暂时不增加limit，offset处理
		:param start_time: 同步的开始时间，若输入了时间，则按着输入时间为准； 若没有输入时间，则同步最近1小时数据
		:return:
	"""
	if start_time:
		start_timestamp = int(time.mktime(datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").timetuple()))
	else:
		start_timestamp = int(time.mktime((datetime.now() - timedelta(hours=1)).timetuple()))
	
	logger.info("sync home data start timestamp: %s", start_timestamp)
	
	main(start_timestamp)


def main(start_timestamp):
	tbl = Config["master.recommend_tbl"]
	sql = """select article_id, channel, is_deleted, is_top, time_sort from zdmdb_home
				where substring(time_sort, 1, CHAR_LENGTH(time_sort) -2) >= {ts}""".format(ts=start_timestamp)
	slaveMysqlClient = MysqlClient()
	result = slaveMysqlClient.getAll(sql)
	if result:
		for (t_article_id, t_channel, t_is_delete, t_is_top, t_time_sort) in result:
			sync_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(str(t_time_sort)[0:-2])))
			# wiki数据不需要同步标签，品牌，品类属性
			if int(t_channel) == WIKI_CHANNEL_ID:
				now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
				value_list = [(t_article_id, t_channel, "%s:%s" % (t_article_id, t_channel), WIKI_CHANNEL, '', '', '', '',
					 '', '', 1, t_is_top, EDITOR_STOCK_STATUS_MAP[0], sync_time, sync_time, now, 0, '',0,0,0,0,0,0,0,0,0,0)]
				
				save(value_list)
				continue
				
			# 直播数据不需要同步标签，品牌，品类属性
			if int(t_channel) == ZHIBO_CHANNEL_ID:
				now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
				value_list = [(t_article_id, t_channel, "%s:%s" % (t_article_id, t_channel), ZHIBO_CHANNEL, '', '', '', '',
					 '', '', 1, t_is_top, EDITOR_STOCK_STATUS_MAP[0], sync_time, sync_time, now, 0, '',0,0,0,0,0,0,0,0,0,0)]
				
				save(value_list)
				continue
			
			logger.info("sync_time: %s", sync_time)
			
			# 更新其它的同步到首页的数据
			syn_home = 1
			# 小编取消同步首页状态
			if t_is_delete == 1:
				syn_home = 3
			
			sync_sql = """update {tbl} set is_top={is_top}, sync_home={syn_home}, sync_home_time='{synchometime}'
							where article_id={aid} and channel_id = {cid}""".format(tbl=tbl, is_top=t_is_top,
			                    syn_home= syn_home, synchometime=sync_time, aid=t_article_id, cid=t_channel)
			logger.info("sync sql: %s", sync_sql)
			masterMysqlClient = MysqlClient(mode="master")
			res = masterMysqlClient.update(sync_sql)
			logger.info("syn home data msg count: %s", res)

	# 处理取消置顶数据状态
	top_sql = u"""select article_id, channel_id from {tbl} where is_top = 1""".format(tbl=tbl)
	master_mysql_client = MysqlClient(mode="master")
	slave_mysql_client = MysqlClient()
	top_data = master_mysql_client.getAll(top_sql, dispose=False)
	if top_data:
		for (aid, cid) in top_data:
			logger.info("recommend db article_id: %s, channel_id: %s have top", aid, cid)
			check_sql = """select count(1) from zdmdb_home where article_id={aid} and channel = {c} and is_top=1""".format(aid=aid, c=cid)
			check_data = slave_mysql_client.getOne(check_sql, dispose=False)
			logger.info("check home db article_id: %s, channel_id: %s, checkdata: %s", aid, cid, check_data)
			if check_data and check_data[0] == 0:
				up_sql = """update {tbl} set is_top=0 where article_id={aid} and channel_id={c} and is_top=1""".format(tbl=tbl, aid=aid, c=cid)
				res = master_mysql_client.update(up_sql, dispose=False)
				logger.info("取消置顶数据状态: aritcle_id: %s, channel_id: %s, res: %s", aid, cid, res)
		
	# 处理置顶数据状态
	top_sql = """select article_id, channel from zdmdb_home where is_top=1 """
	top_data = slave_mysql_client.getAll(top_sql)
	if top_data:
		for (aid, cid) in top_data:
			up_sql = """update {tbl} set is_top=1 where article_id={aid} and channel_id={c} and is_top=0""".format(tbl=tbl, aid=aid, c=cid)
			res = master_mysql_client.update(up_sql, dispose=False)
			logger.info("置顶数据状态: aritcle_id: %s, channel_id: %s, res: %s", aid, cid, res)
	
	master_mysql_client.close()
