# -*- coding: utf-8 -*-
import logging
from util.consts import *
from util.mysql import MysqlClient
from util.utils import get_pid, write_pid
from base.config import Config

logger = logging.getLogger(__name__)


def machine_report(pid=0):
	"""
	desc: 同步机器判断的过期售罄
	:param pid: youhui_meta表的主键meta_id
	:return:
	"""
	if int(pid) > 0:
		tpid = pid
	else:
		tpid = get_pid(MACHINE_REPORT_PID_FILE_PATH)
	
	# 更新过期售罄的优惠文章
	update(tpid)


def update(pid):
	# 查询增量的过期售罄文章
	# 增量数据不会过多，暂不做limit，offset设置
	new_comm = """select meta_id,article_id,meta_value from youhui_meta where meta_key = 'machine_report'
					and meta_id > {pid} order by meta_id desc"""
	
	new_sql = new_comm.format(pid=pid)
	
	logger.info("machine report new_sql is: %s", new_sql)
	slaveMysqlClient = MysqlClient()
	new_result = slaveMysqlClient.getMany(new_sql, ROW_COUNT)
	
	if new_result:
		for (pk, t_article_id, t_meta_value) in new_result:
			# 将本次最大的主键写入文件中
			logger.info("pk: %s, pid: %s, article_id:%s, meta_value: %s", pk, pid, t_article_id, t_meta_value)
			if int(pk) > pid:
				write_pid(MACHINE_REPORT_PID_FILE_PATH, pk)
			
			tbl = Config["master.recommend_tbl"]
			
			# 更新推荐侧数据库中的过期售罄字段
			sync_sql = """update {tbl} set machine_report={mr} where  article_id={aid}
								and channel='yh'""".format(tbl=tbl, mr=MACHINE_STATUS_MAP.get(int(t_meta_value), 2), aid=t_article_id)
			
			logger.info("sync_sql: %s", sync_sql)
			res = 0
			try:
				masterMysqlClient = MysqlClient(mode="master")
				res = masterMysqlClient.update(sync_sql)
				logger.info("update machine report count: %s", res)
			except Exception as e:
				logger.warn("machine_report update warn %s", str(e))
			return res
