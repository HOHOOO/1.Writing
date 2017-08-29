# -*- coding: utf-8 -*-
import logging
import json
from tornado import gen
from tornado.log import gen_log
from datetime import datetime, timedelta
from base.config import Config
from util.mysql import TorMysqlClient
from biz.home_article_b import HomeArticleB
from util.redis_client import RedisClient

from comm.consts import *

logger = logging.getLogger(__name__)


class HomeArticleTools(object):
	def __init__(self, redis_conn, action, device_id='', smzdm_id=''):
		self._device_id = device_id
		self._smzdm_id = smzdm_id
		self._action = action
		self._redis_conn = redis_conn
		self._cate_prefer = {}
		self._tag_prefer = {}
		self._brand_prefer = {}
		self._prefer_source = u''
		# 基于设备和用户偏好的key
		self._prefer_device_redis_key = PREFER_DEVICE_REDIS_KEY % self._device_id
		self._prefer_user_redis_key = PREFER_USER_REDIS_KEY % self._smzdm_id
		
		self._cate_prefer = {
			YOUHUI_KEY: {
				ACCURATE_KEY: {},
				BLUR_KEY: {},
				PARA: {}
			},
			YUANCHUANG_KEY: {}
		}
		
		self._tag_prefer = {
			YOUHUI_KEY: {},
			YUANCHUANG_KEY: {}
		}
		
		self._brand_prefer = {
			YOUHUI_KEY: {},
			YUANCHUANG_KEY: {}
		}
		
		# 获取不感兴趣的文章id和channel
		dislike_key = DISLIKE_KEY % (smzdm_id if smzdm_id != '0' else '', device_id)
		self._dislike_list = []
		if self._redis_conn.exists(dislike_key):
			dislike_value = self._redis_conn.lrange(dislike_key, 0, -1)
			for d in dislike_value:
				if COLON in d:
					article_id, channel_id = d.split(COLON)
					if int(channel_id) in YOUHUI_CHANNEL_MAP:
						channel_id = 3
					v = "%s:%s" % (article_id, channel_id)
					# 将不再不感兴趣列表中的值添加到列表中
					if v not in self._dislike_list:
						self._dislike_list.append(v)

		gen_log.info("dislike_key: %s", dislike_key)
		gen_log.debug("dislike_value: %s", self._dislike_list)
		
		# 计算时间半小时，1小时，3小时，12小时，24小时时间，供文章对时间的加权使用
		now = datetime.now()
		self._half_hour_ago = (now - timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
		self._one_hour_ago = (now - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
		self._three_hour_ago = (now - timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")
		self._twelve_hour_ago = (now - timedelta(hours=12)).strftime("%Y-%m-%d %H:%M:%S")
		self._twenty_four_hour_ago = (now - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
	
	@gen.coroutine
	def _parse_prefer_info(self):
		# 偏好获取
		if self._device_id:
			# 品类偏好
			try:
				prefer_json_data = ''
				if self._redis_conn.exists(self._prefer_user_redis_key):
					prefer_json_data = self._redis_conn.get(self._prefer_user_redis_key)
					self._prefer_source = u'用户ID: %s' % self._prefer_user_redis_key
				elif (not prefer_json_data) and self._redis_conn.exists(self._prefer_device_redis_key):
					prefer_json_data = self._redis_conn.get(self._prefer_device_redis_key)
					self._prefer_source = u'设备key: %s' % self._prefer_device_redis_key
				
				if prefer_json_data:
					v_json = HomeArticleTools._parse_prefer_to_json(prefer_json_data)
					# cate
					cate_prefer = v_json.get(PREFER_LEVEL_KEY, {})
					cate_yh_prefer = cate_prefer.get(YOUHUI_KEY, {})
					cate_yc_prefer = cate_prefer.get(YUANCHUANG_KEY, [])
					
					cate_yh_accurate = cate_yh_prefer.get(ACCURATE_KEY, [])
					cate_yh_blur = cate_yh_prefer.get(BLUR_KEY, [])
					cate_yh_vc = cate_yh_prefer.get(PARA, [])
					
					if cate_yh_accurate:
						for v in cate_yh_accurate:
							d = v.split(COLON)
							self._cate_prefer[YOUHUI_KEY][ACCURATE_KEY][u"%s" % d[0].strip()] = ','.join(d[1:])
					
					if cate_yh_blur:
						for v in cate_yh_blur:
							d = v.split(COLON)
							self._cate_prefer[YOUHUI_KEY][BLUR_KEY][u"%s" % d[0].strip()] = ','.join(d[1:])
					if cate_yc_prefer:
						for v in cate_yc_prefer:
							d = v.split(COLON)
							self._cate_prefer[YUANCHUANG_KEY][u'%s' % d[0].strip()] = ','.join(d[1:])
					if cate_yh_vc:
						for v in cate_yh_vc:
							d = v.split(COLON)
							self._cate_prefer[YOUHUI_KEY][PARA][u"%s" % d[0].strip()] = ','.join(d[1:])
					
					# tag
					tag_prefer = v_json.get(PREFER_TAG_KEY, {})
					tag_yh_prefer = tag_prefer.get(YOUHUI_KEY, [])
					tag_yc_prefer = tag_prefer.get(YUANCHUANG_KEY, [])
					if tag_yh_prefer:
						for v in tag_yh_prefer:
							d = v.split(COLON)
							self._tag_prefer[YOUHUI_KEY][u'%s' % d[0].strip()] = ','.join(d[1:])
					if tag_yc_prefer:
						for v in tag_yc_prefer:
							d = v.split(COLON)
							self._tag_prefer[YUANCHUANG_KEY][u'%s' % d[0].strip()] = ','.join(d[1:])
					
					# brand
					brand_prfer = v_json.get(PREFER_BRAND_KEY, {})
					brand_yh_prefer = brand_prfer.get(YOUHUI_KEY, [])
					brand_yc_prefer = brand_prfer.get(YUANCHUANG_KEY, [])
					if brand_yh_prefer:
						for v in brand_yh_prefer:
							d = v.split(COLON)
							self._brand_prefer[YOUHUI_KEY][u'%s' % d[0].strip()] = ','.join(d[1:])
					if brand_yc_prefer:
						for v in brand_yc_prefer:
							d = v.split(COLON)
							self._brand_prefer[YUANCHUANG_KEY][u'%s' % d[0].strip()] = ','.join(d[1:])
			
			except Exception as e:
				gen_log.warn("get user(device_id: %s) prefer from redis exception(%s)", self._device_id, str(e))
			self._prefer_flag = True
		
		gen_log.info("device_id: %s, cate_prefer: %s, tag_prefer: %s, brand_prefer: %s",
		             self._device_id, self._cate_prefer, self._tag_prefer, self._brand_prefer)
		
	@gen.coroutine
	def get_ori_editor_article_list(self):
		"""
		desc: 获取小编的原始数据
		:return:
		"""
		# 成人用品
		where = ''
		now = datetime.now().strftime("%H:%M:%D")
		if (now < SEX_PRODUCT_START_TIME) and (now > SEX_PRODUCT_END_TIME):
			level1_list = Config["sex_product.level1"].split(DOT)
			level2_list = Config["sex_product.level2"].split(DOT)
			level3_list = Config["sex_product.level2"].split(DOT)
			level4_list = Config["sex_product.level2"].split(DOT)
			l1 = u"'-1'"
			l2 = u"'-1'"
			l3 = u"'-1'"
			l4 = u"'-1'"
			for d in level1_list:
				l1 += u", '%s'" % d
			
			for d in level2_list:
				l2 += u", '%s'" % d
				
			for d in level3_list:
				l3 += u", '%s'" % d
				
			for d in level4_list:
				l4 += u", '%s'" % d
			
			where = """ level1_ids not in ({l1}) and level2_ids not in ({l2}) and level3_ids not in ({l3})
						and level4_ids not in ({l4}) """.format(l1=l1, l2=l2, l3=l3, l4=l4)
			
		gen_log.info("sex_product_where: %s", where)
		
		# 不感兴趣
		# 不喜欢文章过滤
		if self._dislike_list:
			dislike = u"'-1'"
			for d in self._dislike_list:
				dislike += u", '%s'" % d
			where += """ and article_channel not in ({dislike}) """.format(dislike=dislike)
		
		tbl = Config["mysql.home_tbl"]
		result_list = []
		now = datetime.now()
		start_time = (now - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
		comm_sql = """select article_id, channel_id, level1_ids, level2_ids, level3_ids, level4_ids, tag_ids,brand_ids,
						sync_home, is_top, publish_time, sync_home_time, title, comment_count, collection_count, praise,
						 sum_collect_comment, mall, brand, digital_price, worthy, unworthy """
		
		
		sql = """{select} from {tbl} where status=0
						 and machine_report=0 and sync_home=1 and is_top=0 and sync_home_time >='{start_time}' and {w}
						 order by sync_home_time desc
						 limit 300 """.format(select=comm_sql, tbl=tbl, start_time=start_time, w=where)
		
		gen_log.info("get_ori_editor_article_list sql: %s", sql)
		
		data = yield TorMysqlClient().fetchall(sql)
		if data:
			index = 0
			for d in data:
				index += 1
				l1_res = yield self._get_level_tag_brand_info(TOOLS_LEVEL_CONST, d[2])
				l2_res = yield self._get_level_tag_brand_info(TOOLS_LEVEL_CONST, d[3])
				l3_res = yield self._get_level_tag_brand_info(TOOLS_LEVEL_CONST, d[4])
				l4_res = yield self._get_level_tag_brand_info(TOOLS_LEVEL_CONST, d[5])
				tag_res = yield self._get_level_tag_brand_info(TOOLS_TAG_CONST, d[6])
				brand_res = yield self._get_level_tag_brand_info(TOOLS_BRAND_CONST, d[7])
				
				row_dict = {
					u'文章id': d[0],
					u'频道': TOOLS_CHANNEL_MAP.get(d[1], u'未知'),
					u'标题': d[12],
					u'发布时间': u'%s' % d[10],
					u'同步到首页时间': u'%s' % d[11],
					u'文章位置': index,
					u'品类': {
						u'一级品类': l1_res,
						u'二级品类': l2_res,
						u'三级品类': l3_res,
						u'四级品类': l4_res,
					},
					u'标签': tag_res,
					u'品牌': brand_res,
				}
				result_list.append(row_dict)
		
		raise gen.Return(result_list)
	
	@gen.coroutine
	def get_top_article(self):
		result_list = []
		start_time = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d %H:%M:%S")
		tbl = Config["mysql.home_tbl"]
		comm_sql = """select article_id, channel_id, level1_ids, level2_ids, level3_ids, level4_ids, tag_ids,brand_ids,
								sync_home, is_top, publish_time, sync_home_time, title, comment_count, collection_count, praise,
								 sum_collect_comment, mall, brand, digital_price, worthy, unworthy """
		sql = """{select} from {tbl} where status=0
											 and machine_report=0 and sync_home=1 and is_top=1 and sync_home_time >='{start_time}'
											 order by sync_home_time desc
											  """.format(select=comm_sql, tbl=tbl, start_time=start_time)
		
		data = yield TorMysqlClient().fetchall(sql)
		if data:
			d = data[0]
			l1_res = yield self._get_level_tag_brand_info(TOOLS_LEVEL_CONST, d[2])
			l2_res = yield self._get_level_tag_brand_info(TOOLS_LEVEL_CONST, d[3])
			l3_res = yield self._get_level_tag_brand_info(TOOLS_LEVEL_CONST, d[4])
			l4_res = yield self._get_level_tag_brand_info(TOOLS_LEVEL_CONST, d[5])
			tag_res = yield self._get_level_tag_brand_info(TOOLS_TAG_CONST, d[6])
			brand_res = yield self._get_level_tag_brand_info(TOOLS_BRAND_CONST, d[7])

			row_dict = {
				u'文章id': d[0],
				u'频道': TOOLS_CHANNEL_MAP.get(d[1], u'未知'),
				u'标题': d[12],
				u'发布时间': u'%s' % d[10],
				u'同步到首页时间': u'%s' % d[11],
				u'品类': {
					u'一级品类': l1_res,
					u'二级品类': l2_res,
					u'三级品类': l3_res,
					u'四级品类': l4_res,
				},
				u'标签': tag_res,
				u'品牌': brand_res,
			}
			result_list.append(row_dict)
		raise gen.Return(result_list)
	
	@gen.coroutine
	def get_sort_or_mem_editor_article_list(self, ori_type, weight={}):
		"""
		desc: 获取小编的原始数据
		:return:
		"""
		result_list = []
		redis_data = []
		_history_key = ''
		
		if ACTION_EDITOR_REDIS == ori_type:
			_history_key = HISTORY_B_KEY % self._device_id
			redis_data = self._redis_conn.lrange(_history_key, 0, -1)
			redis_data = [json.loads(d) for d in redis_data]
		else:
			# 直接从ES服务器上取数据，废弃直接从缓存中取数据
			redis_conn = yield RedisClient().get_redis_client()
			ha = HomeArticleB(redis_conn=redis_conn, device_id=self._device_id, smzdm_id=self._smzdm_id, w=weight)
			redis_data = yield ha.tools_get_editor_sort_article_list()
			
		gen_log.info("get_sort_editor_article_list _history_key:%s,  len: %s", _history_key, len(redis_data))
		if redis_data:
			index = 0
			for d in redis_data:
				index += 1
				aid = d.get("article_id", 0)
				channel_id = d.get("channel", 0)
				source = d.get("type", 0)
				score = d.get("score", 0)
				mysql_data = yield self._get_home_article_info(aid, channel_id)
				title = u''
				publish_time = u''
				sync_home_time = u''
				l1_res = []
				l2_res = []
				l3_res = []
				l4_res = []
				tag_res = []
				brand_res = []
				
				if mysql_data:
					title = mysql_data[0]
					publish_time = mysql_data[1]
					sync_home_time = mysql_data[2]
					
					l1_res = yield self._get_level_tag_brand_info(TOOLS_LEVEL_CONST, mysql_data[4])
					l2_res = yield self._get_level_tag_brand_info(TOOLS_LEVEL_CONST, mysql_data[5])
					l3_res = yield self._get_level_tag_brand_info(TOOLS_LEVEL_CONST, mysql_data[6])
					l4_res = yield self._get_level_tag_brand_info(TOOLS_LEVEL_CONST, mysql_data[7])
					tag_res = yield self._get_level_tag_brand_info(TOOLS_TAG_CONST, mysql_data[8])
					brand_res = yield self._get_level_tag_brand_info(TOOLS_BRAND_CONST, mysql_data[9])
					
				row_dict = {
					u'文章位置': index,
					u'文章id': aid,
					u'频道': TOOLS_CHANNEL_MAP.get(channel_id, UNKNOWN),
					u'标题': title,
					u'发布时间': u'%s' % publish_time,
					u'同步到首页时间': u'%s' % sync_home_time,
					u'来源': TOOLS_SOURCE_MAP.get(int(source), UNKNOWN),
					u'加权': score,
					u'品类': {
						u'一级品类': l1_res,
						u'二级品类': l2_res,
						u'三级品类': l3_res,
						u'四级品类': l4_res,
					},
					u'标签': tag_res,
					u'品牌': brand_res,
				}
				result_list.append(row_dict)
		raise gen.Return(result_list)
	
	@gen.coroutine
	def get_dislike_article_list(self):
		"""
		desc: 返回用户不感兴趣的文章信息
		:return:
		"""
		result_list = []

		if self._dislike_list:
			for d in self._dislike_list:
				aid, cid = d.split(COLON)
				
				title = u''
				publish_time = u''
				sync_home_time = u''
				source = -1
				l1_res = []
				l2_res = []
				l3_res = []
				l4_res = []
				tag_res = []
				brand_res = []
				mysql_data = yield self._get_home_article_info(aid, cid)
				if mysql_data:
					title = mysql_data[0]
					publish_time = mysql_data[1]
					sync_home_time = mysql_data[2]
					source = mysql_data[3]
					l1_res = yield self._get_level_tag_brand_info(TOOLS_LEVEL_CONST, mysql_data[4])
					l2_res = yield self._get_level_tag_brand_info(TOOLS_LEVEL_CONST, mysql_data[5])
					l3_res = yield self._get_level_tag_brand_info(TOOLS_LEVEL_CONST, mysql_data[6])
					l4_res = yield self._get_level_tag_brand_info(TOOLS_LEVEL_CONST, mysql_data[7])
					tag_res = yield self._get_level_tag_brand_info(TOOLS_TAG_CONST, mysql_data[8])
					brand_res = yield self._get_level_tag_brand_info(TOOLS_BRAND_CONST, mysql_data[9])
				
				row_dict = {
					u'文章id': aid,
					u'频道': TOOLS_CHANNEL_MAP.get(int(cid), UNKNOWN),
					u'标题': title,
					u'发布时间': u'%s' % publish_time,
					u'同步到首页时间': u'%s' % sync_home_time,
					u'来源': TOOLS_SOURCE_MAP.get(int(source), UNKNOWN),
					u'品类': {
						u'一级品类': l1_res,
						u'二级品类': l2_res,
						u'三级品类': l3_res,
						u'四级品类': l4_res,
					},
					u'标签': tag_res,
					u'品牌': brand_res,
					
				}
				result_list.append(row_dict)
		raise gen.Return(result_list)
	
	@gen.coroutine
	def get_user_prefer_info(self):
		"""
		desc: 获取用户偏好数据
		:return:
		"""
		yield self._parse_prefer_info()

		prefer_dict = {
			PREFER_SOURCE: self._prefer_source,
			TOOLS_CATE_PREFER: {
				TOOLS_YOUHUI: {
					TOOLS_ACCURATE_PREFER: {},
					TOOLS_BLUR_PREFER: {},
					TOOLS_VARIATION_COEFFICIENT: {}
				},
				TOOLS_YUANCHUANG: {}
			},
			TOOLS_TAG_PREFER: {
				TOOLS_YOUHUI: {},
				TOOLS_YUANCHUANG: {}
			},
			TOOLS_BRAND_PREFER: {
				TOOLS_YOUHUI: {},
				TOOLS_YUANCHUANG: {}
			}
		}
		# 品类偏好
		if self._cate_prefer:
			cate_list = []
			# 原创
			yc_cate_prefer = self._cate_prefer.get(YUANCHUANG_KEY, {})
			gen_log.debug("yh_vc_prefer: %s", self._cate_prefer)
			# 优惠精确偏好和模糊偏好
			yh_accurate_prefer = self._cate_prefer.get(YOUHUI_KEY, {}).get(ACCURATE_KEY, {})
			yh_blur_prefer = self._cate_prefer.get(YOUHUI_KEY, {}).get(BLUR_KEY, {})
			yh_vc_prefer = self._cate_prefer.get(YOUHUI_KEY, {}).get(PARA, {})
			if yc_cate_prefer:
				map(lambda (x, y): prefer_dict[TOOLS_CATE_PREFER][TOOLS_YUANCHUANG].setdefault(int(x), y.strip()), yc_cate_prefer.iteritems())
				cate_list += yc_cate_prefer.keys()
				
			if yh_accurate_prefer:
				map(lambda (x, y): prefer_dict[TOOLS_CATE_PREFER][TOOLS_YOUHUI][TOOLS_ACCURATE_PREFER].setdefault(int(x), y), yh_accurate_prefer.iteritems())
				cate_list += yh_accurate_prefer.keys()
			
			if yh_blur_prefer:
				map(lambda (x, y): prefer_dict[TOOLS_CATE_PREFER][TOOLS_YOUHUI][TOOLS_BLUR_PREFER].setdefault(int(x), y), yh_blur_prefer.iteritems())
				cate_list += yh_blur_prefer.keys()
			
			if yh_vc_prefer:
				map(lambda (x, y): prefer_dict[TOOLS_CATE_PREFER][TOOLS_YOUHUI][TOOLS_VARIATION_COEFFICIENT].setdefault(int(x), y), yh_vc_prefer.iteritems())
				cate_list += yh_vc_prefer.keys()
			
			if cate_list:
				cate_data = yield self._get_level_id_and_title_list_by_ids(cate_list)
				if cate_data:
					for (cid, title) in cate_data:
						gen_log.info("cate cid: %s, title: %s", cid, title)
						if cid in prefer_dict[TOOLS_CATE_PREFER][TOOLS_YUANCHUANG].keys():
							prefer_dict[TOOLS_CATE_PREFER][TOOLS_YUANCHUANG][cid] = u"%s:%s" % (yc_cate_prefer.get(u"%s" % cid, -1), title)
						elif cid in prefer_dict[TOOLS_CATE_PREFER][TOOLS_YOUHUI][TOOLS_ACCURATE_PREFER].keys():
							prefer_dict[TOOLS_CATE_PREFER][TOOLS_YOUHUI][TOOLS_ACCURATE_PREFER][cid] = u"%s:%s" % (yh_accurate_prefer.get(u"%s" % cid, -1), title)
						if cid in prefer_dict[TOOLS_CATE_PREFER][TOOLS_YOUHUI][TOOLS_BLUR_PREFER].keys():
							prefer_dict[TOOLS_CATE_PREFER][TOOLS_YOUHUI][TOOLS_BLUR_PREFER][cid] = u"%s:%s" % (yh_blur_prefer.get(u"%s" % cid, -1), title)
						
						if cid in prefer_dict[TOOLS_CATE_PREFER][TOOLS_YOUHUI][TOOLS_VARIATION_COEFFICIENT].keys():
							prefer_dict[TOOLS_CATE_PREFER][TOOLS_YOUHUI][TOOLS_VARIATION_COEFFICIENT][cid] = u"%s:%s" % (yh_vc_prefer.get(u"%s" % cid, -1), title)
			yc_cate_sort_list = sorted(prefer_dict[TOOLS_CATE_PREFER][TOOLS_YUANCHUANG].items(), lambda x, y: cmp(float(x[1].split(COLON)[0]), float(y[1].split(COLON)[0])), reverse=True)
			yh_accurate_sort_list = sorted(prefer_dict[TOOLS_CATE_PREFER][TOOLS_YOUHUI][TOOLS_ACCURATE_PREFER].items(), lambda x, y: cmp(float(x[1].split(COLON)[0]), float(y[1].split(COLON)[0])), reverse=True)
			yh_blur_sort_list = sorted(prefer_dict[TOOLS_CATE_PREFER][TOOLS_YOUHUI][TOOLS_BLUR_PREFER].items(), lambda x, y: cmp(float(x[1].split(COLON)[0]), float(y[1].split(COLON)[0])), reverse=True)
			
			prefer_dict[TOOLS_CATE_PREFER][TOOLS_YUANCHUANG] = [u"%s:%s" % (k, v) for (k, v) in yc_cate_sort_list]
			prefer_dict[TOOLS_CATE_PREFER][TOOLS_YOUHUI][TOOLS_ACCURATE_PREFER] = [u"%s:%s" % (k, v) for (k, v) in yh_accurate_sort_list]
			prefer_dict[TOOLS_CATE_PREFER][TOOLS_YOUHUI][TOOLS_BLUR_PREFER] = [u"%s:%s" % (k, v) for (k, v) in yh_blur_sort_list]
					
		# 品牌偏好
		if self._brand_prefer:
			yh_brand_prefer = self._brand_prefer.get(YOUHUI_KEY, {})
			yc_brand_prefer = self._brand_prefer.get(YUANCHUANG_KEY, {})
			brand_id_list = []
			if yh_brand_prefer:
				map(lambda (x, y): prefer_dict[TOOLS_BRAND_PREFER][TOOLS_YOUHUI].setdefault(int(x), y), yh_brand_prefer.iteritems())
				brand_id_list += yh_brand_prefer.keys()
			if yc_brand_prefer:
				map(lambda (x, y): prefer_dict[TOOLS_BRAND_PREFER][TOOLS_YUANCHUANG].setdefault(int(x), y), yc_brand_prefer.iteritems())
				brand_id_list += yc_brand_prefer.keys()
			
			if brand_id_list:
				brand_data = yield self._get_brand_id_and_title_list_by_ids(brand_id_list)
				if brand_data:
					for (cid, title) in brand_data:
						if cid in prefer_dict[TOOLS_BRAND_PREFER][TOOLS_YOUHUI].keys():
							prefer_dict[TOOLS_BRAND_PREFER][TOOLS_YOUHUI][cid] = u"%s:%s" % (yh_brand_prefer.get(u"%s" % cid, -1), title)
						elif cid in prefer_dict[TOOLS_BRAND_PREFER][TOOLS_YUANCHUANG].keys():
							prefer_dict[TOOLS_BRAND_PREFER][TOOLS_YUANCHUANG][cid] = u"%s:%s" % (yc_brand_prefer.get(u"%s" % cid, -1), title)
				brand_yh_sort_list = sorted(prefer_dict[TOOLS_BRAND_PREFER][TOOLS_YOUHUI].items(),
				                            lambda x,y: cmp(float(x[1].split(COLON)[0]), float(y[1].split(COLON)[0])), reverse=True)
				brand_yc_sort_list = sorted(prefer_dict[TOOLS_BRAND_PREFER][TOOLS_YUANCHUANG].items(),
				                            lambda x,y: cmp(float(x[1].split(COLON)[0]), float(y[1].split(COLON)[0])), reverse=True)
				prefer_dict[TOOLS_BRAND_PREFER][TOOLS_YOUHUI] = [u"%s:%s" % (k, v) for (k, v) in brand_yh_sort_list]
				prefer_dict[TOOLS_BRAND_PREFER][TOOLS_YUANCHUANG] = [u"%s:%s" % (k, v) for (k, v) in brand_yc_sort_list]
		# 标签偏好
		if self._tag_prefer:
			yh_tag_prefer = self._tag_prefer.get(YOUHUI_KEY, {})
			yc_tag_prefer = self._tag_prefer.get(YUANCHUANG_KEY, {})
			tag_id_list = []
			if yh_tag_prefer:
				map(lambda (x, y): prefer_dict[TOOLS_TAG_PREFER][TOOLS_YOUHUI].setdefault(int(x), y), yh_tag_prefer.iteritems())
				tag_id_list += yh_tag_prefer.keys()
			if yc_tag_prefer:
				map(lambda (x, y): prefer_dict[TOOLS_TAG_PREFER][TOOLS_YUANCHUANG].setdefault(int(x), y), yc_tag_prefer.iteritems())
				tag_id_list += yc_tag_prefer.keys()
			if tag_id_list:
				tag_data = yield self._get_tag_id_and_title_list_by_ids(tag_id_list)
				if tag_data:
					for (cid, title) in tag_data:
						if cid in prefer_dict[TOOLS_TAG_PREFER][TOOLS_YOUHUI].keys():
							prefer_dict[TOOLS_TAG_PREFER][TOOLS_YOUHUI][cid] = u"%s:%s" % (yh_tag_prefer.get(u"%s" % cid, -1), title)
						elif cid in prefer_dict[TOOLS_TAG_PREFER][TOOLS_YUANCHUANG].keys():
							prefer_dict[TOOLS_TAG_PREFER][TOOLS_YUANCHUANG][cid] = u"%s:%s" % (yc_tag_prefer.get(u"%s" % cid, -1), title)
			
			tag_yh_sort_list = sorted(prefer_dict[TOOLS_TAG_PREFER][TOOLS_YOUHUI].items(),
			                          lambda x, y: cmp(float(x[1].split(COLON)[0]), float(y[1].split(COLON)[0])), reverse=True)
			tag_yc_sort_list = sorted(prefer_dict[TOOLS_TAG_PREFER][TOOLS_YUANCHUANG].items(),
			                          lambda x, y: cmp(float(x[1].split(COLON)[0]), float(y[1].split(COLON)[0])), reverse=True)
			prefer_dict[TOOLS_TAG_PREFER][TOOLS_YOUHUI] = [u"%s:%s" % (k, v) for (k, v) in tag_yh_sort_list]
			prefer_dict[TOOLS_TAG_PREFER][TOOLS_YUANCHUANG] = [u"%s:%s" % (k, v) for (k, v) in tag_yc_sort_list]
		raise gen.Return([prefer_dict])
	
	@gen.coroutine
	def _get_level_tag_brand_info(self, source, ids):
		res = []
		if ids and source:
			id_list = [i for i in ids.strip().split(DOT) if i and i not in [DOT]]
			id_info = u''
			if source == TOOLS_LEVEL_CONST:
				id_info = yield self._get_level_id_and_title_list_by_ids(id_list)
			elif source == TOOLS_BRAND_CONST:
				id_info = yield self._get_brand_id_and_title_list_by_ids(id_list)
			elif source == TOOLS_TAG_CONST:
				id_info = yield self._get_tag_id_and_title_list_by_ids(id_list)
			if id_info:
				res = [u"%s:%s" % (lid, title) for (lid, title) in id_info]
			
		raise gen.Return(res)
	
	@gen.coroutine
	def _get_brand_id_and_title_list_by_ids(self, ids=[]):
		result_list = []
		if ids:
			sql = "select id, associate_title from sync_smzdm_brand where id in ({ids}) and is_deleted=0 ".format(
				ids=','.join(ids))
			mysql_data = yield TorMysqlClient().fetchall(sql)
			for d in mysql_data:
				cid = int(d[0])
				title = u"%s" % d[1]
				result_list.append((cid, title))
		raise gen.Return(result_list)
	
	@gen.coroutine
	def _get_tag_id_and_title_list_by_ids(self, ids=[]):
		result_list = []
		if ids:
			sql = "select id, name from sync_smzdm_tag_type where id in ({ids}) ".format(ids=','.join(ids))
			mysql_data = yield TorMysqlClient().fetchall(sql)
			for d in mysql_data:
				cid = int(d[0])
				title = u"%s" % d[1]
				result_list.append((cid, title))
		raise gen.Return(result_list)
	
	@gen.coroutine
	def _get_level_id_and_title_list_by_ids(self, ids=[]):
		result_list = []
		if ids:
			sql = "select id, title from sync_smzdm_product_category where id in ({ids})".format(
				ids=','.join(ids))
			gen_log.info("cate sql: %s", sql)
			mysql_data = yield TorMysqlClient().fetchall(sql)
			for d in mysql_data:
				cid = int(d[0])
				title = u"%s" % d[1]
				result_list.append((cid, title))
		raise gen.Return(result_list)
	
	@gen.coroutine
	def _get_home_article_info(self, aid, cid):
		tbl = Config["mysql.home_tbl"]
		result_list = []
		
		if aid and cid:
			sql = "select title,publish_time,sync_home_time,sync_home,level1_ids, level2_ids, level3_ids, level4_ids, tag_ids,brand_ids from {tbl} where article_id={aid} and channel_id={c}".format(
				tbl=tbl, aid=aid, c=cid)
			mysql_data = yield TorMysqlClient().fetchall(sql)
			if mysql_data:
				result_list = mysql_data[0]
		raise gen.Return(result_list)
	
	@staticmethod
	def _parse_cate_prefer(value):
		"""
			desc: 解析品类偏好字符串
				优惠： 精确偏好 + 泛偏好
				原创： 精确偏好
		:param value:
		:return:
		"""
		accurate_set = set()
		blur_set = set()
		if value:
			accurate, blur = value.strip().split(DOT)
			if accurate:
				_, accurate_value = accurate.split(COLON)
				accurate_set = HomeArticleTools._parse_str_to_set(accurate_value)
			
			if blur:
				_, blur_value = blur.split(COLON)
				blur_set = HomeArticleTools._parse_str_to_set(blur_set, VERTICAL_LINE)
				
		cate_prefer_dict = {
			"accurate": accurate_set,
			"blur": blur_set,
		}
		return cate_prefer_dict
	
	@staticmethod
	def _parse_prefer_to_json(value):
		"""
		desc: 从redis中获取的偏好解析为json格式
		:param value: value的取值举例
			# 品类
			value = {
				"youhui": {
					"accurate": set(["1","2","3"]),
					"blur": set(["1","2","3"])
				},
				"yuanchuang": set(["1","2","3"])
			}
			
			# 标签
			value = {
				"youhui": set(["1","2","3"]),
				"yuanchuang": set(["1","2","3"]),
			}
			
			# 品牌
			value = {
				"youhui": set(["1","2","3"]),
				"yuanchuang": set(["1","2","3"]),
			}

		:return:
		"""
		d = {}
		if value:
			d = json.loads(value)
		return d
	
	@staticmethod
	def _parse_str_to_set(value, seq=DOT):
		"""
		desc: 解析逗号字符串，并返回一个set值； 默认返回set()
		:param value:
		:return:
		"""
		result_set = set()
		if value:
			result_set = set(value.strip().split(seq))
		return result_set
	
	@gen.coroutine
	def _filter_sex_porduct(self, article):
		# 夜间不过滤成人用品
		now = datetime.now().strftime("%H:%M:%D")
		if (now > SEX_PRODUCT_START_TIME) or (now < SEX_PRODUCT_END_TIME):
			raise gen.Return(article)
		result_list = []
		level1_filter_set = set(Config["sex_product.level1"].split(DOT))
		level2_filter_set = set(Config["sex_product.level2"].split(DOT))
		level3_filter_set = set(Config["sex_product.level2"].split(DOT))
		level4_filter_set = set(Config["sex_product.level2"].split(DOT))
		
		for d in article:
			level1_ids_set = set(d["_source"]["level1_ids"].split(DOT))
			level2_ids_set = set(d["_source"]["level2_ids"].split(DOT))
			level3_ids_set = set(d["_source"]["level3_ids"].split(DOT))
			level4_ids_set = set(d["_source"]["level4_ids"].split(DOT))
			if level1_filter_set.intersection(level1_ids_set) \
				or level2_filter_set.intersection(level2_ids_set) \
				or level3_filter_set.intersection(level3_ids_set) \
				or level4_filter_set.intersection(level4_ids_set):
				continue
			
			result_list.append(d)
		raise gen.Return(result_list)
	
	@gen.coroutine
	def _filter_dislike(self, article):
		result_list = []
		if self._dislike_list:
			for d in article:
				dislike = "%s:%s" % (d["_source"]["article_id"], d["_source"]["channel_id"])
				if dislike in self._dislike_list:
					continue
				result_list.append(d)
		else:
			raise gen.Return(article)
		raise gen.Return(result_list)
	
	@gen.coroutine
	def _filter_dislike_cache(self, article):
		result_list = []
		if self._dislike_list:
			for d in article:
				dislike = "%s:%s" % (d["article_id"], d["channel"])
				if dislike in self._dislike_list:
					continue
				result_list.append(d)
		else:
			raise gen.Return(article)
		raise gen.Return(result_list)

	@gen.coroutine
	def shunt_add_device(self, timeout):
		try:
			if self._device_id:
				key = "%s:%s" % (PREFER_SHUNT_DEVICE_KEY, self._device_id)
				self._redis_conn.set(key, 'test', timeout)
		except Exception as e:
			raise gen.Return("shunt add device(%s) error(%s)" % (self._device_id, str(e)))
		raise gen.Return("shunt add device(%s) success." % self._device_id)
	
	@gen.coroutine
	def shunt_cancel_device(self):
		try:
			key = "%s:%s" % (PREFER_SHUNT_DEVICE_KEY, self._device_id)
			if self._redis_conn.exists(key):
				self._redis_conn.delete(key)
				gen_log.info("shunt cancel device(%s) success." % self._device_id)
		
		except Exception as e:
			raise gen.Return("shunt cancel device(%s) error(%s)" % (self._device_id, str(e)))
		
		raise gen.Return("shunt cancel device(%s) success." % self._device_id)
