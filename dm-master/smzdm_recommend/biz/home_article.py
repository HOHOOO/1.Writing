# -*- coding: utf-8 -*-
import logging
import random
import json
from tornado import gen
from tornado.log import gen_log
from datetime import datetime, timedelta
from base.config import Config
import time

from comm.consts import *
from util.es import ES

logger = logging.getLogger(__name__)


class HomeArticle(object):
	def __init__(self, redis_conn, device_id='', smzdm_id='', page=1, nums=20):
		self._device_id = device_id
		self._smzdm_id = smzdm_id
		self._page = page
		self._nums = nums
		self._editor_nums = self._nums - PAGE_RECOMMEND_SIZE
		self._redis_conn = redis_conn
		self._cate_prefer = {}
		self._tag_prefer = {}
		self._brand_prefer = {}
		self._history_key = HISTORY_KEY % self._device_id
		# 偏好获取
		if device_id:
			# 品类偏好
			level_key = "%s_level" % device_id
			tag_key = "%s_tag" % device_id
			brand_key = "%s_brand" % device_id
			gen_log.info("level_key: %s, tag_key: %s, brand_key: %s", level_key, tag_key, brand_key)
			try:
				# TODO 新的偏好key暂时为设备id，后面需要调整有前缀
				if self._redis_conn.exists(self._device_id):
					v = self._redis_conn.get(self._device_id)
					v_json = HomeArticle._parse_prefer_to_json(v)
					if PREFER_USER_KEY in v_json.keys():
						self._cate_prefer = v_json.get(PREFER_USER_KEY, {}).get("level", {})
						self._tag_prefer = v_json.get(PREFER_USER_KEY, {}).get("tag", {})
						self._brand_prefer = v_json.get(PREFER_USER_KEY, {}).get("brand", {})
					elif PREFER_DEVICE_KEY in v_json.keys():
						self._cate_prefer = v_json.get(PREFER_DEVICE_KEY, {}).get("level", {})
						self._tag_prefer = v_json.get(PREFER_DEVICE_KEY, {}).get("tag", {})
						self._brand_prefer = v_json.get(PREFER_DEVICE_KEY, {}).get("brand", {})
				#
				# if self._redis_conn.exists(level_key):
				# 	cate_prefer = self._redis_conn.get(level_key)
				# 	self._cate_prefer = HomeArticle._parse_prefer_to_json(cate_prefer)
				#
				# # 标签偏好
				# if self._redis_conn.exists(tag_key):
				# 	tag_prefer = self._redis_conn.get(tag_key)
				# 	self._tag_prefer = HomeArticle._parse_prefer_to_json(tag_prefer)
				#
				# # 品牌偏好
				# if self._redis_conn.exists(brand_key):
				# 	brand_prefer = self._redis_conn.get(brand_key)
				# 	self._brand_prefer = HomeArticle._parse_prefer_to_json(brand_prefer)
			except Exception as e:
				gen_log.warn("get user(device_id: %s) prefer from redis exception(%s)", self._device_id, str(e))
				
		gen_log.info("device_id: %s, cate_prefer: %s, tag_prefer: %s, brand_prefer: %s",
		             device_id, self._cate_prefer, self._tag_prefer, self._brand_prefer)
		
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
				accurate_set = HomeArticle._parse_str_to_set(accurate_value)
			
			if blur:
				_, blur_value = blur.split(COLON)
				blur_set = HomeArticle._parse_str_to_set(blur_set, VERTICAL_LINE)
				
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
	def _get_query_dict(self, gte_time, lte_time, home=True, is_top=0):
		"""
		# 从es查询数据条件
		:param gte_time:
		:param lte_time:
		:return:
		"""
		query_dict = {
				"size": self._nums * PAGE_NUM,
				"from": 0,
				"sort": [
					{
						"sync_home_time": {
							"order": "desc"
						}
					}
				],
				"query": {
					"filtered": {
						"query": {
							"match_all": {}
						},
						"filter": {
							"bool": {
								"must": [
									
									{
										"term": {
											"status": 0
										}
									},
									{
										"term": {
											"machine_report": 0
										}
									},
									{
										"term": {
											
											"is_top": is_top
										}
									}
								],
								"must_not": [
									
								]
							}
						}
					}
				}
			}
		
		condition = [
			{
				"term": {
					"sync_home": 1
				}
			},
			{
				"range": {
					"sync_home_time": {
						"gte": gte_time,
						"lt": lte_time
					}
				}
			}
		]
		
		# 非同步到首页的优惠和原创数据
		if not home:
			condition = [
				{
					"term": {
						"sync_home": 0
					}
				},
				{
					"terms": {
						"channel": ["yh", "yc"]
					}
				},
				{
					"range": {
						"sync_home_time": {
							"gte": gte_time,
							"lt": lte_time
						}
					}
				}
				
			]
		query_dict["query"]["filtered"]["filter"]["bool"]["must"].extend(condition)
		
		return query_dict
	
	@gen.coroutine
	def get_home_article_list(self):
		# 一页数据结果
		result_list = []
		
		# 获取推荐列表
		# 首先从缓存中获取该用户的文章列表; 缓存中没有，则查询es并根据该用户偏好计算权值，存入es，并写入缓存
		# 取老的数据，如果不够一页，则仍要取之前的数据
		# page = 1 第一页
		timeout = (HOUR_CONST - int(datetime.now().strftime("%H"))) * 3600
		pull_down_last_time_key = PULL_DOWN_LAST_TIME_PREFIX_KEY + self._device_id
		pull_up_last_time_key = PULL_UP_LAST_TIME_PREFIX_KEY + self._device_id
		hour_24_ago = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%d 00:00:00")
		
		# 是否设置过期时间
		if 1 == self._page:
			redis_flag = False
			if not self._redis_conn.exists(self._history_key):
				redis_flag = True
			# 从缓存中获取上一次下拉的时间
			pull_down_start_time = self._redis_conn.get(pull_down_last_time_key)
			# pull_up_start_time = pull_down_start_time if pull_down_start_time else datetime.now().strftime("%Y-%m-%d 00:00:00")
			pull_down_start_time = pull_down_start_time if pull_down_start_time else hour_24_ago
			
			pull_down_end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			gen_log.info("pull_down_last_time_key: %s, start_time: %s, end_time: %s", pull_down_last_time_key, pull_down_start_time, pull_down_end_time)
			# 下拉的最后时间记录为当前时间
			self._redis_conn.set(pull_down_last_time_key, pull_down_end_time, timeout)
			
			# 获取小编和推荐数据
			# 不需要重新排序
			editor_data = yield self._get_es_article(pull_down_start_time, pull_down_end_time)
			# 推荐侧的非同步到首页的文章， 需要加权排序
			recommend_data = yield self._get_es_article(pull_down_start_time, pull_down_end_time, False)
			recommend_data = yield self._calc_one_page_article_weight_or_sort(recommend_data, 1, True)
			
			editor_data_len = len(editor_data)
			gen_log.debug("page=%d, editor_data len: %d, recommend_data len: %d", self._page, editor_data_len, len(recommend_data))
			
			# TODO 小编的数据为null，是否获取推荐的数据进行补充； 小编和推荐数据的时间范围一样，可能需要调整
			# 如果下拉的新数据为0，则从用户的历史浏览缓存中读取一页数据并返回
			if 0 == editor_data_len:
				redis_data = self._redis_conn.lrange(self._history_key, (self._page - 1) * self._nums, self._page * self._nums - 1)
				result_list = [json.loads(d) for d in redis_data]
				filter_list = yield self._filter_dislike_cache(result_list)
				gen_log.debug("page=%d, return result filter list: %s", self._page, filter_list)
				raise gen.Return(filter_list)

			# 获取的数据如果大于 self._nums * (PAGE_NUM - 2),删除历史缓存
			if editor_data_len > (self._editor_nums * (PAGE_NUM - 2)):
				redis_flag = True
				self._redis_conn.delete(self._history_key)
			
			s_page = editor_data_len / self._editor_nums
			if editor_data_len % self._editor_nums != 0:
				s_page += 1
			
			# 写入缓存
			for i in range(0, s_page):
				start_index = (s_page - i - 1) * PAGE_SIZE
				data = editor_data[start_index: (s_page - i) * PAGE_SIZE]
				home_article_weight = yield self._calc_one_page_article_weight_or_sort(data)
				# 推荐的数据每次从列表中获取PAGE_RECOMMEND_SIZE记录
				recommend_article_list = recommend_data[i * PAGE_RECOMMEND_SIZE: (i+1) * PAGE_RECOMMEND_SIZE]
				yield self._merge_and_save_redis(home_article_weight, recommend_article_list)
			
			if redis_flag:
				self._redis_conn.expire(self._history_key, timeout)
				# 设置pull_up_end_time 时间
				self._redis_conn.set(pull_up_last_time_key, editor_data[-1]["_source"]["sync_home_time"], timeout)
		else:
			# page > 1 时， 直接取缓存中的列表数据
			redis_data = self._redis_conn.lrange(self._history_key, (self._page - 1) * self._nums, self._page * self._nums - 1)
			result_list = [json.loads(d) for d in redis_data]
			result_list_len = len(result_list)
			
			# 过滤已经缓存的不感兴趣
			result_list = yield self._filter_dislike_cache(result_list)
			
			gen_log.debug("page=%d, redis_data len: %d, filter result len: %d", self._page, result_list_len, len(result_list))
			
			# 取出的数据为一页数据
			if PAGE_SIZE == result_list_len:
				raise gen.Return(result_list)
				
			# 超过24小时时间，则最后一页，返回不足一页的数据
			pull_up_end_time = self._redis_conn.get(pull_up_last_time_key)
			
			gen_log.info("pull up start_time: %s, pull_up_end_time: %s", hour_24_ago, pull_up_end_time)
			editor_data = yield self._get_es_article(hour_24_ago, pull_up_end_time)
			editor_data_len = len(editor_data)
			gen_log.debug("page=%d, editor_data len: %d", self._page, editor_data_len)
			if 0 == editor_data_len:
				raise gen.Return(result_list)
			
			# 取推荐侧非同步到首页的数据并且排序
			recommend_data = yield self._get_es_article(hour_24_ago, pull_up_end_time, False)
			recommend_data = yield self._calc_one_page_article_weight_or_sort(recommend_data, 1, True)
			
			gen_log.debug("page=%d, recommend_data len: %d", self._page, len(recommend_data))
			# 将新数据写入到缓存中
			
			s_page = editor_data_len / self._editor_nums
			if editor_data_len % self._editor_nums != 0:
				s_page += 1
			for i in range(0, s_page):
				start_index = i * PAGE_SIZE
				data = editor_data[start_index: start_index + PAGE_SIZE]
				home_article_weight = yield self._calc_one_page_article_weight_or_sort(data)
				# 推荐的数据每次从列表中获取PAGE_RECOMMEND_SIZE记录
				recommend_article_list = recommend_data[i * PAGE_RECOMMEND_SIZE: (i + 1) * PAGE_RECOMMEND_SIZE]
				yield self._merge_and_save_redis(home_article_weight, recommend_article_list, False)
				
			# 设置pull_up_end_time 时间
			if editor_data:
				self._redis_conn.set(pull_up_last_time_key, editor_data[-1]["_source"]["sync_home_time"], timeout)
				
		# 从缓存中取一页数据返回
		result_list = self._redis_conn.lrange(self._history_key, (self._page - 1) * self._nums, self._page * self._nums - 1)
		result_list = [json.loads(d) for d in result_list]
		raise gen.Return(result_list)
	
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
	def _get_es_article(self, pull_start_time, pull_end_time, home=True):
		es = ES()
		r_query_dict = yield self._get_query_dict(pull_start_time, pull_end_time, home)
		home_es_index = Config["es.index"]
		article = yield es.search(home_es_index, r_query_dict)
		# es结果过滤
		filter_article = yield self._filter_sex_porduct(article)
		filter_dislike = yield self._filter_dislike(filter_article)
		raise gen.Return(filter_dislike)
	
	@gen.coroutine
	def get_top_article(self, pull_start_time, pull_end_time, home=True, is_top=0):
		if self._redis_conn.exists(TOP_ARTICLE_KEY):
			top_data = self._redis_conn.get(TOP_ARTICLE_KEY)
			raise gen.Return([json.loads(top_data)])
			
		es = ES()
		r_query_dict = yield self._get_query_dict(pull_start_time, pull_end_time, home, is_top)
		home_es_index = Config["es.index"]
		article = yield es.search(home_es_index, r_query_dict)
		top_data = []
		if article:
			article = article[0]
			time_sort = datetime.strptime(article["_source"]["sync_home_time"], "%Y-%m-%d %H:%M:%S")
			time_sort_timestamp = int(time.mktime(time_sort.timetuple()))
			one_row_dict = {
			        "id": article["_source"]["id"],
			        "article_id": article["_source"]["article_id"],
			        "channel": article["_source"]["channel_id"],
			        # "is_delete": 0 if d["_source"]["sync_home"] == 0 else d["_source"]["sync_home"],
			        "is_top": article["_source"]["is_top"],
			        "time_sort": time_sort_timestamp,
			        "type": 1,
			        "score": 10
			
			}
			top_data.append(one_row_dict)
			self._redis_conn.set(TOP_ARTICLE_KEY, json.dumps(one_row_dict), 60)
		raise gen.Return(top_data)
	
	@gen.coroutine
	def _merge_and_save_redis(self, home_list, recommend_list, if_list_head=True):
		"""
		desc: 对每个文章进行加权； 排序；写入redis
		:param data:
		:param redis_conn:
		:param if_list_head:
		:param result_list:
		:param if_append:
		:return:
		"""
		# 编辑流和推荐流合并
		home_list.extend(recommend_list)
		
		# 将合并的编辑流和推荐流排序
		home_sort_list = yield self._sort(home_list)
		
		if if_list_head:
			sort_list_length = len(home_sort_list)
			for i in range(0, sort_list_length):
				self._redis_conn.lpush(self._history_key, json.dumps(home_sort_list[sort_list_length - i - 1]))
		else:
			for i in home_sort_list:
				self._redis_conn.rpush(self._history_key, json.dumps(i))
				
		
	@gen.coroutine
	def _merge_recommend_and_save_redis(self, home_list, recommend_list, if_list_head=True, result_list=[], if_append=False):
		"""
		desc: 对每个文章进行加权； 排序；写入redis
		:param data:
		:param redis_conn:
		:param if_list_head:
		:param result_list:
		:param if_append:
		:return:
		"""
		# 编辑流和推荐流合并
		home_list.extend(recommend_list)
		
		# 将合并的编辑流和推荐流排序
		home_sort_list = yield self._sort(home_list)

		if if_append:
			result_list.extend(home_sort_list)
		
		if if_list_head:
			sort_list_length = len(home_sort_list)
			for i in range(0, sort_list_length):
				self._redis_conn.lpush(self._device_id, json.dumps(home_sort_list[sort_list_length-i-1]))
		else:
			for i in home_sort_list:
				self._redis_conn.rpush(self._device_id, json.dumps(i))
				
	@gen.coroutine
	def _sort(self, data_list, reverse=True):
		data_list.sort(cmp=lambda x, y: cmp(x["score"], y["score"]), reverse=reverse)
		
		raise gen.Return(data_list)
		
	@gen.coroutine
	def _calc_one_page_article_weight_or_sort(self, data, is_home=0, if_sort=False):
		weight_list_or_sort = []
		for d in data:
			score = yield self._calc_score(d)
			time_sort = datetime.strptime(d["_source"]["sync_home_time"], "%Y-%m-%d %H:%M:%S")
			time_sort_timestamp = int(time.mktime(time_sort.timetuple()))
			one_row_dict = {
				"id": d["_source"]["id"],
				"article_id": d["_source"]["article_id"],
				"channel": d["_source"]["channel_id"],
				# "is_delete": 0 if d["_source"]["sync_home"] == 0 else d["_source"]["sync_home"],
				"is_top": d["_source"]["is_top"],
				"time_sort": time_sort_timestamp,
				"type": is_home,
				"score": score
			}
			weight_list_or_sort.append(one_row_dict)
		if if_sort:
			weight_list_or_sort = yield self._sort(weight_list_or_sort)
		
		raise gen.Return(weight_list_or_sort)

	@gen.coroutine
	def _calc_score(self, data):
		"""
		desc: 对每篇文章加权
		:param data: 一篇文章的数据
		:return:
		"""
		score = 0.0
		if data:
			level1_ids = data["_source"]["level1_ids"]
			level2_ids = data["_source"]["level2_ids"]
			level3_ids = data["_source"]["level3_ids"]
			level4_ids = data["_source"]["level4_ids"]
			tag_ids = data["_source"]["tag_ids"]
			brand_ids = data["_source"]["brand_ids"]
			channel = data["_source"]["channel"]
			sync_home = data["_source"]["sync_home"]
			
			level1_ids_set = set(level1_ids.split(DOT))
			level2_ids_set = set(level2_ids.split(DOT))
			level3_ids_set = set(level3_ids.split(DOT))
			level4_ids_set = set(level4_ids.split(DOT))
			tag_ids_set = set(tag_ids.split(DOT))
			brand_ids_set = set(brand_ids.split(DOT))
			
			try:
			
				if channel == YOUHUI_CHANNEL:
					# 品类精确偏好
					youhui_accurate_prefer = set(self._cate_prefer.get(YOUHUI_KEY, {}).get("accurate", set()))
					youhui_blur_prefer = set(self._cate_prefer.get(YOUHUI_KEY, {}).get("blur", set()))
					
					if level1_ids_set - youhui_accurate_prefer:
						score += SUB_PORTRAIT_WIGHT["ACCURATE_CATE"]
					# 品类模糊偏好
					if (level3_ids_set - youhui_blur_prefer) or (level4_ids_set - youhui_blur_prefer):
						score += SUB_PORTRAIT_WIGHT["BLUR_CATE"]
					# 标签偏好
					if tag_ids_set - set(self._tag_prefer.get(YOUHUI_KEY, set())):
						score += SUB_PORTRAIT_WIGHT["TAG"]
						
					# 品牌偏好
					if brand_ids_set - set(self._brand_prefer.get(YOUHUI_KEY, set())):
						score += SUB_PORTRAIT_WIGHT["BRAND"]
				elif channel == YUANCHUANG_CHANNEL:
					if level2_ids_set - set(self._cate_prefer.get(YUANCHUANG_KEY, set())):
						score += PORTRAIT_TOTAL_WEIGHT
						
					# 标签偏好
					if tag_ids_set - set(self._tag_prefer.get(YUANCHUANG_KEY, set())):
						score += SUB_PORTRAIT_WIGHT["TAG"]
					
					# 品牌偏好
					if brand_ids_set - set(self._brand_prefer.get(YUANCHUANG_KEY, set())):
						score += SUB_PORTRAIT_WIGHT["BRAND"]
			except Exception as e:
				logger.error("_calc_score error(%s)", e.message)
				
			# 同步到首页的文章
			if sync_home > 0:
				score += EDITOR_SYNC_TOTAL_WEIGHT
				
				# 同步到首页的非好价好文的文章, 给以随机的权重
				if channel != YOUHUI_CHANNEL and channel != YUANCHUANG_CHANNEL:
					score += (PORTRAIT_TOTAL_WEIGHT * round(random.uniform(0, 1.0), 2))
			
			# 时间段加权
			sync_home_time = data["_source"]["sync_home_time"]
			if sync_home_time >= self._half_hour_ago:
				score += SUB_TIME_WEIGHT["HALF_HOUR"]
			elif sync_home_time >= self._one_hour_ago:
				score += SUB_TIME_WEIGHT["HOUR_1"]
			elif sync_home_time >= self._three_hour_ago:
				score += SUB_TIME_WEIGHT["HOUR_3"]
			elif sync_home_time >= self._twelve_hour_ago:
				score += SUB_TIME_WEIGHT["HOUR_12"]
			elif sync_home_time >= self._twenty_four_hour_ago:
				score += SUB_TIME_WEIGHT["HOUR_24"]
				
		raise gen.Return(score)
