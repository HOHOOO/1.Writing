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
from util.rabbit_mq import RabbitMQ
logger = logging.getLogger(__name__)


class HomeArticleB(object):
    def __init__(self, redis_conn, ab_test_type, device_id='', smzdm_id='', page=1, nums=20, w={}):
        self._device_id = device_id
        self._smzdm_id = smzdm_id
        self._page = page
        self._nums = nums
        self._redis_conn = redis_conn
        self._ab_test_type = ab_test_type
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
        self._w = W
        if w:
            self._w = w
        self._prefer_flag = False
        self._history_key = HISTORY_B_KEY % self._device_id
        self._history_article_list_key = HISTORY_B_ARTICLE_FILTER_LIST_KEY % self._device_id
        self._pull_down_last_time_key = PULL_DOWN_LAST_TIME_PREFIX_B_KEY % self._device_id

        self._yh_recommend_history_key = YH_HAVE_RECOMMEND_KEY % self._device_id
        self._yc_recommend_history_key = YC_HAVE_RECOMMEND_KEY % self._device_id
        self._yh_recommend_list_key = YH_RECOMMEND_KEY % self._device_id
        self._yc_recommend_list_key = YC_RECOMMEND_KEY % self._device_id

        # 基于设备和用户偏好的key
        self._prefer_device_redis_key = PREFER_DEVICE_REDIS_KEY % self._device_id
        self._prefer_user_redis_key = PREFER_USER_REDIS_KEY % self._smzdm_id

        # 获取不感兴趣的文章id和channel
        dislike_key = DISLIKE_KEY % (
            smzdm_id if smzdm_id != '0' else '', device_id)
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
        self._half_hour_ago = (now - timedelta(minutes=30)
                               ).strftime("%Y-%m-%d %H:%M:%S")
        self._one_hour_ago = (now - timedelta(hours=1)
                              ).strftime("%Y-%m-%d %H:%M:%S")
        self._three_hour_ago = (now - timedelta(hours=3)
                                ).strftime("%Y-%m-%d %H:%M:%S")
        self._twelve_hour_ago = (
            now - timedelta(hours=12)).strftime("%Y-%m-%d %H:%M:%S")
        self._twenty_four_hour_ago = (
            now - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")

    @gen.coroutine
    def _parse_prefer_info(self):
        # 偏好获取
        if self._device_id and (not self._prefer_flag):
            # 品类偏好
            try:
                prefer_json_data = ''
                if self._redis_conn.exists(self._prefer_user_redis_key):
                    prefer_json_data = self._redis_conn.get(
                        self._prefer_user_redis_key)
                elif (not prefer_json_data) and self._redis_conn.exists(self._prefer_device_redis_key):
                    prefer_json_data = self._redis_conn.get(
                        self._prefer_device_redis_key)

                if prefer_json_data:
                    v_json = HomeArticleB._parse_prefer_to_json(
                        prefer_json_data)
                    # cate
                    cate_prefer = v_json.get(PREFER_LEVEL_KEY, {})
                    cate_yh_prefer = cate_prefer.get(YOUHUI_KEY, {})
                    cate_yc_prefer = cate_prefer.get(YUANCHUANG_KEY, [])

                    cate_yh_accurate = cate_yh_prefer.get(ACCURATE_KEY, [])
                    cate_yh_blur = cate_yh_prefer.get(BLUR_KEY, [])
                    cate_yh_blur_para = cate_yh_prefer.get(PARA, [])

                    if cate_yh_accurate:
                        for v in cate_yh_accurate:
                            d = v.split(COLON)
                            self._cate_prefer[YOUHUI_KEY][ACCURATE_KEY][u"%s" % d[0].strip()] = d[1].split(
                                VERTICAL_LINE)

                    if cate_yh_blur:
                        for v in cate_yh_blur:
                            d = v.split(COLON)
                            self._cate_prefer[YOUHUI_KEY][BLUR_KEY][u"%s" % d[0].strip()] = d[1].split(
                                VERTICAL_LINE)
                    # 一级品类变异系数画像偏好
                    # 数据格式为level_1_id: level1_weight#alpha
                    if cate_yh_blur_para:
                        for v in cate_yh_blur_para:
                            d = v.split(COLON)
                            self._cate_prefer[YOUHUI_KEY][PARA][u"%s" %
                                                                d[0].strip()] = d[1].split(VERTICAL_LINE)
                    if cate_yc_prefer:
                        for v in cate_yc_prefer:
                            d = v.split(COLON)
                            self._cate_prefer[YUANCHUANG_KEY][u'%s' %
                                                              d[0].strip()] = d[1].split(VERTICAL_LINE)

                    # tag
                    tag_prefer = v_json.get(PREFER_TAG_KEY, {})
                    tag_yh_prefer = tag_prefer.get(YOUHUI_KEY, [])
                    tag_yc_prefer = tag_prefer.get(YUANCHUANG_KEY, [])
                    if tag_yh_prefer:
                        for v in tag_yh_prefer:
                            d = v.split(COLON)
                            self._tag_prefer[YOUHUI_KEY][u'%s' %
                                                         d[0].strip()] = d[1].split(VERTICAL_LINE)
                    if tag_yc_prefer:
                        for v in tag_yc_prefer:
                            d = v.split(COLON)
                            self._tag_prefer[YUANCHUANG_KEY][u'%s' %
                                                             d[0].strip()] = d[1].split(VERTICAL_LINE)

                    # brand
                    brand_prfer = v_json.get(PREFER_BRAND_KEY, {})
                    brand_yh_prefer = brand_prfer.get(YOUHUI_KEY, [])
                    brand_yc_prefer = brand_prfer.get(YUANCHUANG_KEY, [])
                    if brand_yh_prefer:
                        for v in brand_yh_prefer:
                            d = v.split(COLON)
                            self._cate_prefer[YOUHUI_KEY][u'%s' %
                                                          d[0].strip()] = d[1].split(VERTICAL_LINE)
                    if brand_yc_prefer:
                        for v in brand_yc_prefer:
                            d = v.split(COLON)
                            self._cate_prefer[YUANCHUANG_KEY][u'%s' %
                                                              d[0].strip()] = d[1].split(VERTICAL_LINE)

            except Exception as e:
                gen_log.warn(
                    "get user(device_id: %s) prefer from redis exception(%s)", self._device_id, str(e))
            self._prefer_flag = True

            gen_log.debug("device_id: %s, cate_prefer: %s, tag_prefer: %s, brand_prefer: %s", self._device_id,
                          self._cate_prefer, self._tag_prefer, self._brand_prefer)

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
    def _get_query_dict(self, source, start_time, now, is_top=0, filter_flag=True):
        """

        :param source: 分为3个来源：推荐（优惠，原创）,小编
        :param end_time: 最小截止时间
        :return:
        """
        if source not in [EDITOR_CHANNEL, YOUHUI_CHANNEL, YUANCHUANG_CHANNEL]:
            gen_log.warn(
                "_get_query_dict source value error(source=%s).", str(source))
            raise gen.Return({})

        filter_article_list = []

        query_dict = {
            "size": 300,
            "from": 0,
            "sort": [
                {
                    "sync_home_time": {
                        "order": "desc"
                    }
                }
            ],
            "query": {
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

        if source == YOUHUI_CHANNEL:
            query_dict["sort"] = [
                {
                    "publish_time": {
                        "order": "desc"
                    }
                }
            ]

            condition = [
                {
                    "term": {
                        "sync_home": 0
                    }
                },
                {
                    "terms": {
                        "channel": ["yh"]
                    }
                },
                {
                    "range": {
                        "publish_time": {
                            "gte": start_time,
                            "lt": now
                        }
                    }
                }
            ]
            query_dict["query"]["bool"]["must"].extend(condition)

        if source == YUANCHUANG_CHANNEL:
            query_dict["sort"] = [
                {
                    "publish_time": {
                        "order": "desc"
                    }
                }
            ]

            condition = [
                {
                    "term": {
                        "sync_home": 0
                    }
                },
                {
                    "terms": {
                        "channel": ["yc"]
                    }
                },
                {
                    "range": {
                        "publish_time": {
                            "gte": start_time,
                            "lt": now
                        }
                    }
                }
            ]
            query_dict["query"]["bool"]["must"].extend(condition)

        if source == EDITOR_CHANNEL:
            condition = [
                {
                    "term": {
                        "sync_home": 1
                    }
                },
                {
                    "range": {
                        "sync_home_time": {
                            "gte": start_time,
                            "lt": now
                        }
                    }
                }
            ]
            query_dict["query"]["bool"]["must"].extend(condition)

        # 过滤已经推荐过的文章
        if self._redis_conn.exists(self._yh_recommend_history_key):
            yh_recommend_history = self._redis_conn.lrange(
                self._yh_recommend_history_key, 0, -1)
            if yh_recommend_history:
                filter_article_list.extend(yh_recommend_history)

        if self._redis_conn.exists(self._yc_recommend_history_key):
            yc_recommend_history = self._redis_conn.lrange(
                self._yc_recommend_history_key, 0, -1)
            if yc_recommend_history:
                filter_article_list.extend(yc_recommend_history)

        if self._redis_conn.exists(self._history_article_list_key) and filter_flag:
            user_article_list = self._redis_conn.lrange(
                self._history_article_list_key, 0, -1)
            if user_article_list:
                filter_article_list.extend(user_article_list)

        # 不喜欢文章过滤
        if self._dislike_list:
            filter_article_list += self._dislike_list

        if filter_article_list:
            must_not_condition = {
                "terms": {
                    "article_channel": filter_article_list
                }
            }
            query_dict["query"]["bool"]["must_not"].append(must_not_condition)

        # 成人用品过滤
        # 夜间不过滤成人用品
        now = datetime.now().strftime("%H:%M:%D")
        if (now < SEX_PRODUCT_START_TIME) and (now > SEX_PRODUCT_END_TIME):
            level1_list = Config["sex_product.level1"].split(DOT)
            level2_list = Config["sex_product.level2"].split(DOT)
            level3_list = Config["sex_product.level3"].split(DOT)
            level4_list = Config["sex_product.level4"].split(DOT)

            cate_dict = [
                {
                    "terms": {
                        "level1_ids": level1_list
                    }
                },
                {
                    "terms": {
                        "level2_ids": level2_list
                    }
                },
                {
                    "terms": {
                        "level3_ids": level3_list
                    }
                },
                {
                    "terms": {
                        "level4_ids": level4_list
                    }
                },

            ]
            query_dict["query"]["bool"]["must_not"].extend(cate_dict)
            gen_log.debug(u"成人用品过滤 filter dict: %s", cate_dict)

        gen_log.debug("source: %s, query_dict: %s", source, query_dict)
        raise gen.Return(query_dict)

    @gen.coroutine
    def _save_have_recommend_article_to_redis(self, data, key):
        """
        desc: 将补量的数据存入到已经推过的缓存列表中
        :param data:
        :param key:
        :return:
        """
        expire_flag = False
        if not self._redis_conn.exists(key):
            expire_flag = True

        for d in data:
            article_id = d.get("article_id", '')
            channel_id = d.get("channel", '')
            value = "%s:%s" % (article_id, channel_id)
            if value:
                self._redis_conn.lpush(key, value)

        # 补量的缓存中只保留HAVE_RECOMMEND_COUNT_KEY条数据
        self._redis_conn.ltrim(key, 0, HAVE_RECOMMEND_COUNT_KEY)
        if expire_flag:
            self._redis_conn.expire(key, 3600 * 24 * 7)

    @gen.coroutine
    def _get_recommend_article_from_es(self, source, limit):
        """
        :param source:
        :return:
        """
        if source not in [YOUHUI_CHANNEL, YUANCHUANG_CHANNEL]:
            gen_log.error(
                "_get_recommend_article_from_es error source value(%s).", str(source))
            raise gen.Return([])

        if YOUHUI_CHANNEL == source:
            hours = 6
            key = self._yh_recommend_list_key
            have_key = self._yh_recommend_history_key
        elif YUANCHUANG_CHANNEL == source:
            hours = 72
            key = self._yc_recommend_list_key
            have_key = self._yc_recommend_history_key

        # 先从缓存中获取
        redis_data = self._redis_conn.lrange(key, 0, limit - 1)
        redis_data = [json.loads(d) for d in redis_data]
        if redis_data:
            # 将推荐的数据放入到已经推荐的列表缓存中
            yield self._save_have_recommend_article_to_redis(redis_data, have_key)
            value_len = self._redis_conn.llen(key)
            self._redis_conn.ltrim(key, limit, value_len)    # 删除已经推荐的文章
            gen_log.info(
                "_get_recommend_article_from_es from redis source: %s, limit: %s, data len: %s, key: %s, have_key: %s",
                source, limit, len(redis_data), key, have_key)

            raise gen.Return(redis_data)
        now = datetime.now()
        start_time = (now - timedelta(hours=hours)
                      ).strftime("%Y-%m-%d %H:%M:%S")
        end_time = now.strftime("%Y-%m-%d %H:%M:%S")

        data = yield self._get_es_article(source, start_time, end_time)
        weight_data = yield self._calc_article_weight_or_sort(data, 1, True, 3.0)

        gen_log.info("_get_recommend_article_from_es from es source: %s, limit: %s, data len: %s, key: %s, have_key: %s",
                     source, limit, len(weight_data), key, have_key)
        expire_flag = False
        if not self._redis_conn.exists(key):
            expire_flag = True

        result_list = []
        for d in weight_data:
            if len(result_list) <= (limit - 1):
                result_list.append(d)
                continue
            self._redis_conn.lpush(key, json.dumps(d))

        # 推荐的数据处理
        yield self._save_have_recommend_article_to_redis(result_list, have_key)

        # 设置过期时间为30s
        if expire_flag:
            self._redis_conn.expire(key, 30)

        raise gen.Return(result_list)

    @gen.coroutine
    def rec_monitor_business(self):
        try:
            start_time = datetime.now()
            gen_log.info("====rec_monitor_business start_time: %s===",
                         start_time.strftime("%Y-%m-%d %H:%M:%S"))
            d = {
                u"action": u"business_monitor",
                u"device_id": self._device_id,
                u"smzdm_id": self._smzdm_id
            }
            yield RabbitMQ().send_msg(json.dumps(d))
            total_seconds = datetime.now() - start_time
            gen_log.info("====rec_monitor_business total_seconds: %s(%s)===",
                         total_seconds.total_seconds(), d)
        except Exception as e:
            gen_log.error("rec_monitor_business exception(%s)", str(e))

    @gen.coroutine
    def get_home_article_list(self):
        # 获取推荐列表
        # 首先从缓存中获取该用户的文章列表; 缓存中没有，则查询es并根据该用户偏好计算权值，存入es，并写入缓存
        # 取老的数据，如果不够一页，则仍要取之前的数据
        # page = 1 第一页

        # expire_flag = False
        # 是否设置过期时间
        if 1 == self._page:
            delay_time_key = DELAY_TIME_KEY % self._device_id
            if self._redis_conn.exists(delay_time_key):
                editor_data = []
                editor_data_len = 0
            else:
                # 按这小编同步到首页的降序时间取前300条数据
                pull_down_end_time = self._redis_conn.get(
                    self._pull_down_last_time_key)
                if not pull_down_end_time:
                    # expire_flag = True
                    pull_down_end_time = (datetime.now(
                    ) - timedelta(hours=24) - timedelta(minutes=2)).strftime("%Y-%m-%d %H:%M:%S")
                # 记录本次下拉的时间到缓存中
                now = (datetime.now() - timedelta(minutes=2)
                       ).strftime("%Y-%m-%d %H:%M:%S")
                self._redis_conn.set(self._pull_down_last_time_key, now)

                gen_log.info("page: %s, pull_down_end_time: %s",
                             self._page, pull_down_end_time)
                editor_data = yield self._get_es_article(EDITOR_CHANNEL, pull_down_end_time, now)
                editor_data_len = len(editor_data)
                # 个性化排序
                if editor_data:
                    editor_data = yield self._calc_article_weight_or_sort(editor_data, 0, True)
                gen_log.info("page: %s, editor data len: %s",
                             self._page, editor_data_len)
                self._redis_conn.set(delay_time_key, 180, 60)

            # 小编数据大于等于RECOMMEND_B_SIZE，则返回小编的个性文章
            # if editor_data_len > RECOMMEND_B_MAX_SIZE:
            # 	# 将所有数据加入到缓存中，并返回一页数据
            # 	result_list = yield self._save_redis_and_fetch_top20(editor_data, editor_data_len)
            # elif editor_data_len >= RECOMMEND_B_SIZE:
            if editor_data_len >= RECOMMEND_B_SIZE:
                # 补量
                # 获取推荐侧的最近6小时的优惠数据
                yh_recommend = yield self._get_recommend_article_from_es(YOUHUI_CHANNEL, 3)
                yh_recommend_len = len(yh_recommend)
                # 获取推荐侧的最近72小时的原创数据
                yc_recommend = yield self._get_recommend_article_from_es(YUANCHUANG_CHANNEL, 2)
                yc_recommend_len = len(yc_recommend)

                if yh_recommend_len >= 1:
                    editor_data.insert(3, yh_recommend[0])  # yh
                    gen_log.info("candidate recommend device_id:%s:p3:yh:%s",
                                 self._device_id, str(yh_recommend[0]))
                if yc_recommend_len >= 1:
                    editor_data.insert(4, yc_recommend[0])  # yc
                    gen_log.info("candidate recommend device_id:%s:p4:yc:%s",
                                 self._device_id, str(yc_recommend[0]))
                if yh_recommend_len >= 2:
                    editor_data.insert(7, yh_recommend[1])  # yh
                    gen_log.info("candidate recommend device_id:%s:p7:yh:%s",
                                 self._device_id, str(yh_recommend[1]))
                if yc_recommend_len >= 2:
                    editor_data.insert(8, yc_recommend[1])  # yc
                    gen_log.info("candidate recommend device_id:%s:p8:yc:%s",
                                 self._device_id, str(yc_recommend[1]))
                if yh_recommend_len >= 3:
                    editor_data.insert(9, yh_recommend[2])  # yh
                    gen_log.info("candidate recommend device_id:%s:p9:yh:%s",
                                 self._device_id, str(yh_recommend[2]))

                gen_log.info("recommend article list size: %s",
                             len(editor_data[0:20]))

                result_list = yield self._save_redis_and_fetch_top20(editor_data, len(editor_data))
                if len(result_list) < self._nums:
                    data = self._redis_conn.lrange(
                        self._history_key, 0, self._nums - 1)
                    result_list = [json.loads(d) for d in data]

            elif editor_data_len >= 0:
                # # 获取小编数据小于等于5篇时，补量推荐侧的数据， 补量的数据和小编数据混排
                # # 获取推荐侧的最近6小时的优惠数据
                # yh_recommend = yield self._get_recommend_article_from_es(YOUHUI_CHANNEL, 3)
                # # 获取推荐侧的最近72小时的原创数据
                # yc_recommend = yield self._get_recommend_article_from_es(YUANCHUANG_CHANNEL, 2)
                # # 将小编／推荐数据顺序随机打乱并返回
                result_list = editor_data
                # result_list += yh_recommend
                # result_list += yc_recommend
                # random.shuffle(result_list)

                # if result_list:
                yield self._save_redis_and_fetch_top20(result_list, len(result_list))
                # else:
                # 补量数据和小编数据如果为0，则直接从缓存中取出一页数据返回
                redis_data = self._redis_conn.lrange(
                    self._history_key, 0, self._nums - 1)
                result_list = [json.loads(d) for d in redis_data]

            # 业务监控告警，只监控20%的量
            if round(random.uniform(0, 1), 1) < 0.2:
                yield self.rec_monitor_business()

        else:
            # page > 1 时， 直接取缓存中的列表数据
            redis_data = self._redis_conn.lrange(
                self._history_key, (self._page - 1) * self._nums, self._page * self._nums - 1)
            result_list = [json.loads(d) for d in redis_data]
        result_list_len = len(result_list)

        # 过滤已经缓存的不感兴趣
        result_list = yield self._filter_dislike_cache(result_list)
        gen_log.info("page=%d, redis_data len: %d, filter result len: %d",
                     self._page, result_list_len, len(result_list))

        # 用户的历史缓存中只保存HISTORY_B_COUNT条数据
        redis_data_len = self._redis_conn.llen(self._history_key)
        if redis_data_len > HISTORY_B_COUNT:
            self._redis_conn.ltrim(self._history_key, 0, HISTORY_B_COUNT - 1)
        # if expire_flag:
        self._redis_conn.expire(self._history_key, 3600 * 24)
        self._redis_conn.expire(self._pull_down_last_time_key, 3600 * 24)

        raise gen.Return(result_list)

    @gen.coroutine
    def get_top_article(self, start_time, end_time):
        if self._redis_conn.exists(TOP_ARTICLE_B_KEY):
            top_data = self._redis_conn.get(TOP_ARTICLE_B_KEY)
            raise gen.Return([json.loads(top_data)])

        es = ES()
        r_query_dict = yield self._get_query_dict(EDITOR_CHANNEL, start_time, end_time, 1)
        home_es_index = Config["es.index"]
        article = yield es.search(home_es_index, r_query_dict)
        top_data = []
        if article:
            article = article[0]
            time_sort = datetime.strptime(
                article["_source"]["sync_home_time"], "%Y-%m-%d %H:%M:%S")
            time_sort_timestamp = int(time.mktime(time_sort.timetuple()))
            one_row_dict = {
                "id": article["_source"]["id"],
                "article_id": article["_source"]["article_id"],
                "channel": article["_source"]["channel_id"],
                # "is_delete": 0 if d["_source"]["sync_home"] == 0 else d["_source"]["sync_home"],
                "is_top": article["_source"]["is_top"],
                "time_sort": time_sort_timestamp,
                "type": 1,
                "score": {
                    "total": 0.0,
                    "detail": []
                }

            }
            top_data.append(one_row_dict)
            self._redis_conn.set(
                TOP_ARTICLE_B_KEY, json.dumps(one_row_dict), 60)
        raise gen.Return(top_data)

    @gen.coroutine
    def tools_get_editor_sort_article_list(self):
        # TODO 1.仅供内部人员工具类接口   2.整合和线上接口统一
        pull_down_end_time = (datetime.now() - timedelta(hours=24) - timedelta(minutes=2)).strftime(
            "%Y-%m-%d %H:%M:%S")
        now = (datetime.now() - timedelta(minutes=2)
               ).strftime("%Y-%m-%d %H:%M:%S")
        editor_data = yield self._get_es_article(EDITOR_CHANNEL, pull_down_end_time, now, False)
        if editor_data:
            editor_data = yield self._calc_article_weight_or_sort(editor_data, 0, True, -1.0)

        raise gen.Return(editor_data)

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
                dislike = "%s:%s" % (
                    d["_source"]["article_id"], d["_source"]["channel_id"])
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
    def _get_es_article(self, source, start_time, now, filter_flag=True):
        es = ES()
        r_query_dict = yield self._get_query_dict(source, start_time, now, filter_flag=filter_flag)
        home_es_index = Config["es.index"]
        article = yield es.search(home_es_index, r_query_dict)
        raise gen.Return(article)

    @gen.coroutine
    def _save_redis_and_fetch_top20(self, data, data_len):
        """
        desc: 将data列表数据写入缓存，并返回最多20条数据
        :param data:
        :return:
        """
        if data_len < 1:
            raise gen.Return([])

        take_time_start = datetime.now()
        try:
            pipe = self._redis_conn.pipeline(transaction=False)
            index = 0
            for i in range(0, data_len):
                index += 1
                pipe.lpush(self._history_key, json.dumps(
                    data[data_len - i - 1]))
                try:
                    article_id = data[data_len - i - 1].get("article_id", '')
                    channel_id = data[data_len - i - 1].get("channel", '')
                    value = "%s:%s" % (article_id, channel_id)
                    if value:
                        pipe.lpush(self._history_article_list_key, value)
                except Exception as e:
                    logger.error(
                        "_save_redis_and_fetch_top20 lpush _history_article_list_key error(%s)", str(e))

                if index == HISTORY_B_COUNT:
                    pipe.execute()
                    index = 0

            pipe.execute()
        except Exception as e:
            logger.error(
                "_save_redis_and_fetch_top20 execute error(%s)", str(e))

        take_save_time = datetime.now()
        history_article_len = self._redis_conn.llen(
            self._history_article_list_key)
        if history_article_len > HISTORY_B_COUNT:
            self._redis_conn.ltrim(
                self._history_article_list_key, 0, HISTORY_B_COUNT - 1)
            self._redis_conn.expire(
                self._history_article_list_key, 3600 * 24 * 7)
        take_redis_trim_time = datetime.now()

        logger.info("_save_redis_and_fetch_top20 redis save take: %s, redis trim: %s",
                    (take_save_time - take_time_start).total_seconds(),
                    (take_redis_trim_time - take_save_time).total_seconds())
        raise gen.Return(data[0:20])

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
                self._redis_conn.lpush(self._device_id, json.dumps(
                    home_sort_list[sort_list_length - i - 1]))
        else:
            for i in home_sort_list:
                self._redis_conn.rpush(self._device_id, json.dumps(i))

    @gen.coroutine
    def _sort(self, data_list, reverse=True):
        data_list.sort(cmp=lambda x, y: cmp(x.get(u"score", {}).get(u"total", 0),
                                            y.get(u"score", {}).get(u"total", 0)), reverse=reverse)

        raise gen.Return(data_list)

    @gen.coroutine
    def _calc_article_weight_or_sort(self, data, is_home=0, if_sort=False, threshold=0.0):
        """
        desc: 计算文章权重，是否排序
        :param data:
        :param is_home: 0：推荐文章  1：推荐侧的文章
        :param if_sort: 是否排序，默认不排序
        :return:
        """
        take_start_time = datetime.now()
        yield self._parse_prefer_info()

        weight_list_or_sort = []
        for d in data:
            score = yield self._calc_score(d)
            if score[u"total"] < threshold:
                gen_log.debug("article_id: %s, score(%s) < threshold (%s)",
                              d["_source"]["article_id"], score, threshold)
                continue
            time_sort = datetime.strptime(
                d["_source"]["sync_home_time"], "%Y-%m-%d %H:%M:%S")
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
        take_calc_time = datetime.now()
        if if_sort:
            weight_list_or_sort = yield self._sort(weight_list_or_sort)
        take_sort_time = datetime.now()

        logger.info("calc time take: %s, sort time take: %s", (take_calc_time - take_start_time).total_seconds(),
                    (take_sort_time - take_calc_time).total_seconds())
        raise gen.Return(weight_list_or_sort)

    @staticmethod
    def _get_default_random_value():
        return round(random.uniform(0.0005, 0.001), 4)

    @gen.coroutine
    def _calc_score(self, data):
        """
        desc: 对每篇文章加权
        :param data: 一篇文章的数据
        :return:
        """
        score_dict = {}
        score_total = 0.0
        if data:
            level1_ids = data["_source"]["level1_ids"]
            level2_ids = data["_source"]["level2_ids"]
            level3_ids = data["_source"]["level3_ids"]
            level4_ids = data["_source"]["level4_ids"]
            tag_ids = data["_source"]["tag_ids"]
            brand_ids = data["_source"]["brand_ids"]
            channel = data["_source"]["channel"]
            sync_home = data["_source"]["sync_home"]

            level1_ids_list = level1_ids.split(DOT)
            level2_ids_list = level2_ids.split(DOT)
            level3_ids_list = level3_ids.split(DOT)
            level4_ids_list = level4_ids.split(DOT)
            tag_ids_list = tag_ids.split(DOT)
            brand_ids_list = brand_ids.split(DOT)
            score_detail_list = []
            score_detail_dict = {}
            try:
                # 过滤标识
                filter_flag = True
                if channel == YOUHUI_CHANNEL:
                    # 品类精确偏好, 暂时不考虑4级品类
                    # if level3_ids_list or level4_ids_list:
                    if self._ab_test_type in ["a", "c", "d", "e", "f"]:
                        score_total += yield self._calc_level_by_variation_coefficient(level1_ids_list, level3_ids_list, score_detail_list)
                        if score_total < self._w[W_THRESHOLD]:
                            filter_flag = False
                    else:
                        if level3_ids_list:
                            max_score = 0.0
                            no_prefer_flag = True
                            # for v in level3_ids_list + level4_ids_list:
                            for v in level3_ids_list:
                                if v in self._cate_prefer[YOUHUI_KEY][ACCURATE_KEY].keys():
                                    if no_prefer_flag:
                                        no_prefer_flag = False
                                    wv = float(
                                        self._cate_prefer[YOUHUI_KEY][ACCURATE_KEY][v][-1])

                                    # 判断此偏好是否小于阀值
                                    if wv <= self._w[W_THRESHOLD]:
                                        filter_flag = False
                                        score_detail_list.append(
                                            u"yh_level_accurate filter cid[{cid}],weight[{w}]".format(cid=v, w=wv))
                                        break

                                    score = self._w[W_PORTRAIT_WIGHT][W_ACCURATE_CATE] * wv
                                    if score > max_score:
                                        max_score = score
                                    score_detail_list.append(u"yh_level_accurate[{score}]=basic[{basic_w}]*weight[{w}]lid:{cid}".format(
                                        score=score, basic_w=self._w[W_PORTRAIT_WIGHT][W_ACCURATE_CATE],
                                        cid=v, w=self._cate_prefer[YOUHUI_KEY][ACCURATE_KEY][v][-1]
                                    ))
                            # 取最大值
                            score_total += max_score
                            # 取默认值
                            if no_prefer_flag and filter_flag:
                                random_w = HomeArticleB._get_default_random_value()
                                default_value = self._w[W_PORTRAIT_WIGHT][W_ACCURATE_CATE] * random_w
                                score_total += default_value
                                score_detail_list.append(
                                    u"yh_level_accurate_default[{score}]=basic[{basic_w}]*random_weight[{w}]".format(
                                        score=default_value, basic_w=self._w[
                                            W_PORTRAIT_WIGHT][W_ACCURATE_CATE], w=random_w
                                    ))

                        # 品类模糊偏好
                        if level1_ids_list and filter_flag:
                            max_score = 0.0
                            no_prefer_flag = True
                            for v in level1_ids_list:
                                if v in self._cate_prefer[YOUHUI_KEY][BLUR_KEY].keys():
                                    if no_prefer_flag:
                                        no_prefer_flag = False
                                    wv = float(
                                        self._cate_prefer[YOUHUI_KEY][BLUR_KEY][v][-1])

                                    # 判断此偏好是否小于阀值
                                    if wv <= self._w[W_THRESHOLD]:
                                        filter_flag = False
                                        score_detail_list.append(
                                            u"yh_level_blur filter cid[{cid}],weight[{w}]".format(cid=v, w=wv))
                                        break

                                    score = self._w[W_PORTRAIT_WIGHT][W_BLUR_CATE] * wv
                                    if score > max_score:
                                        max_score = score
                                    score_detail_list.append(u"yh_level_blur[{score}]=basic[{basic_w}]*weight[{w}]lid:{cid}".format(
                                        score=score, basic_w=self._w[W_PORTRAIT_WIGHT][W_BLUR_CATE],
                                        cid=v, w=self._cate_prefer[YOUHUI_KEY][BLUR_KEY][v][-1]
                                    ))
                            # 取最大值
                            score_total += max_score
                            # 默认值
                            if no_prefer_flag and filter_flag:
                                random_w = HomeArticleB._get_default_random_value()
                                default_value = self._w[W_PORTRAIT_WIGHT][W_BLUR_CATE] * random_w
                                score_total += default_value
                                score_detail_list.append(
                                    u"yh_level_blur_default[{score}]=basic[{basic_w}]*random_weight[{w}]".format(
                                        score=default_value, basic_w=self._w[W_PORTRAIT_WIGHT][W_BLUR_CATE], w=random_w
                                    ))

                    # 标签偏好
                    if tag_ids_list and filter_flag:
                        max_score = 0.0
                        no_prefer_flag = True
                        for v in tag_ids_list:
                            if v in self._tag_prefer[YOUHUI_KEY].keys():
                                if no_prefer_flag:
                                    no_prefer_flag = False

                                # 判断此偏好是否小于阀值
                                wv = float(self._tag_prefer[YOUHUI_KEY][v][-1])
                                if wv <= self._w[W_THRESHOLD]:
                                    filter_flag = False
                                    score_detail_list.append(
                                        u"yh_tag filter tid[{tid}],weight[{w}]".format(tid=v, w=wv))
                                    break

                                score = self._w[W_PORTRAIT_WIGHT][W_TAG] * wv
                                if score > max_score:
                                    max_score = score
                                score_detail_list.append(u"yh_tag[{score}]=basic[{basic_w}]*weight[{w}]tid:{tid}".format(
                                    score=score, basic_w=self._w[W_PORTRAIT_WIGHT][W_TAG],
                                    w=self._tag_prefer[YOUHUI_KEY][v][-1], tid=v
                                ))

                        # 取最大值
                        score_total += max_score
                        # 默认值
                        if no_prefer_flag and filter_flag:
                            random_w = HomeArticleB._get_default_random_value()
                            default_value = self._w[W_PORTRAIT_WIGHT][W_TAG] * random_w
                            score_total += default_value
                            score_detail_list.append(
                                u"yh_tag_default[{score}]=basic[{basic_w}]*random_weight[{w}]".format(
                                    score=default_value, basic_w=self._w[W_PORTRAIT_WIGHT][W_TAG], w=random_w
                                ))

                    # 品牌偏好
                    if brand_ids_list and filter_flag:
                        max_score = 0.0
                        no_prefer_flag = True
                        for v in brand_ids_list:
                            if v in self._brand_prefer[YOUHUI_KEY].keys():
                                if no_prefer_flag:
                                    no_prefer_flag = False

                                # 判断此偏好是否小于阀值
                                wv = float(
                                    self._brand_prefer[YOUHUI_KEY][v][-1])
                                if wv <= self._w[W_THRESHOLD]:
                                    filter_flag = False
                                    score_detail_list.append(
                                        u"yh_tag filter bid[{bid}],weight[{w}]".format(bid=v, w=wv))
                                    break

                                score = self._w[W_PORTRAIT_WIGHT][W_TAG] * wv
                                if score > max_score:
                                    max_score = score
                                score_detail_list.append(u"yh_brand[{score}]=basic[{basic_w}]*weight[{w}]bid:{bid}".format(
                                    score=score, basic_w=self._w[W_PORTRAIT_WIGHT][W_TAG],
                                    w=self._brand_prefer[YOUHUI_KEY][v][-1], bid=v
                                ))
                        score_total += max_score

                        # 默认值
                        if no_prefer_flag and filter_flag:
                            random_w = HomeArticleB._get_default_random_value()
                            default_value = self._w[W_PORTRAIT_WIGHT][W_TAG] * random_w
                            score_total += default_value
                            score_detail_list.append(
                                u"yh_brand_default[{score}]=basic[{basic_w}]*random_weight[{w}]".format(
                                    score=default_value, basic_w=self._w[W_PORTRAIT_WIGHT][W_TAG], w=random_w
                                ))

                elif channel == YUANCHUANG_CHANNEL:
                    # 品类 暂时不考虑4级分类
                    # if level3_ids_list or level4_ids_list:
                    if level3_ids_list:
                        max_score = 0.0
                        no_prefer_flag = True
                        # for v in level3_ids_list + level4_ids_list:
                        for v in level3_ids_list:
                            if v in self._cate_prefer[YUANCHUANG_KEY].keys():
                                if no_prefer_flag:
                                    no_prefer_flag = True

                                # 判断此偏好是否小于阀值
                                wv = float(
                                    self._cate_prefer[YUANCHUANG_KEY][v][-1])
                                if wv <= self._w[W_THRESHOLD]:
                                    filter_flag = False
                                    score_detail_list.append(
                                        u"yc_level3 filter cid[{cid}],weight[{w}]".format(cid=v, w=wv))
                                    break

                                score = self._w[W_PORTRAIT_WIGHT][W_TOTAL] * wv
                                if score > max_score:
                                    max_score = score
                                score_detail_list.append(
                                    u"yc_level[{score}]=basic[{basic_w}]*weight[{w}]lid:{cid}".format(
                                        score=score, basic_w=self._w[W_PORTRAIT_WIGHT][W_TOTAL],
                                        w=self._cate_prefer[YUANCHUANG_KEY][v][-1], cid=v
                                    ))
                        score_total += max_score

                        # 默认值
                        if no_prefer_flag and filter_flag:
                            random_w = HomeArticleB._get_default_random_value()
                            default_value = self._w[W_PORTRAIT_WIGHT][W_TOTAL] * random_w
                            score_total += default_value
                            score_detail_list.append(
                                u"yc_level_default[{score}]=basic[{basic_w}]*random_weight[{w}]".format(
                                    score=default_value, basic_w=self._w[W_PORTRAIT_WIGHT][W_TOTAL], w=random_w
                                ))

                    # 标签偏好
                    if tag_ids_list and filter_flag:
                        max_score = 0.0
                        no_prefer_flag = True
                        for v in tag_ids_list:
                            if v in self._tag_prefer[YUANCHUANG_KEY].keys():
                                if no_prefer_flag:
                                    no_prefer_flag = False

                                # 判断此偏好是否小于阀值
                                wv = float(
                                    self._tag_prefer[YUANCHUANG_KEY][v][-1])
                                if wv <= self._w[W_THRESHOLD]:
                                    filter_flag = False
                                    score_detail_list.append(
                                        u"yc_tag filter tid[{tid}],weight[{w}]".format(tid=v, w=wv))
                                    break

                                score = self._w[W_PORTRAIT_WIGHT][W_TAG] * wv
                                if score > max_score:
                                    max_score = score
                                score_detail_list.append(u"yc_tag[{score}]=basic[{basic_w}]*weight[{w}]tid:{tid}".format(
                                    score=score, basic_w=self._w[W_PORTRAIT_WIGHT][W_TAG],
                                    w=self._tag_prefer[YUANCHUANG_KEY][v][-1], tid=v
                                ))
                        score_total += max_score

                        # 默认值
                        if no_prefer_flag and filter_flag:
                            random_w = HomeArticleB._get_default_random_value()
                            default_value = self._w[W_PORTRAIT_WIGHT][W_TAG] * random_w
                            score_total += default_value
                            score_detail_list.append(
                                u"yc_tag_default[{score}]=basic[{basic_w}]*random_weight[{w}]".format(
                                    score=default_value, basic_w=self._w[W_PORTRAIT_WIGHT][W_TAG], w=random_w
                                ))

                    # 品牌偏好
                    if brand_ids_list and filter_flag:
                        max_score = 0.0
                        no_prefer_flag = True
                        for v in brand_ids_list:
                            if v in self._brand_prefer[YUANCHUANG_KEY].keys():
                                if no_prefer_flag:
                                    no_prefer_flag = False

                                # 判断此偏好是否小于阀值
                                wv = float(
                                    self._brand_prefer[YUANCHUANG_KEY][v][-1])
                                if wv <= self._w[W_THRESHOLD]:
                                    filter_flag = False
                                    score_detail_list.append(
                                        u"yc_brand filter bid[{bid}],weight[{w}]".format(bid=v, w=wv))
                                    break

                                score = self._w[W_PORTRAIT_WIGHT][W_TAG] * wv
                                if score > max_score:
                                    max_score = score
                                score_detail_list.append(u"yc_brand[{score}]=basic[{basic_w}]*weight[{w}]bid:{bid}".format(
                                    score=score, basic_w=self._w[W_PORTRAIT_WIGHT][W_TAG],
                                    w=self._brand_prefer[YUANCHUANG_KEY][v][-1], bid=v
                                ))
                        score_total += max_score

                        # 默认值
                        if no_prefer_flag and filter_flag:
                            random_w = HomeArticleB._get_default_random_value()
                            default_value = self._w[W_PORTRAIT_WIGHT][W_TAG] * random_w
                            score_total += default_value
                            score_detail_list.append(
                                u"yc_brand_default[{score}]=basic[{basic_w}]*random_weight[{w}]".format(
                                    score=default_value, basic_w=self._w[W_PORTRAIT_WIGHT][W_TAG], w=random_w
                                ))

            except Exception as e:
                logger.error("_calc_score error(%s)", e.message)

            # 同步到首页的文章
            if sync_home > 0 and filter_flag:
                score_total += self._w[W_EDITOR_SYNC_TOTAL_WEIGHT]
                score_detail_list.append(u"sync_home[{sync_home_w}]".format(
                    sync_home_w=self._w[W_EDITOR_SYNC_TOTAL_WEIGHT]))

                # 同步到首页的非好价好文的文章, 给以随机的权重
                if channel != YOUHUI_CHANNEL and channel != YUANCHUANG_CHANNEL:
                    w_random = round(random.uniform(0.01, 0.05), 4)
                    score = self._w[W_PORTRAIT_WIGHT][W_TOTAL] * w_random
                    score_total += score
                    score_detail_list.append(u"sync_home_no_yh_yc[{sync_home_w}]=basic[{basic_w}]*random_weight[{w}]".format(
                        sync_home_w=score, basic_w=self._w[W_PORTRAIT_WIGHT][W_TOTAL], w=w_random
                    ))

            # 时间段加权
            if filter_flag:
                sync_home_time = data["_source"]["sync_home_time"]
                if sync_home_time >= self._half_hour_ago:
                    score_total += self._w[W_TIME_WEIGHT][W_HALF_HOUR]
                    score_detail_list.append(u"half_hour[{hour_score}]".format(
                        hour_score=self._w[W_TIME_WEIGHT][W_HALF_HOUR]))
                elif sync_home_time >= self._one_hour_ago:
                    score_total += self._w[W_TIME_WEIGHT][W_HOUR_1]
                    score_detail_list.append(u"one_hour[{hour_score}]".format(
                        hour_score=self._w[W_TIME_WEIGHT][W_HOUR_1]))
                elif sync_home_time >= self._three_hour_ago:
                    score_total += self._w[W_TIME_WEIGHT][W_HOUR_3]
                    score_detail_list.append(u"three_hour[{hour_score}]".format(
                        hour_score=self._w[W_TIME_WEIGHT][W_HOUR_3]))
                elif sync_home_time >= self._twelve_hour_ago:
                    score_total += self._w[W_TIME_WEIGHT][W_HOUR_12]
                    score_detail_list.append(u"twelve_hour[{hour_score}]".format(
                        hour_score=self._w[W_TIME_WEIGHT][W_HOUR_12]))
                elif sync_home_time >= self._twenty_four_hour_ago:
                    score_total += self._w[W_TIME_WEIGHT][W_HOUR_24]
                    score_detail_list.append(u"twenty_hour[{hour_score}]".format(
                        hour_score=self._w[W_TIME_WEIGHT][W_HOUR_24]))

        score_dict[u"total"] = round(score_total, 4) if filter_flag else -1.0
        score_dict[u"detail"] = score_detail_list
        raise gen.Return(score_dict)

    @gen.coroutine
    def _calc_level_by_variation_coefficient(self, level1_ids_list, level3_ids_list, score_detail_list):
        gen_log.debug("level1_ids_list: %s, level3_ids_list: %s",
                      str(level1_ids_list), str(level3_ids_list))
        level_score_total = 0.0
        if not level1_ids_list:
            raise gen.Return(level_score_total)

        try:
            w1_score_detail = ''
            w3_score_detail = ''
            # 判断品类的文章是否需要过滤
            # 当三级品类中的不感兴趣降为0时需要过滤
            filter_flag = False
            for l1 in level1_ids_list:
                weight = self._cate_prefer[YOUHUI_KEY][PARA].get(l1, '')
                gen_log.debug(
                    "_calc_level_by_variation_coefficient l1: %s, weight: %s", l1, weight)
                if weight:
                    alpha, w1 = weight[0].split(WELL)
                    alpha = float(alpha)
                    w1 = float(w1)
                    w1_score = (self._w[W_PORTRAIT_WIGHT]
                                [W_BLUR_CATE] - alpha) * w1
                    w1_score_detail = u"level_by_vc[l1id={l1},{w1_score} " \
                                      u"= ({basic}-{alpha}) * {w1}".format(l1=l1, w1_score=w1_score,
                                                                           basic=self._w[W_PORTRAIT_WIGHT][W_BLUR_CATE], alpha=alpha, w1=w1)
                    w3_score = 0.0
                    compare_score = 0.0
                    gen_log.debug(
                        "_calc_level_by_variation_coefficient alpha: %s, w1: %s", alpha, w1)
                    for l3 in level3_ids_list:
                        w3 = self._cate_prefer[YOUHUI_KEY][ACCURATE_KEY].get(
                            l3, '')
                        gen_log.debug(
                            "_calc_level_by_variation_coefficient l3: %s, w3: %s", l3, w3)
                        if w3:
                            w3 = float(w3[0])
                            # 判断是否降权到过滤
                            if w3 <= self._w[W_THRESHOLD]:
                                filter_flag = True
                                break

                            w3_weight = (
                                self._w[W_PORTRAIT_WIGHT][W_ACCURATE_CATE] + 2 * alpha) * w3

                            gen_log.debug(
                                "_calc_level_by_variation_coefficient w3_weight: %s", w3_weight)

                            if (w1_score + w3_weight) > compare_score:
                                w3_score = w3_weight
                                w3_score_detail = u"l3id={l3}, {score}=({basic} + 2*{alpha}) * {w3}]".format(l3=l3,
                                                                                                             basic=self._w[W_PORTRAIT_WIGHT][
                                                                                                                 W_ACCURATE_CATE],
                                                                                                             alpha=alpha, w3=w3, score=w3_weight)
                    if (w1_score + w3_score) > level_score_total:
                        level_score_total = w1_score + w3_score
                if filter_flag:
                    break
            # 过滤降权值将为-1.0
            if filter_flag:
                level_score_total = -1.0
                w1_score_detail = ''
                w3_score_detail = ''
            score_detail_list.append("%s\n%s" % (
                w1_score_detail, w3_score_detail))
        except Exception as e:
            gen_log.error(
                "_calc_level_by_variation_coefficient error(%s)", str(e))
        gen_log.debug(
            "_calc_level_by_variation_coefficient level_score_total: %s", level_score_total)

        raise gen.Return(level_score_total)
