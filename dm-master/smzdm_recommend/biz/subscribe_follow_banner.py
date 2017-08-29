# coding=utf-8
import json
from random import sample

import tornado
from tornado import gen
from tornado.httpclient import AsyncHTTPClient
from tornado.httputil import url_concat
from tornado.log import gen_log

from biz_config.follow_banner import FOLLOW_BANNER_BACKEND_TOP_REDIS_KEY, FOLLOW_BANNER_BACKEND_TOP_REDIS_KEY_EXPIRE

BACKGROUND_FOLLOW_SIZE = 1000
BACKGROUND_FOLLOW_WITH_MORE = 1
BACKGROUND_FOLLOW_WITH_ARTICLE = 0
BACKGROUND_FOLLOW_URL = "http://dingyueapi.smzdm.com:809/tuijian/rules"
BACKGROUND_FOLLOW_URL_TIME_OUT = 3


class FollowBanner(object):
    def __init__(self, redis_client, user_id, limit):
        self.redis_client = redis_client
        self.user_id = user_id
        self.limit = limit

        # 后台配置关注url
        self.background_follow_url = BACKGROUND_FOLLOW_URL
        self.background_follow_url_parms_key = ["user_id", "size", "with_more", "with_article"]
        self.background_follow_result_fields = ["tuijian", "tuijian_id", "tuijian_type"]  # 后台结果中需要用到的字段
        self.result_fields = ["tuijian", "data_type", "tuijian_id", "tuijian_type", "tuijian_reason"]
        self.background_data_signal = "0"  # 后台配置数据标识
        self.individualization_data_signal = "1"  # 后台配置数据标识

    @tornado.gen.coroutine
    def query_background_follow(self):
        """
        查询后台配置的关注规则，
        内部接口 twiki: http://twiki.team.bq.com:8081/twiki/Main/Guanzhu_tuijian_rules
        :return:
        """
        values = [self.user_id, BACKGROUND_FOLLOW_SIZE, BACKGROUND_FOLLOW_WITH_MORE, BACKGROUND_FOLLOW_WITH_ARTICLE]
        params = dict(zip(self.background_follow_url_parms_key, values))
        # params = {}
        url = url_concat(self.background_follow_url, params)
        gen_log.info("url: %s" % url)
        # http_client = HTTPClient()
        # response = http_client.fetch(url, request_timeout=BACKGROUND_FOLLOW_URL_TIME_OUT)

        http_client = AsyncHTTPClient()
        response = yield http_client.fetch(url, request_timeout=BACKGROUND_FOLLOW_URL_TIME_OUT)
        gen_log.info("background data request time: %s ms" % (response.request_time * 1000))
        if response.code != 200:
            gen_log.error("url: %s faild, status: %s" % (response.effective_url, response.code))
            raise gen.Return([])

        background_result_str = response.body
        # background_result_str = '{"data": [{"qianzhi_id": "", "is_followed": 0, "creation_date": "2017-07-04 14:48:38", "qianzhi_type": "", "is_goodarticle": "1", "id": "461", "keyword_hash": "1637045446", "modification_date": "2017-07-04 16:42:56", "tuijian": "o(\\u2229_\\u2229)o", "qianzhi_hash": "", "sort_num": "1", "tuijian_desc": "", "keyword_id": "83615", "type": "user", "qianzhi": "", "tuijian_type": "user", "status": "1", "tuijian_id": "83615", "is_goodprice": "1", "tuijian_hash": "1637045446", "keyword": "o(\\u2229_\\u2229)o", "level": "0"}, {"qianzhi_id": "", "is_followed": 0, "creation_date": "2017-07-04 11:33:21", "qianzhi_type": "", "is_goodarticle": "1", "id": "462", "keyword_hash": "", "modification_date": "2017-07-04 16:42:56", "tuijian": "G\\u8054\\u8d5b", "qianzhi_hash": "", "sort_num": "2", "tuijian_desc": "", "keyword_id": "107059", "type": "tag", "qianzhi": "", "tuijian_type": "tag", "status": "1", "tuijian_id": "107059", "is_goodprice": "1", "tuijian_hash": "", "keyword": "G\\u8054\\u8d5b", "level": "0"}], "total": "", "error_code": "0", "error_msg": ""}'
        # background_result_str = '{"data": [{"qianzhi_id": "", "is_followed": 0, "creation_date": "2017-07-04 14:48:38", "qianzhi_type": "", "is_goodarticle": "1", "id": "461", "keyword_hash": "1637045446", "modification_date": "2017-07-04 16:42:56", "tuijian": "o(\u2229_\u2229)o", "qianzhi_hash": "", "sort_num": "1", "tuijian_desc": "", "keyword_id": "83615", "type": "user", "qianzhi": "", "tuijian_type": "user", "status": "1", "tuijian_id": "83615", "is_goodprice": "1", "tuijian_hash": "1637045446", "keyword": "o(\u2229_\u2229)o", "level": "1"}, {"qianzhi_id": "", "is_followed": 0, "creation_date": "2017-07-04 11:33:21", "qianzhi_type": "", "is_goodarticle": "1", "id": "462", "keyword_hash": "", "modification_date": "2017-07-04 16:42:56", "tuijian": "G\u8054\u8d5b", "qianzhi_hash": "", "sort_num": "2", "tuijian_desc": "", "keyword_id": "107059", "type": "tag", "qianzhi": "", "tuijian_type": "tag", "status": "1", "tuijian_id": "107059", "is_goodprice": "1", "tuijian_hash": "", "keyword": "G\u8054\u8d5b", "level": "1"}, {"qianzhi_id": "", "is_followed": 0, "creation_date": "2017-07-04 10:48:36", "qianzhi_type": "", "is_goodarticle": "1", "id": "463", "keyword_hash": "", "modification_date": "2017-07-04 16:42:56", "tuijian": "\u4eac\u4e1cATM", "qianzhi_hash": "", "sort_num": "3", "tuijian_desc": "", "keyword_id": "56787", "type": "tag", "qianzhi": "", "tuijian_type": "tag", "status": "1", "tuijian_id": "56787", "is_goodprice": "1", "tuijian_hash": "", "keyword": "\u4eac\u4e1cATM", "level": "1"}, {"qianzhi_id": "", "is_followed": 0, "creation_date": "2017-07-04 10:27:35", "qianzhi_type": "", "is_goodarticle": "1", "id": "464", "keyword_hash": "3571149671", "modification_date": "2017-07-04 16:42:56", "tuijian": "testtest29", "qianzhi_hash": "", "sort_num": "4", "tuijian_desc": "", "keyword_id": "537923", "type": "user", "qianzhi": "", "tuijian_type": "user", "status": "1", "tuijian_id": "537923", "is_goodprice": "1", "tuijian_hash": "3571149671", "keyword": "testtest29", "level": "0"}, {"qianzhi_id": "", "is_followed": 0, "creation_date": "2017-07-04 10:24:27", "qianzhi_type": "", "is_goodarticle": "1", "id": "465", "keyword_hash": "6250149717", "modification_date": "2017-07-04 16:42:56", "tuijian": "\u8473\u8564\u5149\u5e74", "qianzhi_hash": "", "sort_num": "5", "tuijian_desc": "", "keyword_id": "411213", "type": "user", "qianzhi": "", "tuijian_type": "user", "status": "1", "tuijian_id": "411213", "is_goodprice": "1", "tuijian_hash": "6250149717", "keyword": "\u8473\u8564\u5149\u5e74", "level": "0"}, {"qianzhi_id": "", "is_followed": 0, "creation_date": "2017-07-03 14:48:24", "qianzhi_type": "", "is_goodarticle": "1", "id": "466", "keyword_hash": "", "modification_date": "2017-07-04 16:42:56", "tuijian": "sds/\u82f9\u679c111", "qianzhi_hash": "", "sort_num": "6", "tuijian_desc": "", "keyword_id": "32908", "type": "brand", "qianzhi": "", "tuijian_type": "brand", "status": "1", "tuijian_id": "32908", "is_goodprice": "1", "tuijian_hash": "", "keyword": "sds/\u82f9\u679c111", "level": "0"}, {"qianzhi_id": "", "is_followed": 0, "creation_date": "2017-05-12 10:48:10", "qianzhi_type": "", "is_goodarticle": "1", "id": "467", "keyword_hash": "", "modification_date": "2017-07-04 16:42:56", "tuijian": "\u4e00\u8f66\u795e\u4ef7\u683c", "qianzhi_hash": "", "sort_num": "7", "tuijian_desc": "", "keyword_id": "126701", "type": "tag", "qianzhi": "", "tuijian_type": "tag", "status": "1", "tuijian_id": "126701", "is_goodprice": "1", "tuijian_hash": "", "keyword": "\u4e00\u8f66\u795e\u4ef7\u683c", "level": "0"}], "total": "", "error_code": "0", "error_msg": ""}'
        background_result_dict = json.loads(background_result_str)
        background_result_data = background_result_dict.get("data", [])
        gen_log.info("background_result_data num: %s" % len(background_result_data))

        data_is_top, data_not_top = [], []
        # result = []
        for data in background_result_data:
            # 如果该规则用户已关注，则跳过本次
            if int(data.get("is_followed")) == 1:
                continue

            # 只选取需要的字段
            d = {k: v for k, v in data.iteritems() if k in self.background_follow_result_fields or k == "level"}
            d["data_type"] = self.background_data_signal  # 增加标识，表明为后台配置结果
            # 推荐reason
            d["tuijian_reason"] = ""

            if int(d.get("level")) == 1:
                del d["level"]
                data_is_top.append(d)
            else:
                del d["level"]
                data_not_top.append(d)

            # # 如果条数已满足要求，则退出
            # if len(result) >= self.limit:
            #     break
            # result.append(d)
        # raise gen.Return(result)
        raise gen.Return((data_is_top, data_not_top))

    @tornado.gen.coroutine
    def transform(self, data_is_top, data_not_top):
        """
        :param data_is_top: 后台置顶数据
        :param data_not_top: 后台非置顶数据
        :return:
        """
        result_data = []
        gen_log.info("data_is_top num: %s" % len(data_is_top))
        gen_log.info("data_not_top num: %s" % len(data_not_top))
        # 置顶规则不超过一个
        if len(data_is_top) <= 1:
            result_data.extend(data_is_top)
        else:
            key = FOLLOW_BANNER_BACKEND_TOP_REDIS_KEY % self.user_id
            redis_data = self.redis_client.get(key)
            redis_data = json.loads(redis_data) if redis_data else None

            # redis中不存在后台置顶规则或redis中后台置顶规则与最新查询的不一样，则将point赋值为0
            if not redis_data or redis_data.get("data") != data_is_top:
                point = 0
            else:
                point = redis_data.get("point")

            ind = point % len(data_is_top)
            result_data.append(data_is_top[ind])

            # 将point加1后与置顶规则一起存入redis
            point += 1
            rd = {"data": data_is_top, "point": point}
            rd = json.dumps(rd)
            self.redis_client.setex(key, rd, FOLLOW_BANNER_BACKEND_TOP_REDIS_KEY_EXPIRE)

        # 如果存在置顶数据（即长度为1）则总长度减1
        k = self.limit - 1 if len(result_data) == 1 else self.limit
        k = k if k <= len(data_not_top) else len(data_not_top)

        # 随机选取 k 个规则
        result_data.extend(sample(data_not_top, k))

        raise gen.Return(result_data)

    @property
    @tornado.gen.coroutine
    def follow_content(self):
        data_is_top, data_not_top = yield self.query_background_follow()
        background_data = yield self.transform(data_is_top, data_not_top)

        raise gen.Return(background_data)
