# coding=utf-8
import json

import tornado
from tornado import gen
from tornado.httpclient import AsyncHTTPClient, HTTPClient, HTTPRequest
from tornado.httputil import url_concat
from tornado.log import gen_log


BACKGROUND_FOLLOW_SIZE = 1000
BACKGROUND_FOLLOW_WITH_MORE = 1
BACKGROUND_FOLLOW_WITH_ARTICLE = 0
BACKGROUND_FOLLOW_URL = "http://dingyueapi.smzdm.com:809/tuijian/rules"
BACKGROUND_FOLLOW_URL_TIME_OUT = 3


class AddFollow(object):
    def __init__(self, user_id, offset, limit, platform):
        self.user_id = user_id
        self.offset = offset
        self.limit = limit
        self.platform = platform

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
        gen_log.debug("url: %s" % url)
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
        background_result_dict = json.loads(background_result_str)
        background_result_data = background_result_dict.get("data", [])
        gen_log.info("background_result_data num: %s" % len(background_result_data))

        result = []
        for data in background_result_data:
            # 如果该规则用户已关注，则跳过本次
            if int(data.get("is_followed")) == 1:
                continue
            # 如果条数已满足要求，则退出
            if len(result) >= self.limit:
                break

            # 只选取需要的字段
            d = {k: v for k, v in data.iteritems() if k in self.background_follow_result_fields}
            d["data_type"] = self.background_data_signal  # 增加标识，表明为后台配置结果
            # 推荐reason
            d["tuijian_reason"] = ""

            result.append(d)
        raise gen.Return(result)

    @property
    @tornado.gen.coroutine
    def follow_content(self):
        background_data = yield self.query_background_follow()

        raise gen.Return(background_data)
