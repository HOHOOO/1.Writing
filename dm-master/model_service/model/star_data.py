# coding=utf-8

from collections import OrderedDict
from datetime import datetime, timedelta

import pandas
from tornado.log import gen_log
from tornado.options import options

from biz.star_rule import *
from comm.consts import *


COL_ARTICLE_ID = "article_id"
COL_HOT = "hot"
COL_PUBDATE = "pubdate"
COL_TAG = "tags"
COL_MALL_ID = "mall_id"
COL_MALL = "mall"
COL_YH_TYPE = "yh_type"
COL_CATE_1 = "cate_1"
COL_PRICE = "price"

COL_REC = "rec"
COL_INFO = "info"
COL_BASE = "base"
COL_REC_MODULE_NAME = "module_name"


class StarDataRec(object):
    """
    author: wangwei01
    redmine: http://redmine.team.bq.com:1080/issues/52778
    """

    def __init__(self, article_id, current_article_frame, recommend_pools_frame, recommend_data_min_num, recommend_data_max_num):
        """

        :param article_id: 当前文章id,int/long
        :param current_article_frame: 当前文章信息，DataFrame
        :param recommend_pools_frame: 推荐池子信息，DataFrame
        :param recommend_data_min_num: 推荐结果最小数量，int
        :param recommend_data_max_num: 推荐结果最大数量，int
        """

        self.article_id = article_id
        self.current_article_info = current_article_frame.to_dict("index").get(article_id, None)
        gen_log.info("current_article_info: %s" % self.current_article_info)
        self.recommend_pools = recommend_pools_frame
        self.min = recommend_data_min_num
        self.max = recommend_data_max_num

        self.base = None
        self.rec_module_name = None

        self.current_time = datetime.now()
        self.hour_12_ago = (self.current_time - timedelta(hours=HOUR_12)).strftime(TIME_FORMAT_1)
        self.hour_24_ago = (self.current_time - timedelta(hours=HOUR_24)).strftime(TIME_FORMAT_1)

        self.day_3_ago = (self.current_time - timedelta(days=DAY_3)).strftime(TIME_FORMAT_1)
        self.day_7_ago = (self.current_time - timedelta(days=DAY_7)).strftime(TIME_FORMAT_1)

    def _construct_rec_data(self, current_rec_pools=None, module_name=None, base=None):
        result = {COL_INFO: None,
                  COL_REC_MODULE_NAME: None,
                  COL_BASE: None,
                  COL_REC: None
                 }

        result[COL_INFO] = self.current_article_info

        if current_rec_pools is not None:
            # 对生成的推荐数据按照热度降序排
            current_rec_pools = current_rec_pools.sort_values(by=COL_HOT, ascending=False).head(self.max)
            result[COL_REC_MODULE_NAME] = module_name
            result[COL_BASE] = base

            rec_data = current_rec_pools.to_dict("index") if current_rec_pools is not None else None
            gen_log.debug("rec_data: %s" % rec_data)
            result[COL_REC] = rec_data

            self.base = base
            self.rec_module_name = module_name

        return result

    def _cal_huodong_and_quan_rec_data(self):
        """
        针对日志类型为huodong或者quan的文章做推荐
        :return:
        """
        # 当前文章的日志类型特征
        feature = self.current_article_info.get(COL_YH_TYPE)
        rule = RULE.get(RULE_YH_TYPE)

        # 文章特征不在指定规则列表中则跳过
        if feature not in rule.keys():
            return

        module_name = rule.get(feature)
        base_prefix = "yh_type: %s"
        base = base_prefix % feature
        gen_log.info("%s start calculate" % base)

        raw_rec_pools = self.recommend_pools
        # 将日志类型和当前文章日志类型一致的文章作为推荐池子
        raw_rec_pools = raw_rec_pools.query("yh_type == @feature")

        # 得到推荐池子中最近 12h 内的数据
        current_rec_pools = raw_rec_pools[raw_rec_pools[COL_PUBDATE] > self.hour_12_ago]
        # 在推荐池子中过滤掉本身
        current_rec_pools = current_rec_pools[current_rec_pools.index != self.article_id]
        # 如果最近 12h 范围内的推荐数据不够，则扩大时间范围到 24h
        if current_rec_pools.shape[0] < self.min:
            current_rec_pools = raw_rec_pools[raw_rec_pools[COL_PUBDATE] > self.hour_24_ago]
            # 在推荐池子中过滤掉本身
            current_rec_pools = current_rec_pools[current_rec_pools.index != self.article_id]

        if current_rec_pools.shape[0] < self.min:
            gen_log.warn("yh_type: %s recommend pool size is %s" % (feature, current_rec_pools.shape[0]))
            return

        return self._construct_rec_data(current_rec_pools, module_name, base)

    def _cal_more_tag_rec_data(self):
        """
        针对文章标签属性中包含某些特定标签的文章做推荐
        :return:
        """
        # 当前文章的所有标签
        current_article_all_tags = self.current_article_info.get(COL_TAG).split(DOT)

        base_prefix = "more_tag: %s"

        rule_keys = [RULE_TAG_1, RULE_TAG_2, RULE_TAG_3]

        for rule_key in rule_keys:
            rule = RULE.get(rule_key)
            filter_tags = [tag for tag in current_article_all_tags if tag in rule.keys()]

            # 如果当前文章的所有标签都不在特定规则里，则跳出本次计算
            if not filter_tags:
                continue
            # 当前文章的多个标签都满足特定规则，则取第一个标签
            feature = filter_tags[0]

            module_name = rule.get(feature)
            base = base_prefix % feature
            gen_log.info("%s start calculate" % base)

            raw_rec_pools = self.recommend_pools

            # 将包含该特征标签的文章作为推荐池子
            raw_rec_pools = raw_rec_pools[raw_rec_pools[COL_TAG].map(lambda x: feature in x.split(DOT))]

            if rule_key == RULE_TAG_1:
                time_date = self.day_7_ago
            elif rule_key == RULE_TAG_2:
                time_date = self.hour_24_ago
            else:
                time_date = self.day_3_ago

            # 得到最近 x 小时内的数据
            current_rec_pools = raw_rec_pools[raw_rec_pools[COL_PUBDATE] > time_date]
            # 在推荐池子中过滤掉本身
            current_rec_pools = current_rec_pools[current_rec_pools.index != self.article_id]
            # 如果该规则没有数据，则跳过
            if current_rec_pools.shape[0] < self.min:
                gen_log.warn("more_tag: %s recommend pool size is %s" % (feature, current_rec_pools.shape[0]))
                break

            return self._construct_rec_data(current_rec_pools, module_name, base)

    def _cal_collect_goods_rec_data(self):
        """
        针对满足凑单品（日志类型为单品 & 价格高于特定值 & 符合特定商城）文章做推荐
        :return:
        """
        # 当前商品的价格低于特定值则直接不计算
        if self.current_article_info.get(COL_PRICE) < COLLECT_GOOD_PRICE_MIN:
            return
        # 当前商品的类型不是单品则直接不计算
        if self.current_article_info.get(COL_YH_TYPE) != YH_DANPIN_TYPE:
            return

        # 当前文章的商城特征
        feature = self.current_article_info.get(COL_MALL_ID)
        rule = RULE.get(RULE_COLLECT_GOOD)

        # 文章特征不在指定规则列表中则跳过
        if feature not in rule.keys():
            return

        module_name = rule.get(feature)
        base_prefix = "mall_collect_good: %s"
        base = base_prefix % feature
        gen_log.info("%s start calculate" % base)

        raw_rec_pools = self.recommend_pools
        # 将标签中包含凑单品的文章作为推荐池子
        raw_rec_pools = raw_rec_pools[(raw_rec_pools[COL_MALL_ID] == feature) & (raw_rec_pools[COL_TAG].str.contains(COLLECT_GOOD_TAG))]

        # 得到推荐池子中最近 24h 内的数据
        current_rec_pools = raw_rec_pools[raw_rec_pools[COL_PUBDATE] > self.hour_24_ago]
        # 在推荐池子中过滤掉本身
        current_rec_pools = current_rec_pools[current_rec_pools.index != self.article_id]
        if current_rec_pools.shape[0] < self.min:
            gen_log.warn("mall_collect_good: %s recommend pool size is %s" % (feature, current_rec_pools.shape[0]))
            return

        return self._construct_rec_data(current_rec_pools, module_name, base)

    def _cal_mall_rank_list(self):
        """
        针对满足特定商城的文章做排行榜推荐
        :return:
        """
        # 当前文章的商城特征
        feature = self.current_article_info.get(COL_MALL_ID)
        rule = RULE.get(RULE_MALL_RANK_LIST)

        # 文章特征不在指定规则列表中则跳过
        if feature not in rule.keys():
            return

        module_name = rule.get(feature)
        base_prefix = "mall_rank_list: %s"
        base = base_prefix % feature
        gen_log.info("%s start calculate" % base)

        raw_rec_pools = self.recommend_pools
        # 将商城与当前文章商城相同的优惠文章作为推荐池子
        raw_rec_pools = raw_rec_pools[raw_rec_pools[COL_MALL_ID] == feature]

        # 得到推荐池子中最近 12h 内的数据
        current_rec_pools = raw_rec_pools[raw_rec_pools[COL_PUBDATE] > self.hour_12_ago]
        # 在推荐池子中过滤掉本身
        current_rec_pools = current_rec_pools[current_rec_pools.index != self.article_id]
        # 如果最近 12h 范围内的推荐数据不够，则扩大时间范围到 24h
        if current_rec_pools.shape[0] < self.min:
            current_rec_pools = raw_rec_pools[raw_rec_pools[COL_PUBDATE] > self.hour_24_ago]
            # 在推荐池子中过滤掉本身
            current_rec_pools = current_rec_pools[current_rec_pools.index != self.article_id]

        if current_rec_pools.shape[0] < self.min:
            gen_log.warn("mall_rank_list: %s recommend pool size is %s" % (feature, current_rec_pools.shape[0]))
            return

        return self._construct_rec_data(current_rec_pools, module_name, base)

    def _cal_cate_rank_list(self):
        """
        针对满足特定品类的文章做推荐
        :return:
        """
        # 当前文章的一级品类特征
        feature = self.current_article_info.get(COL_CATE_1)
        rule = RULE.get(RULE_CATE_RANK_LIST)

        # 文章特征不在指定规则列表中则跳过
        if feature not in rule.keys():
            return

        module_name = rule.get(feature)
        base_prefix = "cate_rank_list: %s"
        base = base_prefix % feature
        gen_log.info("%s start calculate" % base)

        raw_rec_pools = self.recommend_pools
        # 将一级品类与当前文章一级品类相同的优惠文章作为推荐池子
        raw_rec_pools = raw_rec_pools[raw_rec_pools[COL_CATE_1] == feature]

        # 得到推荐池子中最近 12h 内的数据
        current_rec_pools = raw_rec_pools[raw_rec_pools[COL_PUBDATE] > self.hour_12_ago]
        # 在推荐池子中过滤掉本身
        current_rec_pools = current_rec_pools[current_rec_pools.index != self.article_id]

        # 如果最近 12h 范围内的推荐数据不够，则扩大时间范围到 24h
        if current_rec_pools.shape[0] < self.min:
            current_rec_pools = raw_rec_pools[raw_rec_pools[COL_PUBDATE] > self.hour_24_ago]
            # 在推荐池子中过滤掉本身
            current_rec_pools = current_rec_pools[current_rec_pools.index != self.article_id]

        if current_rec_pools.shape[0] < self.min:
            gen_log.warn("cate_rank_list: %s recommend pool size is %s" % (feature, current_rec_pools.shape[0]))
            return

        return self._construct_rec_data(current_rec_pools, module_name, base)

    def _cal_rec_data(self):
        rec_data = self._cal_huodong_and_quan_rec_data()
        if rec_data:
            return rec_data
        else:
            rec_data = self._cal_more_tag_rec_data()

        if rec_data:
            return rec_data
        else:
            rec_data = self._cal_collect_goods_rec_data()

        if rec_data:
            return rec_data
        else:
            rec_data = self._cal_mall_rank_list()

        if rec_data:
            return rec_data
        else:
            rec_data = self._cal_cate_rank_list()

        # 所有规则都无法推荐出结果，则构造一个只包含当前文章信息的结果
        if not rec_data:
            rec_data = self._construct_rec_data()
        return rec_data

    @property
    def rec_data(self):
        """
        获取单个文章的推荐结果
        :return:
        """
        if not self.current_article_info or self.recommend_pools.shape[0] == 0:
            gen_log.warn("current_article_info or rec_pools is empty!")
            # 防止返回None，构造统一的返回格式。
            return self._construct_rec_data()
        rec_data = self._cal_rec_data()

        return rec_data


class StarDataPreProcess(object):
    def __init__(self, frame):
        self.data = self.process(frame)

    def process(self, frame):
        frame[COL_CATE_1].fillna(value="", inplace=True)
        frame[COL_MALL].fillna(value="", inplace=True)
        frame[COL_TAG].fillna(value="", inplace=True)

        frame[COL_CATE_1] = frame[COL_CATE_1].str.encode("utf-8")
        frame[COL_MALL] = frame[COL_MALL].str.encode("utf-8")
        frame[COL_TAG] = frame[COL_TAG].str.encode("utf-8")

        frame[COL_PRICE] = frame[COL_PRICE].map(lambda x: self._trans_str_price2float(x))
        # 将发布时间字段格式化为字符串
        frame[COL_PUBDATE] = frame[COL_PUBDATE].map(lambda x: x.strftime(TIME_FORMAT_1))

        return frame

    def _trans_str_price2float(self, x, default=-0.1):
        try:
            price = float(x)
        except (ValueError, TypeError):
            price = default
        return price
