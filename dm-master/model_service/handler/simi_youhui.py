# coding=utf-8

import numpy as np
import pandas
import sys
import tornado
from datetime import datetime

from tornado import web, gen
from tornado.log import gen_log

from base.basehandler import BaseHandler
from biz.simi_youhui import REC_MIN_NUM, REC_MAX_NUM, DEFAULT_SIMI_WEIGHTS, DEFAULT_COMMBINE_WEIGHTS, REC_DATA_DAY, \
    REC_TOUR_DATA_DAY, DEFAULT_SIMI_EXTRA_WEIGHTS, OUTPUT_LEVEL_2, THRESHOLD_SIMI_SCORE
from comm.consts import YOUHUI_PRIMARY, YOUHUI_BASE_SIMI_RES_KEY, YOUHUI_BASE_SIMI_RES_KEY_EXPIRE
from model.base_simi_youhui import YouHuiBaseSimilarity
from model.simi_youhui import YouHuiPreProcess, YouHuiSimilarity
from util.mysql import TorMysqlClient
from util.redis_client import RedisClient

RECOMMEND_POOLS_UPDATE_FREQUENCY = 1 * 60
RECOMMEND_POOLS = None
RECOMMEND_POOLS_UPDATE_TIME = datetime(1970, 1, 1)
article_fields = ["article_id", "pubdate", "channel", "mall", "mall_id", "brand", "brand_id",
                  "yh_type", "pro_id", "heat", "level_1_id", "level_1", "level_2_id", "level_2", "level_3_id",
                  "level_3", "level_4_id", "level_4", "title", "subtitle"]


class BaseSimilarityHandler(BaseHandler):
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        try:
            article_id = self.get_argument("article_id", "-1")
            action = self.get_argument("action", "query")
            min_num = int(self.get_argument("min_num", REC_MIN_NUM))
            max_num = int(self.get_argument("max_num", REC_MAX_NUM))
            threshold_simi_score = float(self.get_argument("threshold_simi_score", THRESHOLD_SIMI_SCORE))
            rec_data_day = int(self.get_argument("rec_data_day", REC_DATA_DAY))
            rec_tour_data_day = int(self.get_argument("rec_tour_data_day", REC_TOUR_DATA_DAY))
            output_level = int(self.get_argument("output_level", OUTPUT_LEVEL_2))
            model_type = self.get_argument("model_type", "pure_simi")
            data_source = self.get_argument("data_source", "mysql")
            file_path = self.get_argument("file_path", "./data/simi_youhui_data.xlsx")
            random_state = int(self.get_argument("random_state", "-1"))

            base_weights = DEFAULT_SIMI_WEIGHTS
            extra_weights = DEFAULT_SIMI_EXTRA_WEIGHTS
            # 获取不同特征权重
            weight_pro_id = float(self.get_argument("weight_pro_id", base_weights.get("pro_id")))
            base_weights["pro_id"] = weight_pro_id
            weight_level_4_id = float(self.get_argument("weight_level_4_id", base_weights.get("level_4_id")))
            base_weights["level_4_id"] = weight_level_4_id
            weight_level_3_id = float(self.get_argument("weight_level_3_id", base_weights.get("level_3_id")))
            base_weights["level_3_id"] = weight_level_3_id
            weight_level_2_id = float(self.get_argument("weight_level_2_id", base_weights.get("level_2_id")))
            base_weights["level_2_id"] = weight_level_2_id
            weight_level_1_id = float(self.get_argument("weight_level_1_id", base_weights.get("level_1_id")))
            base_weights["level_1_id"] = weight_level_1_id
            weight_brand_id = float(self.get_argument("weight_brand_id", base_weights.get("brand_id")))
            base_weights["brand_id"] = weight_brand_id
            weight_title = float(self.get_argument("weight_title", base_weights.get("title")))
            base_weights["title"] = weight_title
            weight_sex = float(self.get_argument("weight_sex", base_weights.get("sex")))
            base_weights["sex"] = weight_sex
            weight_crowd = float(self.get_argument("weight_crowd", base_weights.get("crowd")))
            base_weights["crowd"] = weight_crowd
            weight_level_3_defect_title_extra = float(
                self.get_argument("weight_level_3_defect_title_extra", extra_weights.get("level_3_defect_title_extra")))
            extra_weights["level_3_defect_title_extra"] = weight_level_3_defect_title_extra

            commbine_weights = DEFAULT_COMMBINE_WEIGHTS
            weight_simi = float(self.get_argument("weight_simi", commbine_weights.get("simi")))
            commbine_weights["simi"] = weight_simi
            weight_heat = float(self.get_argument("weight_heat", commbine_weights.get("heat")))
            commbine_weights["heat"] = weight_heat

            redis_client = yield RedisClient().get_redis_client()
            mysql_client = TorMysqlClient()

            if action not in ("query", "update", "save"):
                raise ValueError("the value of action must 'query', 'update' or 'save'!")
            if model_type not in ("pure_simi", "base_simi"):
                raise ValueError("the value of model_type must 'pure_simi' or 'base_simi'!")
            if data_source not in ("mysql", "file"):
                raise ValueError("the value of data_source must 'mysql' or 'file'!")

            if action == "query":
                key = YOUHUI_BASE_SIMI_RES_KEY % article_id
                gen_log.info("redis_key: %s" % key)

                rec_data = redis_client.get(key)
                result = pandas.json.loads(rec_data) if rec_data else dict({"info": "",
                                                                            "rec": None})
            else:
                if data_source == "mysql":
                    current_article_info = yield self._query_current_article_info(mysql_client, article_id,
                                                                                  article_fields)

                    recommend_pools = yield self._update_recommend_pools(mysql_client, rec_data_day, rec_tour_data_day,
                                                                         article_fields)
                else:
                    current_article_info = self._query_current_article_info_local(file_path, random_state)

                    recommend_pools = self._update_recommend_pools_local(file_path)
                # recommend_pools = recommend_pools.head(10)
                recommend_pools = recommend_pools
                yhs = YouHuiSimilarity(current_article_info, recommend_pools, base_weights, extra_weights, min_num,
                                       max_num, threshold_simi_score, output_level,
                                       **commbine_weights) if model_type == "pure_simi" else YouHuiBaseSimilarity(
                    current_article_info, recommend_pools, base_weights, extra_weights, min_num,
                    max_num, threshold_simi_score, output_level, **commbine_weights)

                result = yhs.rec_data

            result = pandas.json.dumps(result)

            if action == "save":
                key = YOUHUI_BASE_SIMI_RES_KEY % article_id
                gen_log.info("redis_key: %s" % key)
                redis_client.setex(key, result, YOUHUI_BASE_SIMI_RES_KEY_EXPIRE)
        except Exception as e:
            import traceback
            error_msg = traceback.format_exc()
            gen_log.error(error_msg)
            result = {"error_msg": str(e)}
        self.jsonify(result)

    def _query_current_article_info_local(self, file_path, random_state):
        """
        从指定文件中获取一条文章信息
        :param file_path: 文章路径
        :param random_state: 抽样时所用的随机种子，如果为-1，则自动生成一个随机整数
        :return:
        """
        data = pandas.read_excel(file_path)

        random_state = random_state if random_state >= 0 else np.random.randint(0, sys.maxint, size=1)[0]

        frame = data.sample(n=1, random_state=random_state)
        frame = YouHuiPreProcess(frame).data
        return frame

    @tornado.gen.coroutine
    def _query_current_article_info(self, mysql_client, article_id, fields):
        """
        生成当前文章的信息
        :param mysql_client: mysql client
        :param article_id: 文章id
        :param fields: 查询文章的信息字段
        :return:
        """
        query_article_info_sql = """
            select id as article_id, pubdate, channel, mall, mall_id, brand, brand_id,
            yh_type, pro_id, sum_collect_comment as heat, level_1_id, level_1, level_2_id, level_2, level_3_id,
            level_3, level_4_id, level_4, title, subtitle
            from {youhui_table}
            where id={article_id}
        """.format(youhui_table=YOUHUI_PRIMARY, article_id=article_id)

        data_tuples = yield mysql_client.fetchall(query_article_info_sql)
        frame = pandas.DataFrame(data=list(data_tuples), columns=fields)
        frame = YouHuiPreProcess(frame).data
        raise gen.Return(frame)

    def _update_recommend_pools_local(self, file_path):
        data = pandas.read_excel(file_path)
        frame = YouHuiPreProcess(data).data
        return frame

    @tornado.gen.coroutine
    def _update_recommend_pools(self, mysql_client, date_time, tour_date_time, fields):
        """
        生成指定天数内的推荐数据
        :param mysql_client: mysql client
        :param date_time: 表示普通文章天数，int
        :param tour_date_time: 表示旅游类文章天数，int
        :param fields: 查询文章的信息字段
        :return:
        """
        global RECOMMEND_POOLS, RECOMMEND_POOLS_UPDATE_TIME

        current_time = datetime.now()
        delta = (current_time - RECOMMEND_POOLS_UPDATE_TIME).total_seconds()

        # 指定间隔内不更新推荐池子
        if delta < RECOMMEND_POOLS_UPDATE_FREQUENCY:
            raise gen.Return(RECOMMEND_POOLS)

        # 指定间隔后开始更新推荐数据池

        query_rec_pools_info_sql = """
            select id as article_id, pubdate, channel, mall, mall_id, brand, brand_id,
            yh_type, pro_id, sum_collect_comment as heat, level_1_id, level_1, level_2_id, level_2, level_3_id,
            level_3, level_4_id, level_4, title, subtitle
            from {youhui_table}
            where pubdate > date_add(now(), interval -{date_time} day)
            and yh_status = 1
            and channel in (1, 2, 5)
            and stock_status = 0
            and level_1_id <> {tour_id}
            union
            select id as article_id, pubdate, channel, mall, mall_id, brand, brand_id,
            yh_type, pro_id, sum_collect_comment as heat, level_1_id, level_1, level_2_id, level_2, level_3_id,
            level_3, level_4_id, level_4, title, subtitle
            from {youhui_table}
            where level_1_id = {tour_id}
            and pubdate > date_add(now(), interval -{tour_date_time} day)
            and yh_status = 1
            and channel in (1, 2, 5)
            and stock_status = 0
        """.format(youhui_table=YOUHUI_PRIMARY, date_time=date_time, tour_date_time=tour_date_time, tour_id=5337)

        gen_log.debug("query_rec_pools_info_sql: %s" % query_rec_pools_info_sql)
        data_tuples = yield mysql_client.fetchall(query_rec_pools_info_sql)
        frame = pandas.DataFrame(data=list(data_tuples), columns=fields)
        # frame.set_index(COL_ARTICLE_ID, inplace=True)
        frame = YouHuiPreProcess(frame).data

        RECOMMEND_POOLS = frame
        RECOMMEND_POOLS_UPDATE_TIME = current_time
        gen_log.info("[update recommend pools]")

        raise gen.Return(frame)
