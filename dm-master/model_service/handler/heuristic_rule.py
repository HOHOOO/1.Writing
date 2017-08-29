# coding=utf-8
import pandas
import tornado
from datetime import datetime

from tornado import web, gen
from tornado.log import gen_log

from base.basehandler import BaseHandler
from biz.star_rule import REC_MIN_NUM, REC_MAX_NUM, REC_DATA_DAY
from model.star_data import COL_ARTICLE_ID, StarDataRec, COL_INFO, \
    COL_REC, COL_BASE, COL_REC_MODULE_NAME, StarDataPreProcess
from util.mysql import MySQL, TorMysqlClient
from util.redis_client import RedisClient

from comm.consts import *

RECOMMEND_POOLS_UPDATE_FREQUENCY = 1 * 60
RECOMMEND_POOLS = None
RECOMMEND_POOLS_UPDATE_TIME = datetime(1970, 1, 1)
article_fields = ["article_id", "yh_type", "hot", "price", "cate_1", "mall", "mall_id", "tags", "pubdate"]


class StarDataHandler(BaseHandler):
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        """
        用于查询/更新单个文章的推荐数据
        :return:
        """
        article_id = int(self.get_argument("article_id"))
        action = self.get_argument("action", "query")
        min_num = int(self.get_argument("min_num", REC_MIN_NUM))
        max_num = int(self.get_argument("max_num", REC_MAX_NUM))
        rec_data_day = int(self.get_argument("rec_data_day", REC_DATA_DAY))

        redis_client = yield RedisClient().get_redis_client()
        mysql_client = TorMysqlClient()

        current_article_info = yield self._query_current_article_info(mysql_client, article_id, article_fields)

        try:
            if action not in ("query", "update", "save"):
                raise ValueError("the value of action must 'query', 'update' or 'save'!")

            if action == "query":
                key = STAR_DATA_RES_KEY % article_id
                gen_log.info("redis_key: %s" % key)
                rec_data = redis_client.get(key)
                result = pandas.json.loads(rec_data) if rec_data else dict({COL_INFO: current_article_info,
                                                                            COL_BASE: None,
                                                                            COL_REC_MODULE_NAME: None,
                                                                            COL_REC: None})
            else:
                recommend_pools = yield self._update_recommend_pools(mysql_client, rec_data_day, article_fields)
                sd = StarDataRec(article_id, current_article_info, recommend_pools, min_num, max_num)
                result = sd.rec_data
            result = pandas.json.dumps(result)

            if action == "save":
                key = STAR_DATA_RES_KEY % article_id
                gen_log.info("redis_key: %s" % key)
                redis_client.setex(key, result, STAR_DATA_RES_KEY_EXPIRE)
        except:
            import traceback
            error_msg = traceback.format_exc()
            result = {"error_msg": error_msg}

        self.jsonify(result)

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
        select t.article_id, t.yh_type, t.hot, t.price, t.cate_1, t.mall, t.mall_id, group_concat(t.tag) tags, t.pubdate
        from(
            select a.article_id, a.yh_type, a.hot, a.price, a.cate_1, a.mall, a.mall_id, b.name as tag, a.pubdate
            from (
                select a.id as article_id,
                a.yh_type, a.sum_collect_comment as hot, a.digital_price as price, a.level_1 as cate_1, a.mall, a.mall_id, a.pubdate, b.tag_id
                from {youhui_table}
                as a
                left join {youhui_tag_table} as b
                on a.id = b.article_id
                where a.id = {article_id}
            ) as a
            left join {smzdm_tag} as b
            on a.tag_id = b.id
        ) as t
        group by t.article_id
        """.format(youhui_table=YOUHUI_PRIMARY, youhui_tag_table=YOUHUI_TAG,
                   article_id=article_id, smzdm_tag=SMZDM_TAG)

        data_tuples = yield mysql_client.fetchall(query_article_info_sql)
        frame = pandas.DataFrame(data=list(data_tuples), columns=fields)
        frame.set_index(COL_ARTICLE_ID, inplace=True)
        # frame = self._preprocess_frame(frame)
        frame = StarDataPreProcess(frame).data
        raise gen.Return(frame)

    @tornado.gen.coroutine
    def _update_recommend_pools(self, mysql_client, date_time, fields):
        """
        生成指定天数内的推荐数据
        :param mysql_client: mysql client
        :param date_time: 表示天数，int
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
        select t.article_id, t.yh_type, t.hot, t.price, t.cate_1, t.mall, t.mall_id, group_concat(t.tag) tags, t.pubdate
        from(
            select a.article_id, a.yh_type, a.hot, a.price, a.cate_1, a.mall, a.mall_id, b.name as tag, a.pubdate
            from (
                select a.id as article_id,
                a.yh_type, a.sum_collect_comment as hot, a.digital_price as price, a.level_1 as cate_1, a.mall, a.mall_id, a.pubdate, b.tag_id
                from {youhui_table}
                as a
                left join {youhui_tag_table} as b
                on a.id = b.article_id
                where a.pubdate > date_add(now(), interval -{date_time} day)
                and yh_status=1
                and channel in (1, 2, 5)
                and a.sum_collect_comment > 0
            ) as a
            left join {smzdm_tag} as b
            on a.tag_id = b.id
        ) as t
        group by t.article_id
        """.format(youhui_table=YOUHUI_PRIMARY, youhui_tag_table=YOUHUI_TAG,
                   date_time=date_time, smzdm_tag=SMZDM_TAG)

        data_tuples = yield mysql_client.fetchall(query_rec_pools_info_sql)
        frame = pandas.DataFrame(data=list(data_tuples), columns=fields)
        frame.set_index(COL_ARTICLE_ID, inplace=True)
        # frame = self._preprocess_frame(frame)
        frame = StarDataPreProcess(frame).data
        RECOMMEND_POOLS = frame
        RECOMMEND_POOLS_UPDATE_TIME = current_time
        gen_log.info("[update recommend pools]")

        raise gen.Return(frame)
