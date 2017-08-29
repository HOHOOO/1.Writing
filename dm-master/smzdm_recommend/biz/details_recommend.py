# coding=utf-8
from collections import defaultdict, Counter

import datetime
import pandas
import re
import tornado
from tornado import gen
from tornado.log import gen_log

from biz_config.details_recommend import *
from biz_config.details_recommend_ab import ASSO_RULE, BASE_SIMI, STAR_DATA, BASE_SIMI_AND_WIKI_YC


class DetailsRecommend(object):
    def __init__(self, mysql_client, redis_client, device_id, article_id, channel_id, trace_id, version, ab_module,
                 row1_min_num, row1_max_num, row2_min_num, row2_max_num, row3_min_num, row3_max_num):
        self.mysql_client = mysql_client
        self.device_id = device_id
        self.redis_client = redis_client
        self.article_id = article_id
        self.channel_id = channel_id
        self.trace_id = trace_id
        self.version = version
        self.ab_module = ab_module

        self.row1_min_num = row1_min_num
        self.row1_max_num = row1_max_num
        self.row2_min_num = row2_min_num
        self.row2_max_num = row2_max_num
        self.row3_min_num = row3_min_num
        self.row3_max_num = row3_max_num

        self.data_frame = defaultdict(lambda: {})
        self.data_frame["module_name"] = REC_MODULE_NAME

        self.fields = ["article_id", "article_channel_id", "rs_id1", "rs_id2", "rs_id3", "rs_id4", "rs_id5"]

        self.generate_short_trace_id()

        self.rs_id1 = self.trace_id
        self.rs_id2 = ""
        self.rs_id3 = ""
        # self.rs_id4 = "" # rs_id4 用户标志所用算法类型
        self.rs_id5 = self.short_trace_id

    def generate_short_trace_id(self):
        """
        将长的trace_id转为短的trace_id
        :return:
        """
        current_datetime = datetime.datetime.now()
        cate_datetime = current_datetime + datetime.timedelta(days=1)
        cate_datetime = cate_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
        delta = cate_datetime - current_datetime  # 当前时间至明天凌晨的时间差
        expire_time = int(delta.total_seconds())

        pipe = self.redis_client.pipeline()
        pipe.incr(REDIS_TRACE_ID_COUNT_KEY)
        pipe.expire(REDIS_TRACE_ID_COUNT_KEY, expire_time)

        short_trace_id = pipe.execute()[0]
        self.short_trace_id = short_trace_id

    @tornado.gen.coroutine
    def get_haowu_rec_data(self):

        rec_data = self.data_frame

        rank_list_sql = """
        select id
        from {table}
        where publish_date > date_add(now(), interval -7 day) and id <> {article_id}
        and state = 2 and (review_num + collect_num) > 0
        order by (review_num + collect_num) desc
        limit 10
        """.format(table=HAO_WU_TABLE, article_id=self.article_id)

        mysql_data = yield self.mysql_client.fetchall(rank_list_sql)

        row_data = []
        for item in mysql_data:
            article_id = item[0]
            values = [article_id, HAO_WU_CHANNEL_ID, "", "", "", "", ""]
            row_data.append(dict(zip(self.fields, values)))

        rec_data["row1"]["row_data"] = row_data
        rec_data["row1"]["row_name"] = REC_HAO_WU_MODULE_NAME
        raise gen.Return(rec_data)

    @tornado.gen.coroutine
    def get_youhui_rec_data(self):
        rec_data = self.data_frame

        for row, algor_dict in self.ab_module.algors.items():
            gen_log.debug("row: %s, algor: %s" % (row, algor_dict))
            rec_data[row] = {}

            # 该模块没有算法，则跳过
            if not algor_dict:
                continue

            for i in ("first", "second"):
                algor = algor_dict.get(i)
                # 不存在则跳过
                if not algor:
                    continue
                algor_name = algor.get("name")
                algor_base = algor.get("base")

                if row == "row1":
                    rec_type = "youhui"
                    min_num = self.row1_min_num
                    max_num = self.row1_max_num
                    row_name = REC_YOU_HUI_ROW1_NAME

                elif row == "row2":
                    rec_type = "youhui"
                    min_num = self.row2_min_num
                    max_num = self.row2_max_num
                    row_name = REC_YOU_HUI_ROW2_NAME
                else:
                    rec_type = "yuanchuang"
                    min_num = self.row3_min_num
                    max_num = self.row3_max_num
                    row_name = REC_YOU_HUI_ROW3_NAME

                res = yield algor_name2res(algor_name, self.mysql_client, self.redis_client, self.article_id,
                                           rec_type, min_num,
                                           max_num).algor_res()

                data = res.get("data")
                if data:
                    row_data = [
                        dict(zip(self.fields,
                                 [d[0], d[1], self.rs_id1, self.rs_id2, self.rs_id3, algor_base,
                                  self.rs_id5]))
                        for d in data]
                    # 结果中带有模块名称则使用结果中的，否则使用配置文件中的名称
                    row_name = res.get("row_name") if res.get("row_name") else row_name

                    rec_data[row] = {"row_name": row_name, "row_data": row_data}

                if rec_data[row]:
                    # 如果该模块已经有数据了，则跳出
                    break

        raise gen.Return(rec_data)

    @tornado.gen.coroutine
    def get_yuanchuang_rec_data(self):
        rec_data = self.data_frame

        for row, algor_dict in self.ab_module.algors.items():
            gen_log.debug("row: %s, algor: %s" % (row, algor_dict))
            rec_data[row] = {}

            # 该模块没有算法，则跳过
            if not algor_dict:
                continue

            for i in ("first", "second"):
                algor = algor_dict.get(i)
                # 不存在则跳过
                if not algor:
                    continue
                algor_name = algor.get("name")
                algor_base = algor.get("base")

                if row == "row2":
                    rec_type = "yuanchuang"
                    min_num = self.row2_min_num
                    max_num = self.row2_max_num
                    row_name = REC_YUAN_CHUANG_ROW2_NAME
                else:
                    rec_type = "youhui"
                    min_num = self.row3_min_num
                    max_num = self.row3_max_num
                    row_name = REC_YUAN_CHUANG_ROW3_NAME

                res = yield algor_name2res(algor_name, self.mysql_client, self.redis_client, self.article_id,
                                           rec_type, min_num,
                                           max_num).algor_res()

                data = res.get("data")
                if data:
                    row_data = [
                        dict(zip(self.fields,
                                 [d[0], d[1], self.rs_id1, self.rs_id2, self.rs_id3, algor_base,
                                  self.rs_id5]))
                        for d in data]
                    # 结果中带有模块名称则使用结果中的，否则使用配置文件中的名称
                    row_name = res.get("row_name") if res.get("row_name") else row_name

                    rec_data[row] = {"row_name": row_name, "row_data": row_data}

                if rec_data[row]:
                    # 如果该模块已经有数据了，则跳出
                    break

        raise gen.Return(rec_data)

    def op_statistic(self, data):
        """
        输出之后用于统计的日志
        :param data:
        :return:
        """

        device_id = self.device_id.encode("utf-8")
        channel_id = self.channel_id

        pattern = re.compile(r"\W+")
        device_type = "IOS" if pattern.search(device_id) else "Android"
        for key, value in data.iteritems():
            if key == "module_name":
                continue
            try:
                row_name = data.get(key).get("row_name")
                row_name = row_name.encode("utf-8") if isinstance(row_name, unicode) else row_name
                row_data = data.get(key).get("row_data")
                if row_data:
                    # 获取每个模块下不同类型文章的个数
                    counter = Counter([d.get("article_channel_id") for d in row_data])
                    for article_ch_id, num in counter.iteritems():
                        row_article_type = "优惠" if article_ch_id == "1" else "原创"
                        # gen_log.info("row_data: %s" % row_data)
                        gen_log.info("<statistic>%s,%s,%s,%s,%s,%s" % (
                            device_id, device_type, channel_id, row_name, row_article_type, num
                        ))
            except Exception, e:
                import traceback
                gen_log.error(traceback.format_exc())

    @property
    @tornado.gen.coroutine
    def rec_data(self):
        if self.channel_id == HAO_WU_CHANNEL_ID:
            data = yield self.get_haowu_rec_data()
        elif self.channel_id in YOU_HUI_CHANNEL_ID_LIST:
            data = yield self.get_youhui_rec_data()
        else:
            data = yield self.get_yuanchuang_rec_data()

        # 输出统计信息
        self.op_statistic(data)

        raise gen.Return(dict(data))


class algor_name2res(object):
    """
    根据算法名称获取该算法的推荐结果
    """

    def __init__(self, algor_name, mysql_client, redis_client, article_id, rec_type, min_num, max_num):
        self.algor_name = algor_name
        self.mysql_client = mysql_client
        self.redis_client = redis_client
        self.article_id = article_id
        self.rec_type = rec_type
        self.min_num = min_num
        self.max_num = max_num

    @tornado.gen.coroutine
    def algor_res(self):
        data = {}
        # {'data': [[u'7614547', '1'], [u'7612812', '1'], [u'7616353', '1'], [u'7613882', '1'],
        # [u'7612355', '1'], [u'7613288', '1'], [u'7614795', '1'], [u'7613283', '1']],
        # 'row_name': u'\u4eac\u4e1c\u51d1\u5355\u54c1'}
        if self.algor_name == ASSO_RULE:
            data = yield self.get_asso_rule_rec_data()
        elif self.algor_name == BASE_SIMI:
            data = yield self.get_base_simi_rec_data()
        elif self.algor_name == STAR_DATA:
            data = yield self.get_star_data_rec_data()
        elif self.algor_name == BASE_SIMI_AND_WIKI_YC:
            data = yield self.get_base_simi_and_wiki_yc_rec_data()
        gen_log.debug("data: %s" % data)
        raise gen.Return(data)

    @tornado.gen.coroutine
    def get_base_simi_and_wiki_yc_rec_data(self):
        # TODO(存在一个问题，由于在get_base_simi_rec_data方法中对条数做了限制，这儿再做限制可能不准确)
        base_simi_data = yield self.get_base_simi_rec_data()
        data = base_simi_data

        if not data:
            raise gen.Return(data)
        if len(data.get("data")) < YOU_HUI_ROW1_INSERT_YC_NUM:
            raise gen.Return(data)

        query_sql = """
            select a.article_id
            from {product_article} a
            join {yh_table_name} b
            on a.pro_id = b.pro_id
            and b.id={article_id}
            and a.article_type='yuanchuang'
            and a.is_deleted=0
            join {yc_table_name} c
            on a.article_id=c.id
            order by c.sum_collect_comment desc
            limit 20;
        """.format(yh_table_name=YOU_HUI_TABLE, article_id=self.article_id, product_article=PRODUCT_ARTICLE,
                   yc_table_name=YUAN_CHUANG_TABLE)
        mysql_result = yield self.mysql_client.fetchall(query_sql)
        if mysql_result:
            article_channel_id = "11"
            insert_data = [[str(d[0]), article_channel_id] for d in mysql_result]
            data_list = list(data["data"])

            if len(data.get("data")) == YOU_HUI_ROW1_INSERT_YC_NUM and len(insert_data) > 0:
                data_list.insert(REC_YOU_HUI_ROW1_INSERT_POSTION1 - 1, insert_data[0])

            elif len(data.get("data")) > YOU_HUI_ROW1_INSERT_YC_NUM and len(insert_data) > 1:
                data_list.insert(REC_YOU_HUI_ROW1_INSERT_POSTION1 - 1, insert_data[0])
                data_list.insert(REC_YOU_HUI_ROW1_INSERT_POSTION2 - 1, insert_data[1])

            elif len(base_simi_data.get("data")) > YOU_HUI_ROW1_INSERT_YC_NUM and len(insert_data) == 1 :
                data_list.insert(REC_YOU_HUI_ROW1_INSERT_POSTION1 - 1, insert_data[0])
            data["data"] = data_list[: self.max_num]
            data["row_name"] = REC_YOU_HUI_ROW1_NAME2

        raise gen.Return(data)

    @tornado.gen.coroutine
    def get_base_simi_rec_data(self):
        key = BASE_SIMI_RES_KEY % self.article_id

        redis_data = self.redis_client.get(key)
        data = {}
        if redis_data:
            article_channel_id = "1"
            redis_dict = pandas.json.loads(redis_data)
            rec_data = [[str(d.get("article_id")), article_channel_id] for d in redis_dict.get("rec")]
            rec_data = rec_data[: self.max_num] if len(rec_data) >= self.min_num else []

            data["data"] = rec_data
            data["row_name"] = ""

        # redis_data = self.redis_client.lrange(key, 0, -1)
        # data = {}
        # if redis_data:
        #     article_channel_id = "1"
        #     rec_data = [[d.split("_")[0], article_channel_id] for d in redis_data]
        #     rec_data = rec_data[: self.max_num] if len(rec_data) >= self.min_num else []
        #
        #     data["data"] = rec_data
        #     data["row_name"] = ""

        raise gen.Return(data)

    @tornado.gen.coroutine
    def get_star_data_rec_data(self):
        key = STAR_DATA_RES_KEY % self.article_id

        redis_data = self.redis_client.get(key)

        data = {}
        if redis_data:
            star_data = pandas.json.loads(redis_data)
            module_name = star_data.get("module_name")

            article_channel_id = "1"
            rec_data = sorted(star_data.get("rec").items(), key=lambda item: item[1]["hot"], reverse=True)
            rec_data = [[k, article_channel_id] for k, v in rec_data]
            rec_data = rec_data[: self.max_num] if len(rec_data) >= self.min_num else []
            data["data"] = rec_data
            data["row_name"] = module_name

        raise gen.Return(data)

    @tornado.gen.coroutine
    def get_asso_rule_rec_data(self):
        key, query_sql, expire_time, article_channel_id = None, None, None, None
        if self.rec_type == "youhui":
            key = ASSO_RULE_YH_RES_KEY % self.article_id
            expire_time = ASSO_RULE_YH_RES_KEY_EXPIRE
            article_channel_id = "1"
            query_sql = """
                    select new_article_id
                    from {table_name}
                    where old_article_id={article_id} and new_channel_id=1
                    and score>0.005 and new_article_time>date_add(now(), interval -2 day)
                    order by score desc limit 50
            """.format(table_name=ASSO_RULE_TABLE, article_id=self.article_id)
        elif self.rec_type == "yuanchuang":
            key = ASSO_RULE_YC_RES_KEY % self.article_id
            expire_time = ASSO_RULE_YC_RES_KEY_EXPIRE
            article_channel_id = "11"
            query_sql = """
                select new_article_id
                from {table_name}
                where old_article_id={article_id} and new_channel_id=11
                and score>0.005
                order by score desc limit 50
            """.format(table_name=ASSO_RULE_TABLE, article_id=self.article_id)

        redis_data = self.redis_client.lrange(key, 0, -1)
        data = {}

        if redis_data:
            data["data"] = map(lambda x: x.split(","), redis_data)
            data["row_name"] = ""
            raise gen.Return(data)

        mysql_result = yield self.mysql_client.fetchall(query_sql)

        if mysql_result:
            rec_data = []
            rec_data_insert_redis = []
            tmp_list = []
            for d in mysql_result:
                if len(rec_data) == self.max_num:
                    break

                article_id = d[0]
                # 数据去重
                if article_id not in tmp_list:
                    rec_data.append([article_id, article_channel_id])
                    rec_data_insert_redis.append(",".join([str(article_id), article_channel_id]))
                    tmp_list.append(article_id)

            if len(rec_data) >= self.min_num:
                # 结果存入redis，设置过期时间
                self.redis_client.rpush(key, *rec_data_insert_redis)
                self.redis_client.expire(key, expire_time)

                data["data"] = rec_data
                data["row_name"] = ""

        raise gen.Return(data)
