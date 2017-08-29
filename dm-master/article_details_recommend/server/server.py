# coding=utf-8
import hashlib
import json
import re
import urllib
from random import choice

from tornado import gen
from tornado.web import asynchronous, RequestHandler
from tornado.log import access_log

from base.consts import *
from tasks import *


class AsyncReqDataHandler(RequestHandler):

    def __init__(self, application, request, **kwargs):
        super(AsyncReqDataHandler, self).__init__(application, request, **kwargs)
        self.device_id = None
        self.article_id = None
        self.channel_id = None
        self.trace_id = None
        self.version = None

        self.simi_yh_article_key = None
        self.simi_yh_article_sql = None
        self.simi_yh_article_key_time = None
        self.simi_yh_history_article_key_time = None

        self.asso_rule_yh_article_key = None
        self.asso_rule_yh_article_sql = None
        self.asso_rule_yh_article_key_time = None

        self.asso_rule_yc_article_key = None
        self.asso_rule_yc_article_sql = None
        self.asso_rule_yc_article_key_time = None

        self.from_now_to_dawn_time = None

        self.trans_trace_id = None

        self.query_spec_and_newest_article_sql = None
        self.trace_id_map_file = None

        self.fileds_list = ["article_id", "article_channel_id", "rs_id2", "rs_id3", "rs_id4", "rs_id1", "rs_id5"]
        self.max_num = 3
        self.sep = "_"

    def cut_bucket(self, sts, bucket_num):
        hash = hashlib.md5()
        hash.update(sts)
        md5_sts = hash.hexdigest()
        # md5_sta_num = long("".join([str(ord(s)) for s in md5_sts]))
        md5_sta_num = int(md5_sts, 16)
        return md5_sta_num % bucket_num

    def prepare(self):
        self.device_id = str(urllib.unquote(self.get_argument("device_id", "", False)).replace(" ", "+"))  # 空格替换为+
        self.article_id = str(self.get_argument("article_id", "-1"))
        self.channel_id = str(self.get_argument("channel_id", "-1"))
        # self.user_id = self.get_argument("user_id", "")
        self.trace_id = str(self.get_argument("trace_id", ""))
        self.version = str(self.get_argument("version", ""))

        if AB_TEST_BASE == BASE_TRACE_ID:
            self.bucket = self.cut_bucket(self.trace_id, BUCKET_NUM)
        elif AB_TEST_BASE == BASE_DEVICE_ID:
            self.bucket = self.cut_bucket(self.device_id, BUCKET_NUM)
        access_log.info("bucket: %s" % self.bucket)
        AB_TEST_RANGE_NAME = ""
        if self.bucket in AB_TEST_RANGE_A:
            AB_TEST_RANGE_NAME = AB_TEST_RANGE_A_NAME
        elif self.bucket in AB_TEST_RANGE_B:
            AB_TEST_RANGE_NAME = AB_TEST_RANGE_B_NAME
        elif self.bucket in AB_TEST_RANGE_C:
            AB_TEST_RANGE_NAME = AB_TEST_RANGE_C_NAME

        self.AB_TEST_RANGE_NAME = AB_TEST_RANGE_NAME

        AB_TEST_RANGE_NAME_2 = ""
        if self.bucket in AB_TEST_RANGE_A_2:
            AB_TEST_RANGE_NAME_2 = AB_TEST_RANGE_A_NAME_2
        elif self.bucket in AB_TEST_RANGE_B_2:
            AB_TEST_RANGE_NAME_2 = AB_TEST_RANGE_B_NAME_2
        elif self.bucket in AB_TEST_RANGE_C_2:
            AB_TEST_RANGE_NAME_2 = AB_TEST_RANGE_C_NAME_2

        self.AB_TEST_RANGE_NAME_2 = AB_TEST_RANGE_NAME_2

        self.simi_yh_article_key = self.application.backend.get_youhui_similary_article_key(
            self.article_id, AB_TEST_RANGE_NAME, self.channel_id)
        self.simi_yh_article_sql = self.application.backend.get_youhui_similary_combine_article_sql(
            self.article_id, self.channel_id, AB_TEST_RANGE_NAME)
        self.simi_yh_article_key_time = self.application.backend.get_youhui_similary_article_key_time()
        self.simi_yh_history_article_key_time = self.application.backend.get_youhui_similary_history_article_key_time()

        self.asso_rule_yh_article_key = self.application.backend.get_association_rule_article_key(
            self.article_id, self.channel_id)
        self.asso_rule_yh_article_sql = self.application.backend.get_association_rule_article_sql(
            self.article_id, flag="youhui")
        self.asso_rule_yh_article_key_time = self.application.backend.get_association_rule_article_key_time()

        self.asso_rule_yc_article_key = self.application.backend.get_association_rule_article_key(
            self.article_id, self.channel_id, flag="yuanchuang")
        self.asso_rule_yc_article_sql = self.application.backend.get_association_rule_article_sql(
            self.article_id, flag="yuanchuang")
        self.asso_rule_yc_article_key_time = self.application.backend.get_association_rule_article_key_time(
            flag="yuanchuang")

        self.from_now_to_dawn_time = self.application.backend.get_from_now_to_dawn_time()

        self.query_spec_and_newest_article_sql = self.application.backend.get_query_spec_and_newest_article_sql(
            self.article_id, self.channel_id)

        self.trace_id_map_file = self.application.backend.get_trace_id_map_file()

    @asynchronous
    @gen.coroutine
    def get(self):
        Config = self.application.config
        trans_trace_args = [Config, self.from_now_to_dawn_time, self.trace_id, self.trace_id_map_file]

        if self.channel_id in youhui:  # 优惠文章，推荐优惠文章时优先使用优惠相似度模型的结果，推荐原创文章时只使用关联规则模型的结果
            simi_yh_args = [Config, self.simi_yh_article_key, self.simi_yh_article_sql,
                            self.simi_yh_article_key_time]

            asso_yh_args = [Config, self.asso_rule_yh_article_key, self.asso_rule_yh_article_sql,
                            self.asso_rule_yh_article_key_time]
            asso_yc_args = [Config, self.asso_rule_yc_article_key, self.asso_rule_yc_article_sql,
                            self.asso_rule_yc_article_key_time]
            # print "simi_yh_article_key: %s" % self.simi_yh_article_key
            # print "simi_yh_article_sql: %s" % self.simi_yh_article_sql
            t0 = time.clock()
            trans_trace_result, asso_yc_result = yield [gen.Task(transform_trace_id, *trans_trace_args),
                                                    gen.Task(query_and_insert_data, *asso_yc_args)]

            # simi_yh_result = None
            # 如果是运行的代码版本是最新版本，则支持接收相似度模型数据
            simi_yh_result = yield gen.Task(query_and_insert_data, *simi_yh_args)

            asso_yh_result = None
            if self.AB_TEST_RANGE_NAME_2 == AB_TEST_RANGE_A_NAME_2:
                STAR_DATA_RES_KEY = "star_data_res:%s"
                key = STAR_DATA_RES_KEY % self.article_id
                redis_connect = create_redis_connect(Config)
                star_data = redis_connect.get(key)
                if star_data:
                    star_data = json.loads(star_data)
                    self.row_name2 = star_data.get("module_name")
                    # 按照热度降序排
                    rec_data = sorted(star_data.get("rec").items(), key=lambda item: item[1]["hot"], reverse=True)
                    star_yh_result = ["_".join((article_id, "1", str(info.get("hot")), "")) for article_id, info in rec_data]
                    asso_yh_result = star_yh_result
                    asso_rs_id4 = "star.data"
                else:
                    asso_yh_result = yield gen.Task(query_and_insert_data, *asso_yh_args)
                    self.row_name2 = self.application.backend.youhui_asso_yh_name
                    asso_rs_id4 = "asso.rule"
            else:
                asso_yh_result = yield gen.Task(query_and_insert_data, *asso_yh_args)
                self.row_name2 = self.application.backend.youhui_asso_yh_name
                asso_rs_id4 = "asso.rule"

            simi_rs_id4 = ".".join(("base.simi", self.AB_TEST_RANGE_NAME))  # algorithm_and_version：基于相似度模型
            asso_rs_id4_yc = "asso.rule"  # algorithm_and_version：关联规则模型
            # asso_rs_id4 = self.AB_TEST_RANGE_NAME_2

            t1 = time.clock()
            access_log.debug("youhui query res time: %.2f s" % (t1 - t0))

            # access_log.info("simi_yh_result: %s" % simi_yh_result)
            # redis和mysql中不存在基于相似度模型推荐的结果，则需要在线计算出来，这种情况多适用于历史文章
            # TODO(wv): 历史文章或者数据库中不存在相似度模型计算的推荐结果的文章
            t0 = time.clock()
            #if not simi_yh_result and HISTORY_ARTICLE_FLAG:
            #    cal_history_article_args = [Config, self.query_spec_and_newest_article_sql, self.article_id,
            #                                self.simi_yh_article_key, self.simi_yh_history_article_key_time]
            #    simi_yh_result = yield gen.Task(calculate_history_article_res, *cal_history_article_args)
            #    t1 = time.clock()
            #    access_log.debug("cal_history_article_args: %.2f s" % (t1 - t0))
            #    # access_log.debug("simi_yh_result: %s" % simi_yh_result)

            t1 = time.clock()
            access_log.debug("youhui query res time: %.2f s" % (t1 - t0))

            # 如果存在基于相似度模型的推荐结果，则对它进行如下处理
            # [u'6976590_0.6_0.0_0.0_0.0_0.6_0.0_0.0_0.0_0.0_0.0',
            # u'6976046_0.6_0.0_0.0_0.0_0.6_0.0_0.0_0.0_0.0_0.0',
            # u'6974570_0.6_0.0_0.0_0.0_0.6_0.0_0.0_0.0_0.0_0.0']
            if simi_yh_result:
                article_channel_id = "1"
                rs_id2, rs_id3 = "", ""
                simi_yh_result = map(
                    lambda s: self.sep.join((s.split(self.sep)[0], article_channel_id, rs_id2, rs_id3)),
                    simi_yh_result)

            self.trans_trace_id = trans_trace_result

            rs_id1_and_rs_id5 = self.sep.join((self.trace_id, self.trans_trace_id))
            simi_yh_result = map(lambda s: s + self.sep + simi_rs_id4 + self.sep + rs_id1_and_rs_id5,
                                 simi_yh_result) if simi_yh_result else []
            asso_yh_result = map(lambda s: s + self.sep + asso_rs_id4 + self.sep + rs_id1_and_rs_id5,
                                 asso_yh_result) if asso_yh_result else []
            asso_yc_result = map(lambda s: s + self.sep + asso_rs_id4_yc + self.sep + rs_id1_and_rs_id5,
                                 asso_yc_result) if asso_yc_result else []

            module_name = self.application.backend.youhui_module_name

            row1, row2, row3 = {}, {}, {}
            if self.version >= "8.0":
                row1 = {"row_data": simi_yh_result,
                        "row_name": self.application.backend.youhui_simi_yh_name} if simi_yh_result else row1
                row2 = {"row_data": asso_yh_result,
                        "row_name": self.row_name2} if asso_yh_result else row2
                row3 = {"row_data": asso_yc_result,
                        "row_name": self.application.backend.youhui_asso_yc_name} if asso_yc_result else row3
            else:
                row1 = {"row_data": simi_yh_result,
                        "row_name": self.application.backend.youhui_simi_yh_name} if simi_yh_result else row1

                if asso_yh_result:
                    row2 = {"row_data": asso_yh_result,
                            "row_name": self.row_name2}
                elif asso_yc_result:
                    row2 = {"row_data": asso_yc_result,
                            "row_name": self.application.backend.youhui_asso_yc_name}

            data = {"module_name": module_name, "row1": row1, "row2": row2, "row3": row3}

        else:  # 原创文章，查询关联规则模型的推荐结果

            asso_yh_args = [Config, self.asso_rule_yh_article_key, self.asso_rule_yh_article_sql,
                            self.asso_rule_yh_article_key_time]
            asso_yc_args = [Config, self.asso_rule_yc_article_key, self.asso_rule_yc_article_sql,
                            self.asso_rule_yc_article_key_time]

            t0 = time.clock()
            trans_trace_result, \
            asso_yc_result, asso_yh_result = yield [gen.Task(transform_trace_id, *trans_trace_args),
                                                    gen.Task(query_and_insert_data, *asso_yc_args),
                                                    gen.Task(query_and_insert_data, *asso_yh_args)]

            # trans_trace_result, asso_yh_result = yield [gen.Task(transform_trace_id, *trans_trace_args),
            #                                         gen.Task(query_and_insert_data, *asso_yh_args)]
            # asso_yc_result = {}

            t1 = time.clock()
            access_log.debug("yuanchuang query res time: %.2f s" % (t1 - t0))

            asso_rs_id4 = "asso.rule"  # algorithm_and_version：关联规则模型

            self.trans_trace_id = trans_trace_result

            rs_id1_and_rs_id5 = self.sep.join((self.trace_id, self.trans_trace_id))
            asso_yh_result = map(lambda s: s + self.sep + asso_rs_id4 + self.sep + rs_id1_and_rs_id5,
                                 asso_yh_result) if asso_yh_result else []
            asso_yc_result = map(lambda s: s + self.sep + asso_rs_id4 + self.sep + rs_id1_and_rs_id5,
                                 asso_yc_result) if asso_yc_result else []

            module_name = self.application.backend.yuanchuang_module_name

            row1, row2, row3 = {}, {}, {}
            if self.version >= "8.0":
                row2 = {"row_data": asso_yc_result, "row_name": self.application.backend.yuanchuang_asso_yc_name} if asso_yc_result else {}
                row3 = {"row_data": asso_yh_result, "row_name": self.application.backend.yuanchuang_asso_yh_name} if asso_yh_result else {}
            else:
                # row1 = {"row_data": asso_yc_result, "row_name": self.application.backend.yuanchuang_asso_yc_name} if asso_yc_result else {}
                row2 = {"row_data": asso_yh_result, "row_name": self.application.backend.yuanchuang_asso_yh_name} if asso_yh_result else {}
            data = {"module_name": module_name, "row1": row1, "row2": row2, "row3": row3}

        self.op_json_data(data=data)

    def op_json_data(self,data):
        # print "data: %s" % data
        # data:{'module_name': u'\u731c\u4f60\u559c\u6b22', 'row2': {
        #     'row_data': [u'475627_11_1.0__asso.rule_bbb_3', u'391125_11_1.0__asso.rule_bbb_3',
        #                  u'474389_11_1.0__asso.rule_bbb_3', u'474389_11_1.0__asso.rule_bbb_3'],
        #     'row_name': u'\u70ed\u95e8\u539f\u521b'}, 'row3': {}, 'row1': {
        #     'row_data': [u'6035336_1___base.simi_bbb_3', u'6035335_1___base.simi_bbb_3',
        #                  u'6037501_1___base.simi_bbb_3'], 'row_name': u'\u76f8\u5173\u4f18\u60e0'}}
        data = self.distinct_data(data)

        out_str = json.dumps(self.correct_json(data), ensure_ascii=False)
        self.set_header("Content-Type", "application/json; charset=utf-8")
        self.write(out_str)

    # 数据去重
    def distinct_data(self, res_data):
        # print "version: %s" % self.version
        if self.version >= "8.0":
            # print "===8.0==="
            for key, value in res_data.iteritems():
                if key == "module_name":
                    continue
                # min_num = 4
                max_num = 10

                row_str_list = res_data.get(key).get("row_data", [])
                row_article_ids, row_data_distinct_list = [], []

                for data_str in row_str_list:
                    data = data_str.split(self.sep)
                    if data[0] not in row_article_ids:
                        data_dict = dict(zip(self.fileds_list, data))
                        row_data_distinct_list.append(data_dict)
                        row_article_ids.append(data[0])
                    if len(row_data_distinct_list) == max_num:
                        break

                if row_data_distinct_list:
                    res_data[key]["row_data"] = row_data_distinct_list
                else:
                    res_data[key] = {}
                # if (len(row_data_distinct_list) / min_num) > 0:
                #     remain_num = len(row_data_distinct_list) % 2
                #     res_data[key]["row_data"] = row_data_distinct_list if \
                #         remain_num == 0 else row_data_distinct_list[0: len(row_data_distinct_list) - 1]
                # else:
                #     res_data[key] = {}
        else:
            # print "<<<8.0==="
            for key, value in res_data.iteritems():
                if key == "module_name":
                    continue

                max_num = 3

                row_str_list = res_data.get(key).get("row_data", [])
                row_article_ids, row_data_distinct_list = [], []
                # print "row_str_list: %s" % row_str_list
                # print "--" * 20
                for data_str in row_str_list:
                    data = data_str.split(self.sep)
                    if data[0] not in row_article_ids:
                        data_dict = dict(zip(self.fileds_list, data))
                        row_data_distinct_list.append(data_dict)
                        row_article_ids.append(data[0])
                    if len(row_data_distinct_list) == max_num:
                        break

                if row_data_distinct_list:
                    if len(row_data_distinct_list) == max_num:
                        res_data[key]["row_data"] = row_data_distinct_list
                    else:
                        res_data[key] = {}

        self.op_statistic(res_data)
        return res_data

    def op_statistic(self, res_data):
        """
        输出之后用于统计的日志
        :param res_data:
        :return:
        """
        pattern = re.compile(r"\W+")
        device_type = "IOS" if pattern.search(self.device_id) else "Android"

        for key, value in res_data.iteritems():
            if key == "module_name":
                continue
            try:
                row_name = res_data.get(key).get("row_name")
                row_data = res_data.get(key).get("row_data")

                row_article_type = "优惠" if row_data[0]["article_channel_id"] == "1" else "原创"
                access_log.info("<statistic>%s,%s,%s,%s,%s,%s" % (
                    self.device_id, device_type, self.channel_id, row_name, row_article_type, len(row_data)
                ))
            except KeyError:
                pass
            except TypeError:
                pass

    def error_json(self, error_msg):
        return {"error_code": "1", "data": "0", "error_msg": error_msg}

    def correct_json(self, data):
        return {"error_code": "0", "error_msg": "data", "data": data}

    def write_error(self, status_code, **kwargs):
        self.set_status(status_code)


if __name__ == "__main__":

    # Business(Config)
    pass
