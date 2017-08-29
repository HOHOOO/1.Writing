# coding=utf-8
import json
import re
import urllib
import urllib2
from random import choice
import time
from tornado import gen
from tornado.web import asynchronous, RequestHandler
from tornado.log import access_log
import tornado.httpclient
from tornado.httpclient import HTTPRequest

# from base.consts import *
from tasks import *


class AsyncReqDTDataHandler(RequestHandler):

    def __init__(self, application, request, **kwargs):
        super(AsyncReqDTDataHandler, self).__init__(application, request, **kwargs)
        self.user_id = None
        # self.offset = None
        self.limit = None
        self.page = None
        self.sep = "_"



    def prepare(self):
        self.user_id = str(self.get_argument("user_id", 0))
        self.page = str(self.get_argument("page", 0))
        self.limit = str(self.get_argument("limit", 0))

        #模块名
        self.recommend_name = self.application.backend.get_recommend_name()
        self.backstage_name = self.application.backend.get_backstage_name()
        self.recommend_backstage_name = self.application.backend.get_recommend_backstage_name()
        # json key
        self.tuijian_key = self.application.backend.get_tuijian_key()
        self.tuijian_id_key = self.application.backend.get_tuijian_id_key()
        self.tuijian_type_key = self.application.backend.get_tuijian_type_key()
        self.data_type_key = self.application.backend.get_data_type_key()
        #关注动态key
        self.user_attention_page_user_key = self.application.backend.get_user_attention_page_user_key()
        self.user_attention_page_not_user_key = self.application.backend.get_user_attention_page_not_user_key()
        self.tuijian_zhiding_key = "zhiding"
        self.tuijian_not_zhiding_key = "zhiding_not"

        #biao
        self.dingyue_apphot_table = self.application.backend.get_dingyue_apphot_table()
        self.user_rules_table = self.application.backend.get_user_rules_table()
        self.rules_table = self.application.backend.get_rules_table()
        self.follow_relate_table = self.application.backend.get_follow_relate_table()
        self.dingyue_baike_table = self.application.backend.get_dingyue_baikei_table()
        self.gexinhua_table = self.application.backend.get_gexinhua_table()

        self.dingyue_apphot_history_key_time = self.application.backend.get_dingyue_apphot_history_key_time()

    @asynchronous
    @gen.coroutine
    def get(self):
        Config = self.application.config
        """
        1表示推荐
        0表示后台
        """
        guanzhu_rules_sql = "select distinct user_id,b.keyword from {table1_name} a left join {table2_name} b on a.rule_id = b.id where user_id = {user_id} and type !='title' ".format(
            user_id=self.user_id , table1_name=self.user_rules_table , table2_name = self.rules_table)
        guanzhu_baike_sql = "select user_id,wiki_id from {table_name} where user_id = {user_id} union all select user_id,wiki_hash_id from {table_name} where user_id = {user_id}".format(
            user_id=self.user_id , table_name=self.dingyue_baike_table)
        rules_zhiding_sql = "select id,tuijian ,tuijian_id ,tuijian_type,sort_num  from {table_name} where qianzhi = '' and status = 1 ORDER BY sort_num ASC,id DESC".format(
            table_name = self.dingyue_apphot_table)
        dingyue_user_sql ="select user_id,follow_user_id from {table_name} where user_id = {user_id} ".format(
            user_id=self.user_id , table_name=self.follow_relate_table)
        rules_not_zhiding_sql = "select id,tuijian ,tuijian_id ,tuijian_type,sort_num  from {table_name} where qianzhi <> '' and status = 1 ORDER BY sort_num ASC,id DESC".format(
            table_name=self.dingyue_apphot_table)

        guanzhu_sql = "select tuijian ,tuijian_id ,tuijian_type,tuijian_reason  from {table_name} ".format( table_name=self.gexinhua_table)

        t0 = time.clock()
        tuijian_key = ["id", self.tuijian_key, self.tuijian_id_key, self.tuijian_type_key, "sort_num",
                       self.data_type_key]
        not_china_key = ["user_id", "tuijian_id"]
        china_key = ["user_id", "tuijian"]
        key = [self.tuijian_key, self.tuijian_id_key, self.tuijian_type_key, self.data_type_key]
        gexin_key = [self.tuijian_key, self.tuijian_id_key, self.tuijian_type_key, "tuijian_reason", self.data_type_key]
        t1 = time.clock()
        # print "leibiao"
        access_log.debug("dingyue query res time: %.2f s" % (t1 - t0))
        """
        个性化接口
        """

        if int(self.user_id) != 0:
            # print "page" , self.page
            if int(self.page) == 1:
                tuijian_args = [Config, self.user_attention_page_user_key, guanzhu_sql,
                                self.dingyue_apphot_history_key_time]

                tuijian_result = yield gen.Task(one_key_concern_158, *tuijian_args)
                tuijian_result = map(lambda x: x + self.sep + "1", tuijian_result) if tuijian_result else []
                # tuijian_result_count = len(tuijian_result)
                tuijian_result_value = map(lambda x: dict(zip(gexin_key, x.split("_"))), tuijian_result)

                guanzhu_baike_args = [Config, "baike", guanzhu_baike_sql, self.dingyue_apphot_history_key_time]
                guanzhu_rules_args = [Config, "rules", guanzhu_rules_sql, self.dingyue_apphot_history_key_time]
                dingyue_user_args = [Config, "dinigyue_user", dingyue_user_sql, self.dingyue_apphot_history_key_time]
                t2 = time.clock()
                guanzhu_baike_data = yield gen.Task(one_key_concern, *guanzhu_baike_args)
                guanzhu_rules_data = yield gen.Task(one_key_concern, *guanzhu_rules_args)
                dingyue_user_data = yield gen.Task(select_UserDB, *dingyue_user_args)
                # print "guanzhu_baike_data", guanzhu_baike_data
                # print "guanzhu_rules_data", guanzhu_rules_data
                # print "dingyue_user_data", dingyue_user_data
                if guanzhu_baike_data:
                    if dingyue_user_data:
                        if guanzhu_rules_data:
                            guize = guanzhu_baike_data + guanzhu_rules_data + dingyue_user_data
                        else:
                            guize = guanzhu_baike_data + dingyue_user_data
                    else:
                        if guanzhu_rules_data:
                            guize = guanzhu_baike_data + guanzhu_rules_data
                        else:
                            guize = guanzhu_baike_data
                else:
                    if dingyue_user_data:
                        if guanzhu_rules_data:
                            guize = guanzhu_rules_data + dingyue_user_data
                        else:
                            guize = dingyue_user_data
                    else:
                        guize = guanzhu_rules_data
                print "user_id" , self.user_id
                print "guize" ,guize
                if guize:
                    guize_data_not_china = map(lambda x: dict(zip(not_china_key, x.split("_"))), guize)
                    guize_data_china = map(lambda x: dict(zip(china_key, x.split("_"))), guize)
                    tuijian_list_result = set.difference(
                        *[{d['tuijian_id'] for d in ls} for ls in [tuijian_result_value, guize_data_not_china]])
                    rules_result_new = [d for d in tuijian_result_value if d['tuijian_id'] in tuijian_list_result]
                    rules_result_all = set.difference(
                        *[{d['tuijian'] for d in ls} for ls in [rules_result_new, guize_data_china]])
                    result_data_guolv = [d for d in rules_result_new if d['tuijian'] in rules_result_all]
                    # print "个性滑动"
                    # print "result_data_guolv", result_data_guolv
                    # result_data = map(
                    #     lambda k: (
                    #         k.get("tuijian"), k.get("tuijian_id"), k.get('tuijian_type'), k.get("data_type")),
                    #     result_data_guolv)
                    # result_data_jiegou = [dict(zip(key, i)) for i in result_data]

                    if result_data_guolv:
                        tuijian_gexin_length = len(result_data_guolv)
                        # print "tuijian_gexin_length" , tuijian_gexin_length
                        # print "zhanshulin"
                        if (int(tuijian_gexin_length) >= 3):
                            if (int(tuijian_gexin_length) < 7):
                                rules_zhiding_args = [Config, "new_attention_page_user_key" + self.sep + self.user_id,
                                                      rules_zhiding_sql, self.dingyue_apphot_history_key_time]
                                rules_zhiding_data = yield gen.Task(one_key_concern, *rules_zhiding_args)
                                rules__zhiding_result = map(lambda s: s + self.sep + "0",
                                                            rules_zhiding_data) if rules_zhiding_data else []
                                rules_zhiding_result_value = map(lambda x: dict(zip(tuijian_key, x.split("_"))),
                                                                 rules__zhiding_result)
                                tuijian_list_zhiding_zhiding = set.difference(*[{d['tuijian_id'] for d in ls} for ls in
                                                                                [rules_zhiding_result_value,
                                                                                 guize_data_not_china]])
                                rules_result_zhiding_new = [d for d in rules_zhiding_result_value if
                                                            d['tuijian_id'] in tuijian_list_zhiding_zhiding]
                                rules_result_zhiding_all = set.difference(
                                    *[{d['tuijian'] for d in ls} for ls in
                                      [rules_result_zhiding_new, guize_data_china]])
                                result_data_zhiding_guolv = [d for d in rules_result_zhiding_new if
                                                             d['tuijian'] in rules_result_zhiding_all]
                                if result_data_zhiding_guolv:
                                    result_data = map(
                                        lambda k: (
                                            k.get("tuijian"), k.get("tuijian_id"), k.get('tuijian_type'),
                                            k.get("data_type")),
                                        result_data_zhiding_guolv)
                                    result_data_zhiding_jiegou = [dict(zip(key, i)) for i in result_data]
                                    gexin_length = int(10) - int(tuijian_gexin_length)
                                    zhiding_length = len(result_data_zhiding_jiegou)
                                    # zhiding_jiegou = result_data_zhiding_jiegou[0:gexin_length]
                                    if (zhiding_length < gexin_length):
                                        rules_not_zhiding_args = [Config,
                                                                  "zhiding_not_zhiding_key" + self.sep + self.user_id,
                                                                  rules_not_zhiding_sql,
                                                                  self.dingyue_apphot_history_key_time]
                                        tuijian_list_not_zhiding_result = yield gen.Task(one_key_concern,
                                                                                         *rules_not_zhiding_args)
                                        tuijian_list_not_zhiding_result = map(lambda s: s + self.sep + "0",
                                                                              tuijian_list_not_zhiding_result) if tuijian_list_not_zhiding_result else []
                                        tuijian_list_result_value = map(lambda x: dict(zip(tuijian_key, x.split("_"))),
                                                                        tuijian_list_not_zhiding_result)
                                        tuijian_list_resulnot_zhding = set.difference(
                                            *[{d['tuijian_id'] for d in ls} for ls in
                                              [tuijian_list_result_value,
                                               guize_data_not_china]])
                                        rules_result_newnot_zhding = [d for d in tuijian_list_result_value if
                                                                      d['tuijian_id'] in tuijian_list_resulnot_zhding]
                                        rules_result_allnot_zhding = set.difference(
                                            *[{d['tuijian'] for d in ls} for ls in
                                              [rules_result_newnot_zhding, guize_data_china]])
                                        result_data_not_zhding = [d for d in rules_result_newnot_zhding if
                                                                  d['tuijian'] in rules_result_allnot_zhding]
                                        if result_data_not_zhding:
                                            result_data_not_zhding_data = map(
                                                lambda k: (
                                                    k.get("tuijian"), k.get("tuijian_id"), k.get('tuijian_type'),
                                                    k.get("data_type")),
                                                result_data_not_zhding)
                                            result_data_not_zhiding_jiegou = [dict(zip(key, i)) for i in
                                                                              result_data_not_zhding_data]
                                            length = int(10) - int(tuijian_gexin_length) - int(zhiding_length)
                                            not_zhiding_result = result_data_not_zhiding_jiegou[0:length]
                                            gexin_result = result_data_zhiding_jiegou + not_zhiding_result
                                            gexin_result_length = len(gexin_result)
                                            if gexin_result_length >= 3:
                                                row = [{"row_data": result_data_guolv, 'page': self.page, 'position': 5,
                                                    "module_name": self.recommend_name},
                                                   {"row_data": gexin_result, 'page': self.page, 'position': 20,
                                                    "module_name": self.backstage_name}]
                                                # self.op_json_data(data=row)
                                            else:
                                                row = [{"row_data": result_data_guolv, 'page': self.page, 'position': 5,
                                                        "module_name": self.recommend_name}]
                                                # self.op_json_data(data=row)
                                        else:
                                            # zhiding_jiegou = result_data_zhiding_jiegou[0:gexin_length]
                                            if zhiding_length >= 3:
                                                row = [{"row_data": result_data_guolv, 'page': self.page, 'position': 5,
                                                    "module_name": self.recommend_name},
                                                    {"row_data": result_data_zhiding_jiegou, 'page': self.page,
                                                    'position': 20,
                                                    "module_name": self.backstage_name}]
                                                # self.op_json_data(data=row)
                                            else:
                                                row = [{"row_data": result_data_guolv, 'page': self.page, 'position': 5,
                                                        "module_name": self.recommend_name}]
                                                # self.op_json_data(data=row)
                                    else:
                                        if zhiding_length >= 3:
                                            zhiding_jiegou = result_data_zhiding_jiegou[0:gexin_length]
                                            print "gexin_length", gexin_length
                                            print "zhiding_jiegou" , zhiding_jiegou
                                            row = [{"row_data": result_data_guolv, 'page': self.page, 'position': 5,
                                                "module_name": self.recommend_name},
                                               {"row_data": zhiding_jiegou, 'page': self.page, 'position': 20,
                                                "module_name": self.backstage_name}]
                                            print "row"
                                            # self.op_json_data(data=row)
                                        else:
                                            row = [{"row_data": result_data_guolv, 'page': self.page, 'position': 5,
                                                    "module_name": self.recommend_name}]
                                            # self.op_json_data(data=row)
                                else:
                                    rules_not_zhiding_args = [Config,
                                                              "zhiding_not_zhiding_key" + self.sep + self.user_id,
                                                              rules_not_zhiding_sql,
                                                              self.dingyue_apphot_history_key_time]
                                    tuijian_list_not_zhiding_result = yield gen.Task(one_key_concern,
                                                                                     *rules_not_zhiding_args)
                                    tuijian_list_not_zhiding_result = map(lambda s: s + self.sep + "0",
                                                                          tuijian_list_not_zhiding_result) if tuijian_list_not_zhiding_result else []
                                    tuijian_list_result_value = map(lambda x: dict(zip(tuijian_key, x.split("_"))),
                                                                    tuijian_list_not_zhiding_result)
                                    tuijian_list_resulnot_zhding = set.difference(
                                        *[{d['tuijian_id'] for d in ls} for ls in
                                          [tuijian_list_result_value,
                                           guize_data_not_china]])
                                    rules_result_newnot_zhding = [d for d in tuijian_list_result_value if
                                                                  d['tuijian_id'] in tuijian_list_resulnot_zhding]
                                    rules_result_allnot_zhding = set.difference(
                                        *[{d['tuijian'] for d in ls} for ls in
                                          [rules_result_newnot_zhding, guize_data_china]])
                                    result_data_not_zhding = [d for d in rules_result_newnot_zhding if
                                                              d['tuijian'] in rules_result_allnot_zhding]
                                    result_data_not_zhding_length = len(result_data_not_zhding)
                                    if result_data_not_zhding:
                                        result_data_not_zhding_data = map(
                                            lambda k: (
                                                k.get("tuijian"), k.get("tuijian_id"), k.get('tuijian_type'),
                                                k.get("data_type")),
                                            result_data_not_zhding)
                                        result_data_not_zhiding_jiegou = [dict(zip(key, i)) for i in
                                                                          result_data_not_zhding_data]
                                        length = int(10) - int(tuijian_gexin_length)
                                        not_zhiding_result = result_data_not_zhiding_jiegou[0:length]
                                        # gexin_result = result_data_zhiding_jiegou + not_zhiding_result
                                        gexin_result_length = len(not_zhiding_result)
                                        if gexin_result_length >= 3:
                                            row = [{"row_data": result_data_guolv, 'page': self.page, 'position': 5,
                                                    "module_name": self.recommend_name},
                                                   {"row_data": not_zhiding_result, 'page': self.page, 'position': 20,
                                                    "module_name": self.backstage_name}]
                                            # self.op_json_data(data=row)
                                        else:
                                            row = [{"row_data": result_data_guolv, 'page': self.page, 'position': 5,
                                                    "module_name": self.recommend_name}]
                                            # self.op_json_data(data=row)
                                    else:
                                        row = [{"row_data": result_data_guolv, 'page': self.page, 'position': 5,
                                                "module_name": self.recommend_name}]
                                        # self.op_json_data(data=row)
                            else:
                                result_data_guolv_all = result_data_guolv[0:10]
                                # print "result_data_guolv_all" , result_data_guolv_all
                                row = [{"row_data": result_data_guolv_all, 'page': self.page, 'position': 5,
                                        "module_name": self.recommend_name}]
                                # self.op_json_data(data=row)
                    else:
                        """
                            个性话数据没有的时
                        """
                        # print "111111111111111111111111"
                        rules_zhiding_args = [Config, "new_attention_page_user_key" + self.sep + self.user_id,
                                              rules_zhiding_sql, self.dingyue_apphot_history_key_time]
                        # print "rules_zhiding_sql" , rules_zhiding_sql
                        # print "rules_zhiding_args" , rules_zhiding_args
                        # t2 = time.clock()
                        rules_zhiding_data = yield gen.Task(one_key_concern, *rules_zhiding_args)

                        rules_zhiding_result_new = map(lambda s: s + self.sep + "0",
                                                       rules_zhiding_data) if rules_zhiding_data else []
                        rules_zhiding_result = map(lambda x: dict(zip(tuijian_key, x.split("_"))), rules_zhiding_result_new)
                        # print "rules_zhiding_result" , rules_zhiding_result
                        rules_zhiding_list_result = set.difference(
                            *[{d['tuijian_id'] for d in ls} for ls in [rules_zhiding_result, guize_data_not_china]])
                        rules_result_new = [d for d in rules_zhiding_result if
                                            d['tuijian_id'] in rules_zhiding_list_result]
                        rules_result_all = set.difference(
                            *[{d['tuijian'] for d in ls} for ls in [rules_result_new, guize_data_china]])
                        result_data_guolv = [d for d in rules_result_new if d['tuijian'] in rules_result_all]
                        result_data = map(
                            lambda k: (
                            k.get("tuijian"), k.get("tuijian_id"), k.get('tuijian_type'), k.get("data_type")),
                            result_data_guolv)
                        result_data_jiegou = [dict(zip(key, i)) for i in result_data]

                        tuijian_zhiding_result_length = len(result_data_jiegou)
                        # print "result_data_jiegou" , result_data_jiegou
                        # print "tuijian_zhiding_result_length" , tuijian_zhiding_result_length
                        if result_data_jiegou:
                            if (int(tuijian_zhiding_result_length) >= 3):
                                if (int(tuijian_zhiding_result_length) < 10):
                                    rules_not_zhiding_args = [Config,
                                                              "zhiding_not_zhiding_key" + self.sep + self.user_id,
                                                              rules_not_zhiding_sql,
                                                              self.dingyue_apphot_history_key_time]
                                    tuijian_list_not_zhiding_result = yield gen.Task(one_key_concern,
                                                                                     *rules_not_zhiding_args)
                                    tuijian_list_not_zhiding_result = map(lambda s: s + self.sep + "0",
                                                                          tuijian_list_not_zhiding_result) if tuijian_list_not_zhiding_result else []
                                    t4 = time.clock()
                                    # access_log.debug("dingyue query res time: %.2f s" % (t4 - t3))
                                    # tuijian_lsit = tuijian_list_not_zhiding_result
                                    tuijian_list_result_value = map(lambda x: dict(zip(tuijian_key, x.split("_"))),
                                                                    tuijian_list_not_zhiding_result)

                                    tuijian_list_resulnot_zhding = set.difference(
                                        *[{d['tuijian_id'] for d in ls} for ls in
                                          [tuijian_list_result_value,
                                           guize_data_not_china]])
                                    rules_result_newnot_zhding = [d for d in tuijian_list_result_value if
                                                                  d['tuijian_id'] in tuijian_list_resulnot_zhding]
                                    rules_result_allnot_zhding = set.difference(
                                        *[{d['tuijian'] for d in ls} for ls in
                                          [rules_result_newnot_zhding, guize_data_china]])
                                    result_data_not_zhding = [d for d in rules_result_newnot_zhding if
                                                              d['tuijian'] in rules_result_allnot_zhding]
                                    if result_data_not_zhding:
                                        result_data_not_zhding_data = map(lambda k: (
                                            k.get("tuijian"), k.get("tuijian_id"), k.get('tuijian_type'),
                                            k.get("data_type")),
                                                                          result_data_not_zhding)
                                        result_data_not_zhiding_jiegou = [dict(zip(key, i)) for i in
                                                                          result_data_not_zhding_data]
                                        # 可能有bug
                                        # result_data_not_zhiding_jiegou_length_new = len(result_data_not_zhiding_jiegou)
                                        length_not_zhiding = int(10) - tuijian_zhiding_result_length
                                        not_zhiding = result_data_not_zhiding_jiegou[0:length_not_zhiding]
                                        zhiding = result_data_jiegou + not_zhiding
                                        # result_data_new = zhiding[0:10]
                                        row = [{"row_data": zhiding, 'page': self.page, 'position': 20,
                                                "module_name": self.backstage_name}]
                                        # result_data_new_1 = {"row_data": row}
                                        # self.op_json_data(data=row)
                                    else:
                                        row = [{"row_data": result_data_jiegou, 'page': self.page, 'position': 20,
                                                "module_name": self.backstage_name}]
                                        # result_data_new_1 = {"row_data": row}
                                        # self.op_json_data(data=row)
                                else:
                                    zhiding = result_data_jiegou[0:10]
                                    row = [{"row_data": zhiding, 'page': self.page, 'position': 20,
                                            "module_name": self.backstage_name}]
                                    # self.op_json_data(data=row)
                            else:
                                rules_not_zhiding_args = [Config,
                                                          "zhiding_not_zhiding_key" + self.sep + self.user_id,
                                                          rules_not_zhiding_sql, self.dingyue_apphot_history_key_time]
                                tuijian_list_not_zhiding_result = yield gen.Task(one_key_concern,
                                                                                 *rules_not_zhiding_args)
                                tuijian_list_not_zhiding_result = map(lambda s: s + self.sep + "0",
                                                                      tuijian_list_not_zhiding_result) if tuijian_list_not_zhiding_result else []
                                t4 = time.clock()
                                # access_log.debug("dingyue query res time: %.2f s" % (t4 - t3))
                                # tuijian_lsit = tuijian_list_not_zhiding_result
                                tuijian_list_result_value = map(lambda x: dict(zip(tuijian_key, x.split("_"))),
                                                                tuijian_list_not_zhiding_result)

                                tuijian_list_resulnot_zhding = set.difference(*[{d['tuijian_id'] for d in ls} for ls in
                                                                                [tuijian_list_result_value,
                                                                                 guize_data_not_china]])
                                rules_result_newnot_zhding = [d for d in tuijian_list_result_value if
                                                              d['tuijian_id'] in tuijian_list_resulnot_zhding]
                                rules_result_allnot_zhding = set.difference(
                                    *[{d['tuijian'] for d in ls} for ls in
                                      [rules_result_newnot_zhding, guize_data_china]])
                                result_data_not_zhding = [d for d in rules_result_newnot_zhding if
                                                          d['tuijian'] in rules_result_allnot_zhding]
                                if result_data_not_zhding:
                                    result_data_not_zhding_data = map(lambda k: (
                                        k.get("tuijian"), k.get("tuijian_id"), k.get('tuijian_type'),
                                        k.get("data_type")),
                                                                      result_data_not_zhding)
                                    result_data_not_zhiding_jiegou = [dict(zip(key, i)) for i in
                                                                      result_data_not_zhding_data]
                                    not_zhiding = len(result_data_not_zhiding_jiegou)
                                    if (int(not_zhiding) >= int(2)):
                                        if int(not_zhiding) < int(9):
                                            # length_not_zhiding = int(10) - tuijian_zhiding_result_length
                                            # not_zhiding_data = result_data_not_zhiding_jiegou
                                            all_data = result_data_jiegou + result_data_not_zhiding_jiegou
                                            row = [{"row_data": all_data, 'page': self.page, 'position': 20,
                                                    "module_name": self.backstage_name}]
                                            # self.op_json_data(data=row)
                                        else:
                                            length_not_zhiding = int(10) - tuijian_zhiding_result_length
                                            not_zhiding_data_new = result_data_not_zhiding_jiegou[0:length_not_zhiding]
                                            all_data = result_data_jiegou + not_zhiding_data_new
                                            row = [{"row_data": all_data, 'page': self.page, 'position': 20,
                                                    "module_name": self.backstage_name}]
                                            # self.op_json_data(data=row)
                        else:
                            rules_not_zhiding_args = [Config, "zhiding_not_zhiding_key" + self.sep + self.user_id,
                                                      rules_not_zhiding_sql, self.dingyue_apphot_history_key_time]
                            tuijian_list_not_zhiding_result = yield gen.Task(one_key_concern, *rules_not_zhiding_args)
                            tuijian_list_not_zhiding_result = map(lambda s: s + self.sep + "0",
                                                                  tuijian_list_not_zhiding_result) if tuijian_list_not_zhiding_result else []
                            t4 = time.clock()
                            # access_log.debug("dingyue query res time: %.2f s" % (t4 - t3))
                            # tuijian_lsit = tuijian_list_not_zhiding_result
                            tuijian_list_result_value = map(lambda x: dict(zip(tuijian_key, x.split("_"))),
                                                            tuijian_list_not_zhiding_result)

                            tuijian_list_resulnot_zhding = set.difference(
                                *[{d['tuijian_id'] for d in ls} for ls in
                                  [tuijian_list_result_value, guize_data_not_china]])
                            rules_result_newnot_zhding = [d for d in tuijian_list_result_value if
                                                          d['tuijian_id'] in tuijian_list_resulnot_zhding]
                            rules_result_allnot_zhding = set.difference(
                                *[{d['tuijian'] for d in ls} for ls in [rules_result_newnot_zhding, guize_data_china]])
                            result_data_not_zhding = [d for d in rules_result_newnot_zhding if
                                                      d['tuijian'] in rules_result_allnot_zhding]
                            # result_data_not_zhding.sort(key=lambda k: (k.get('id', 0)))
                            # result_data_not_zhding.sort(key=lambda k: (k.get("sort_num", 0)))
                            result_data_not_zhding_data = map(
                                lambda k: (
                                    k.get("tuijian"), k.get("tuijian_id"), k.get('tuijian_type'), k.get("data_type")),
                                result_data_not_zhding)
                            result_data_not_zhiding_jiegou = [dict(zip(key, i)) for i in result_data_not_zhding_data]
                            not_zhiding_length = len(result_data_not_zhiding_jiegou)
                            if (int(not_zhiding_length) >= int(3)):
                                if int(not_zhiding_length) < int(10):
                                    row = [
                                        {"row_data": result_data_not_zhiding_jiegou, 'page': self.page, 'position': 20,
                                         "module_name": self.backstage_name}]
                                    # self.op_json_data(data=row)
                                else:
                                    jieguo = result_data_not_zhiding_jiegou[0:10]
                                    row = [
                                        {"row_data": jieguo, 'page': self.page, 'position': 20,
                                         "module_name": self.backstage_name}]
                                    # self.op_json_data(data=row)

    def op_json_data(self, data):
        out_str = json.dumps(self.correct_json(data), ensure_ascii=False)
        self.set_header("Content-Type", "application/json; charset=utf-8")
        self.write(out_str)

    def error_json(self, error_msg):
        return {"error_code": "1", "data": "0", "error_msg": error_msg}

    def correct_json(self, data):
        return {"data": data}
        # return {"data": data}
    def write_error(self, status_code, **kwargs):
        self.set_status(status_code)


if __name__ == "__main__":

    # Business(Config)
    pass
