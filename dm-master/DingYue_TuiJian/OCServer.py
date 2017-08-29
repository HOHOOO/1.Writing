# coding=utf-8
import json
import re
import urllib
from random import choice
import time
from tornado import gen
from tornado.web import asynchronous, RequestHandler
from tornado.log import access_log

# from base.consts import *
from tasks import *


class AsyncReqOCDataHandler(RequestHandler):
    def __init__(self, application, request, **kwargs):
        super(AsyncReqOCDataHandler, self).__init__(application, request, **kwargs)
        self.user_id = None
        self.offset = None
        self.limit = None

        # self.OC_zhiding_sql = None
        # self.dingyue_apphot_key = None
        # self.dingyue_apphot_history_key_time = None
        # self.trace_id_map_file = None

        self.sep = "_"

    def prepare(self):
        self.user_id = str(self.get_argument("user_id", -1))
        self.offset = int(self.get_argument("offset", 0))
        self.limit = int(self.get_argument("limit", 0))

        # 模块名
        self.recommend_name = self.application.backend.get_recommend_name()
        self.backstage_name = self.application.backend.get_backstage_name()

        # json key
        self.tuijian_key = self.application.backend.get_tuijian_key()
        self.tuijian_id_key = self.application.backend.get_tuijian_id_key()
        self.tuijian_type_key = self.application.backend.get_tuijian_type_key()
        self.data_type_key = self.application.backend.get_data_type_key()

        # 一键关注key
        self.one_key_concern_zhiding_key = self.application.backend.get_one_key_concern_zhiding_key()
        self.one_key_concern_not_zhiding_key = self.application.backend.get_one_key_concern_not_zhiding_key()
        self.user_attention_page_user_key = self.application.backend.get_user_attention_page_user_key()

        # biao
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
        guanzhu_rules_sql = "select distinct user_id,b.keyword from {table_name1} a left join {table_name2} b on a.rule_id = b.id where user_id = {user_id} and type !='title' " .format(
            user_id=self.user_id,table_name1=self.user_rules_table , table_name2 = self.rules_table)
        guanzhu_baike_sql = "select user_id,wiki_id from {table_name} where user_id = {user_id} union all select user_id,wiki_hash_id from {table_name} where user_id = {user_id}".format(
            user_id=self.user_id , table_name=self.dingyue_baike_table)
        dingyue_user_sql = "select user_id,follow_user_id from {table_name} where user_id = {user_id} ".format(
            user_id=self.user_id, table_name=self.follow_relate_table)
        rules_zhiding_sql = "select id,tuijian ,tuijian_id ,tuijian_type,sort_num  from {table_name} where qianzhi = '' and tuijian_type <> 'baike' and status = 1 ORDER BY sort_num ASC,id DESC".format(
            table_name = self.dingyue_apphot_table)
        rules_not_zhiding_sql = "select id,tuijian ,tuijian_id ,tuijian_type,sort_num  from {table_name} where qianzhi <> '' and tuijian_type <> 'baike' and status = 1 ORDER BY sort_num ASC,id DESC".format(
            table_name=self.dingyue_apphot_table)
        guanzhu_sql = "select tuijian ,tuijian_id ,tuijian_type  from {table_name} ".format(
            table_name=self.gexinhua_table)

        rules_zhiding_baike_sql = "select tuijian ,tuijian_id ,tuijian_type from {table_name} where qianzhi = ''  and status = 1 and tuijian_type <> 'baike' ORDER BY sort_num ASC,id DESC".format(
            table_name=self.dingyue_apphot_table)
        rules_not_zhiding_baike_sql = "select tuijian ,tuijian_id ,tuijian_type  from {table_name} where qianzhi <> ''  and status = 1 and tuijian_type <> 'baike' ORDER BY sort_num ASC,id DESC".format(table_name=self.dingyue_apphot_table)

        # OC_zhiding_sql = " select tuijian,tuijian_id,tuijian_type from {table1_name} where qianzhi = '' and status = 1  and tuijian_type <> 'baike' ORDER BY sort_num ASC,id DESC ".format(
        #     table1_name=self.dingyue_apphot_table)
        # OC_not_zhiding_sql = " select tuijian,tuijian_id,tuijian_type from {table1_name} where qianzhi <> '' and status = 1  and tuijian_type <> 'baike' ORDER BY sort_num ASC,id DESC ".format(
        #     table1_name=self.dingyue_apphot_table)

        # print "zhan"
        t0 = time.clock()
        tuijian_key = ["id",self.tuijian_key, self.tuijian_id_key, self.tuijian_type_key, "sort_num",self.data_type_key]
        not_china_key = ["user_id","tuijian_id"]
        china_key = ["user_id","tuijian"]
        key = [self.tuijian_key, self.tuijian_id_key, self.tuijian_type_key, self.data_type_key]

        t1 = time.clock()
        access_log.debug("dingyue query res time: %.2f s" % (t1 - t0))
        if int(self.user_id) == 0:
            # print "未登录"
            tuijian_zhiding = [Config, self.one_key_concern_zhiding_key, rules_zhiding_baike_sql,
                               self.dingyue_apphot_history_key_time]
            t2 = time.clock()
            tuijian_zhiding_result = yield gen.Task(one_key_concern, *tuijian_zhiding)
            # print "tuijian_zhiding_result", tuijian_zhiding_result
            tuijian_zhiding_result = map(lambda s: s + self.sep + "0",
                                         tuijian_zhiding_result) if tuijian_zhiding_result else []
            # print Not_Individualization
            access_log.debug("dingyue quecdry res time: %.2f s" % (t2 - t1))
            tuijian_zhiding_result_value = map(lambda x: dict(zip(key, x.split("_"))), tuijian_zhiding_result)
            # print "tuijian_zhiding_result_value", tuijian_zhiding_result_value
            if tuijian_zhiding_result_value:
                Not_Indiv_count = len(tuijian_zhiding_result_value)
                if self.limit > Not_Indiv_count:
                    tuijian_not_zhiding_args = [Config, self.one_key_concern_not_zhiding_key,
                                                rules_not_zhiding_baike_sql, self.dingyue_apphot_history_key_time]
                    tuijian_not_zhiding_result = yield gen.Task(one_key_concern, *tuijian_not_zhiding_args)
                    tuijian_not_zhiding_result = map(lambda s: s + self.sep + "0",
                                                     tuijian_not_zhiding_result) if tuijian_not_zhiding_result else []
                    t3 = time.clock()
                    access_log.debug("dingyue query res time: %.2f s" % (t3 - t2))
                    not_zhiding = map(lambda x: dict(zip(key, x.split("_"))), tuijian_not_zhiding_result)
                    # print not_zhiding
                    length = int(15) - int(Not_Indiv_count)
                    not_zhiding_result = not_zhiding[0:length]
                    result = tuijian_zhiding_result_value + not_zhiding_result
                    # result_value = map(lambda x: dict(zip(key, x.split("_"))), result)
                    row = {"row_data": result}
                    self.op_json_data(data=row)
                else:
		    tuijian_zhiding_result_value_data = tuijian_zhiding_result_value[0:15]
                    row = {"row_data": tuijian_zhiding_result_value_data}
                    self.op_json_data(data=row)
            else:
                tuijian_not_zhiding_args = [Config, self.one_key_concern_not_zhiding_key,
                                            rules_not_zhiding_baike_sql,
                                            self.offset, self.limit, self.dingyue_apphot_history_key_time]
                tuijian_not_zhiding_result = yield gen.Task(select_data, *tuijian_not_zhiding_args)
                tuijian_not_zhiding_result = map(lambda s: s + self.sep + "0",
                                                 tuijian_not_zhiding_result) if tuijian_not_zhiding_result else []
                t3 = time.clock()
                access_log.debug("dingyue query res time: %.2f s" % (t3 - t2))
                result_value = map(lambda x: dict(zip(key, x.split("_"))), tuijian_not_zhiding_result)
                result_value_data = result_value[0:15]
                row = {"row_data": result_value_data}
                self.op_json_data(data=row)
        else:
            """
            非个性化
            """
            # print "登录"
            # tuijian_zhiding = [Config, self.one_key_concern_zhiding_key+self.sep+self.user_id, self.OC_guanzhuye_zhiding_sql,sql,self.user_id,self.limit]
            # rules_zhiding_args = [Config, self.one_key_concern_zhiding_key, rules_zhiding_sql,
            #                       self.dingyue_apphot_history_key_time]
            # guanzhu_baike_args = [Config, "baike", guanzhu_baike_sql, self.dingyue_apphot_history_key_time]
            # guanzhu_rules_args = [Config, "rules", guanzhu_rules_sql, self.dingyue_apphot_history_key_time]
            # dingyue_user_args = [Config, "dinigyue_user", dingyue_user_sql, self.dingyue_apphot_history_key_time]
            #
            # t2 = time.clock()
            # rules_zhiding_data = yield gen.Task(one_key_concern, *rules_zhiding_args)
            # guanzhu_baike_data = yield gen.Task(one_key_concern, *guanzhu_baike_args)
            # guanzhu_rules_data = yield gen.Task(one_key_concern, *guanzhu_rules_args)
            # dingyue_user_data = yield gen.Task(one_key_concern, *dingyue_user_args)
            # # print 'guanzhu_rules_data' , guanzhu_rules_data
            #
            # rules_zhiding_data = map(lambda s: s + self.sep + "0",
            #                          rules_zhiding_data) if rules_zhiding_data else []
            #
            # access_log.debug("dingyue quecdry res time: %.2f s" % (t2 - t1))
            # rules_zhiding_data = map(lambda x: dict(zip(tuijian_key, x.split("_"))), rules_zhiding_data)
            # # guize_data = guanzhu_baike_data + guanzhu_rules_data
            # # guize_data_not_china = map(lambda x: dict(zip(not_china_key,x.split("_"))),guanzhu_rules_data)
            # # guize_data_china = map(lambda x: dict(zip(china_key, x.split("_"))), guanzhu_rules_data)
            # if guanzhu_baike_data:
            #     if dingyue_user_data:
            #         if guanzhu_rules_data:
            #             guize = guanzhu_baike_data + guanzhu_rules_data + dingyue_user_data
            #         else:
            #             guize = guanzhu_baike_data + dingyue_user_data
            #     else:
            #         if guanzhu_rules_data:
            #             guize = guanzhu_baike_data + guanzhu_rules_data
            #         else:
            #             guize = guanzhu_baike_data
            # else:
            #     if dingyue_user_data:
            #         if guanzhu_rules_data:
            #             guize = guanzhu_rules_data + dingyue_user_data
            #         else:
            #             guize = dingyue_user_data
            #     else:
            #         guize = guanzhu_rules_data
            # # print "guize" , guize
            # guize_data_not_china = map(lambda x: dict(zip(not_china_key, x.split("_"))), guize)
            # guize_data_china = map(lambda x: dict(zip(china_key, x.split("_"))), guize)
            # tuijian_list_result = set.difference(
            #     *[{d['tuijian_id'] for d in ls} for ls in [rules_zhiding_data, guize_data_not_china]])
            # rules_result_new = [d for d in rules_zhiding_data if d['tuijian_id'] in tuijian_list_result]
            # rules_result_all = set.difference(
            #     *[{d['tuijian'] for d in ls} for ls in [rules_result_new, guize_data_china]])
            # result_data_guolv = [d for d in rules_result_new if d['tuijian'] in rules_result_all]
            # result_data = map(
            #     lambda k: (
            #         k.get("tuijian"), k.get("tuijian_id"), k.get('tuijian_type'), k.get("data_type")),
            #     result_data_guolv)
            # result_data_jiegou = [dict(zip(key, i)) for i in result_data]
            #
            # if result_data_jiegou:
            #     Not_Indiv_count = len(result_data_jiegou)
            #     if self.limit > Not_Indiv_count:
            #         rules_not_zhiding_args = [Config, "zhiding_not_zhiding_key" + self.sep + self.user_id,
            #                                   rules_not_zhiding_sql, self.dingyue_apphot_history_key_time]
            #         tuijian_list_not_zhiding_result = yield gen.Task(one_key_concern, *rules_not_zhiding_args)
            #         tuijian_list_not_zhiding_result = map(lambda s: s + self.sep + "0",
            #                                               tuijian_list_not_zhiding_result) if tuijian_list_not_zhiding_result else []
            #         # print tuijian_list_not_zhiding_result
            #         t4 = time.clock()
            #         # access_log.debug("dingyue query res time: %.2f s" % (t4 - t3))
            #         # tuijian_lsit = tuijian_list_not_zhiding_result
            #         tuijian_list_result_value = map(lambda x: dict(zip(tuijian_key, x.split("_"))),
            #                                         tuijian_list_not_zhiding_result)
            #
            #         tuijian_list_resulnot_zhding = set.difference(*[{d['tuijian_id'] for d in ls} for ls in
            #                                                         [tuijian_list_result_value,
            #                                                          guize_data_not_china]])
            #         rules_result_newnot_zhding = [d for d in tuijian_list_result_value if
            #                                       d['tuijian_id'] in tuijian_list_resulnot_zhding]
            #         rules_result_allnot_zhding = set.difference(
            #             *[{d['tuijian'] for d in ls} for ls in [rules_result_newnot_zhding, guize_data_china]])
            #         result_data_not_zhding = [d for d in rules_result_newnot_zhding if
            #                                   d['tuijian'] in rules_result_allnot_zhding]
            #         # result_data_not_zhding.sort(key=lambda k: (k.get('id', 0)))
            #         # result_data_not_zhding.sort(key=lambda k: (k.get("sort_num", 0)))
            #         result_data_not_zhding_data = map(
            #             lambda k: (
            #                 k.get("tuijian"), k.get("tuijian_id"), k.get('tuijian_type'), k.get("data_type")),
            #             result_data_not_zhding)
            #         result_data_not_zhiding_jiegou = [dict(zip(key, i)) for i in result_data_not_zhding_data]
            #         length = int(6) - int(Not_Indiv_count)
            #         not_zhiding_data = result_data_not_zhiding_jiegou[0:length]
            #         zhiding_not_zhiding_jiegou = result_data_jiegou + not_zhiding_data
            #         row = {"row_data": zhiding_not_zhiding_jiegou}
            #         self.op_json_data(data=row)
            #     else:
            #         tuijian_zhiding_result_value_data = result_data_jiegou[0:6]
            #         row = {"row_data": tuijian_zhiding_result_value_data}
            #         self.op_json_data(data=row)
            # else:
            #     rules_not_zhiding_args = [Config, "zhiding_not_zhiding_key" + self.sep + self.user_id,
            #                               rules_not_zhiding_sql, self.dingyue_apphot_history_key_time]
            #     tuijian_list_not_zhiding_result = yield gen.Task(one_key_concern, *rules_not_zhiding_args)
            #     tuijian_list_not_zhiding_result = map(lambda s: s + self.sep + "0",
            #                                           tuijian_list_not_zhiding_result) if tuijian_list_not_zhiding_result else []
            #     t4 = time.clock()
            #     # access_log.debug("dingyue query res time: %.2f s" % (t4 - t3))
            #     # tuijian_lsit = tuijian_list_not_zhiding_result
            #     tuijian_list_result_value = map(lambda x: dict(zip(tuijian_key, x.split("_"))),
            #                                     tuijian_list_not_zhiding_result)
            #
            #     tuijian_list_resulnot_zhding = set.difference(*[{d['tuijian_id'] for d in ls} for ls in
            #                                                     [tuijian_list_result_value,
            #                                                      guize_data_not_china]])
            #     rules_result_newnot_zhding = [d for d in tuijian_list_result_value if
            #                                   d['tuijian_id'] in tuijian_list_resulnot_zhding]
            #     rules_result_allnot_zhding = set.difference(
            #         *[{d['tuijian'] for d in ls} for ls in [rules_result_newnot_zhding, guize_data_china]])
            #     result_data_not_zhding = [d for d in rules_result_newnot_zhding if
            #                               d['tuijian'] in rules_result_allnot_zhding]
            #     # result_data_not_zhding.sort(key=lambda k: (k.get('id', 0)))
            #     # result_data_not_zhding.sort(key=lambda k: (k.get("sort_num", 0)))
            #     result_data_not_zhding_data = map(
            #         lambda k: (
            #             k.get("tuijian"), k.get("tuijian_id"), k.get('tuijian_type'), k.get("data_type")),
            #         result_data_not_zhding)
            #     result_data_not_zhiding_jiegou = [dict(zip(key, i)) for i in result_data_not_zhding_data]
            #     not_zhid_a = result_data_not_zhiding_jiegou[0:6]
            #     self.op_json_data(data=not_zhid_a)
            """
            个性化
            """
            rules_zhiding_args = [Config, self.one_key_concern_zhiding_key, rules_zhiding_sql,
                                  self.dingyue_apphot_history_key_time]
            guanzhu_baike_args = [Config, "baike", guanzhu_baike_sql, self.dingyue_apphot_history_key_time]
            guanzhu_rules_args = [Config, "rules", guanzhu_rules_sql, self.dingyue_apphot_history_key_time]
            dingyue_user_args = [Config, "dinigyue_user", dingyue_user_sql, self.dingyue_apphot_history_key_time]


            t2 = time.clock()
            rules_zhiding_data = yield gen.Task(one_key_concern, *rules_zhiding_args)
            guanzhu_baike_data = yield gen.Task(one_key_concern, *guanzhu_baike_args)
            guanzhu_rules_data = yield gen.Task(one_key_concern, *guanzhu_rules_args)
            dingyue_user_data = yield gen.Task(select_UserDB, *dingyue_user_args)
            # print 'guanzhu_rules_data' , guanzhu_rules_data

            rules_zhiding_data = map(lambda s: s + self.sep + "0",
                                     rules_zhiding_data) if rules_zhiding_data else []

            access_log.debug("dingyue quecdry res time: %.2f s" % (t2 - t1))
            rules_zhiding_data = map(lambda x: dict(zip(tuijian_key, x.split("_"))), rules_zhiding_data)
            # print "rules_zhiding_data " , rules_zhiding_data
            # guize_data = guanzhu_baike_data + guanzhu_rules_data
            # guize_data_not_china = map(lambda x: dict(zip(not_china_key,x.split("_"))),guanzhu_rules_data)
            # guize_data_china = map(lambda x: dict(zip(china_key, x.split("_"))), guanzhu_rules_data)
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
            if guize:
                guize_data_not_china = map(lambda x: dict(zip(not_china_key, x.split("_"))), guize)
                guize_data_china = map(lambda x: dict(zip(china_key, x.split("_"))), guize)
                tuijian_list_result = set.difference(
                    *[{d['tuijian_id'] for d in ls} for ls in [rules_zhiding_data, guize_data_not_china]])
                rules_result_new = [d for d in rules_zhiding_data if d['tuijian_id'] in tuijian_list_result]
                rules_result_all = set.difference(
                    *[{d['tuijian'] for d in ls} for ls in [rules_result_new, guize_data_china]])
                result_data_guolv = [d for d in rules_result_new if d['tuijian'] in rules_result_all]
                print "1111111"
                print "user_Id" , self.user_id
                if result_data_guolv:
                    result_data = map(
                        lambda k: (
                            k.get("tuijian"), k.get("tuijian_id"), k.get('tuijian_type'), k.get("data_type")),
                        result_data_guolv)
                    result_data_jiegou = [dict(zip(key, i)) for i in result_data]
                    Not_Indiv_count = len(result_data_jiegou)
                    if int(self.limit) > Not_Indiv_count:
                        gexinhua_args = [Config, self.user_attention_page_user_key, guanzhu_sql,
                                         self.dingyue_apphot_history_key_time]
                        gexinhua_result = yield gen.Task(one_key_concern_158, *gexinhua_args)
                        gexinhua_result = map(lambda x: x + self.sep + "1", gexinhua_result) if gexinhua_result else []
                        # tuijian_result_count = len(gexinhua_result)
                        gexinhua_result_value = map(lambda x: dict(zip(key, x.split("_"))), gexinhua_result)
                        # print "gexinhua_result_value ", gexinhua_result_value
                        gexinhua_result_value_guolv = set.difference(
                            *[{d['tuijian_id'] for d in ls} for ls in [gexinhua_result_value, guize_data_not_china]])
                        gexinhua_result_value_guolv_all = [d for d in gexinhua_result_value if
                                                           d['tuijian_id'] in gexinhua_result_value_guolv]
                        gexinhua_result_value_guolv_new = set.difference(
                            *[{d['tuijian'] for d in ls} for ls in [gexinhua_result_value_guolv_all, guize_data_china]])
                        gexinhua_data_guolv = [d for d in gexinhua_result_value_guolv_all if
                                               d['tuijian'] in gexinhua_result_value_guolv_new]
                        if gexinhua_data_guolv:
                            gexinhua_zhiding = result_data_jiegou + gexinhua_data_guolv
                            gexinhua_zhiding_length = len(gexinhua_zhiding)
                            if int(self.limit) > gexinhua_zhiding_length:
                                rules_not_zhiding_args = [Config, "zhiding_not_zhiding_key" + self.sep + self.user_id,
                                                          rules_not_zhiding_sql, self.dingyue_apphot_history_key_time]
                                tuijian_list_not_zhiding_result = yield gen.Task(one_key_concern,
                                                                                 *rules_not_zhiding_args)
                                tuijian_list_not_zhiding_result = map(lambda s: s + self.sep + "0",
                                                                      tuijian_list_not_zhiding_result) if tuijian_list_not_zhiding_result else []
                                # print tuijian_list_not_zhiding_result
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
                                    result_data_not_zhding_data = map(
                                        lambda k: (
                                            k.get("tuijian"), k.get("tuijian_id"), k.get('tuijian_type'),
                                            k.get("data_type")),
                                        result_data_not_zhding)
                                    result_data_not_zhiding_jiegou = [dict(zip(key, i)) for i in
                                                                      result_data_not_zhding_data]
                                    length = int(6) - int(gexinhua_zhiding_length)
                                    not_zhiding_data = result_data_not_zhiding_jiegou[0:length]
                                    zhiding_not_zhiding_jiegou = gexinhua_zhiding + not_zhiding_data
                                    row = {"row_data": zhiding_not_zhiding_jiegou}
                                    self.op_json_data(data=row)
                                else:
                                    tuijian_zhiding_result_value_data = gexinhua_zhiding[0:gexinhua_zhiding_length]
                                    row = {"row_data": tuijian_zhiding_result_value_data}
                                    self.op_json_data(data=row)
                            else:
                                tuijian_zhiding_result_value_data = gexinhua_zhiding[0:6]
                                row = {"row_data": tuijian_zhiding_result_value_data}
                                self.op_json_data(data=row)
                        else:
                            rules_not_zhiding_args = [Config, "zhiding_not_zhiding_key" + self.sep + self.user_id,
                                                      rules_not_zhiding_sql, self.dingyue_apphot_history_key_time]
                            tuijian_list_not_zhiding_result = yield gen.Task(one_key_concern, *rules_not_zhiding_args)
                            tuijian_list_not_zhiding_result = map(lambda s: s + self.sep + "0",
                                                                  tuijian_list_not_zhiding_result) if tuijian_list_not_zhiding_result else []
                            # print tuijian_list_not_zhiding_result
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
                                result_data_not_zhding_data = map(
                                    lambda k: (
                                        k.get("tuijian"), k.get("tuijian_id"), k.get('tuijian_type'),
                                        k.get("data_type")),
                                    result_data_not_zhding)
                                result_data_not_zhiding_jiegou = [dict(zip(key, i)) for i in
                                                                  result_data_not_zhding_data]
                                length = int(6) - int(Not_Indiv_count)
                                not_zhiding_data = result_data_not_zhiding_jiegou[0:length]
                                zhiding_not_zhiding_jiegou = result_data_jiegou + not_zhiding_data
                                row = {"row_data": zhiding_not_zhiding_jiegou}
                                self.op_json_data(data=row)
                            else:
                                row = {"row_data": result_data_jiegou}
                                self.op_json_data(data=row)
                    else:
                        tuijian_zhiding_result_value_data = result_data_jiegou[0:6]
                        row = {"row_data": tuijian_zhiding_result_value_data}
                        self.op_json_data(data=row)
                else:
                    gexinhua_args = [Config, self.user_attention_page_user_key, guanzhu_sql,
                                     self.dingyue_apphot_history_key_time]
                    gexinhua_result = yield gen.Task(one_key_concern_158, *gexinhua_args)
                    gexinhua_result = map(lambda x: x + self.sep + "1", gexinhua_result) if gexinhua_result else []
                    # tuijian_result_count = len(gexinhua_result)
                    gexinhua_result_value = map(lambda x: dict(zip(key, x.split("_"))), gexinhua_result)
                    # print "gexinhua_result_value ", gexinhua_result_value
                    gexinhua_result_value_guolv = set.difference(
                        *[{d['tuijian_id'] for d in ls} for ls in [gexinhua_result_value, guize_data_not_china]])
                    gexinhua_result_value_guolv_all = [d for d in gexinhua_result_value if
                                                       d['tuijian_id'] in gexinhua_result_value_guolv]
                    gexinhua_result_value_guolv_new = set.difference(
                        *[{d['tuijian'] for d in ls} for ls in [gexinhua_result_value_guolv_all, guize_data_china]])
                    gexinhua_data_guolv = [d for d in gexinhua_result_value_guolv_all if
                                           d['tuijian'] in gexinhua_result_value_guolv_new]
                    if gexinhua_data_guolv:
                        gexinhua_data_guolv_length = len(gexinhua_data_guolv)
                        if int(self.limit) > gexinhua_data_guolv_length:
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

                            tuijian_list_resulnot_zhding = set.difference(*[{d['tuijian_id'] for d in ls} for ls in
                                                                            [tuijian_list_result_value,
                                                                             guize_data_not_china]])
                            rules_result_newnot_zhding = [d for d in tuijian_list_result_value if
                                                          d['tuijian_id'] in tuijian_list_resulnot_zhding]
                            rules_result_allnot_zhding = set.difference(
                                *[{d['tuijian'] for d in ls} for ls in [rules_result_newnot_zhding, guize_data_china]])
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
                                result_data_not_zhiding_jiegou_length = len(result_data_not_zhiding_jiegou)
                                limit_length = int(6) - result_data_not_zhiding_jiegou_length
                                not_zhid_a = result_data_not_zhiding_jiegou[0:limit_length]
                                gexinhua_not_zhiding = gexinhua_data_guolv + not_zhid_a
                                row = {"row_data": gexinhua_not_zhiding}
                                self.op_json_data(data=row)
                            else:
                                row = {"row_data": gexinhua_data_guolv}
                                self.op_json_data(data=row)
                        else:
                            gexinhua = gexinhua_data_guolv[0:6]
                            row = {"row_data": gexinhua}
                            self.op_json_data(data=row)
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
                        tuijian_list_resulnot_zhding = set.difference(*[{d['tuijian_id'] for d in ls} for ls in
                                                                        [tuijian_list_result_value,
                                                                         guize_data_not_china]])
                        rules_result_newnot_zhding = [d for d in tuijian_list_result_value if
                                                      d['tuijian_id'] in tuijian_list_resulnot_zhding]
                        rules_result_allnot_zhding = set.difference(
                            *[{d['tuijian'] for d in ls} for ls in [rules_result_newnot_zhding, guize_data_china]])
                        result_data_not_zhding = [d for d in rules_result_newnot_zhding if
                                                  d['tuijian'] in rules_result_allnot_zhding]
                        if tuijian_list_not_zhiding_result:
                            result_data_not_zhding_data = map(
                                lambda k: (
                                    k.get("tuijian"), k.get("tuijian_id"), k.get('tuijian_type'), k.get("data_type")),
                                result_data_not_zhding)
                            result_data_not_zhiding_jiegou = [dict(zip(key, i)) for i in result_data_not_zhding_data]
                            not_zhid_a = result_data_not_zhiding_jiegou[0:6]
                            self.op_json_data(data=not_zhid_a)
            else:
                tuijian_zhiding = [Config, self.one_key_concern_zhiding_key, rules_zhiding_baike_sql,
                                   self.dingyue_apphot_history_key_time]
                t2 = time.clock()
                tuijian_zhiding_result = yield gen.Task(one_key_concern, *tuijian_zhiding)
                # print "tuijian_zhiding_result", tuijian_zhiding_result
                tuijian_zhiding_result = map(lambda s: s + self.sep + "0",
                                             tuijian_zhiding_result) if tuijian_zhiding_result else []
                # print Not_Individualization
                access_log.debug("dingyue quecdry res time: %.2f s" % (t2 - t1))
                tuijian_zhiding_result_value = map(lambda x: dict(zip(key, x.split("_"))), tuijian_zhiding_result)
                # print "tuijian_zhiding_result_value" , tuijian_zhiding_result_value
                # print "tuijian_zhiding_result_value", tuijian_zhiding_result_value
                if tuijian_zhiding_result_value:
                    Not_Indiv_count = len(tuijian_zhiding_result_value)
                    if self.limit > Not_Indiv_count:
                        # print "a"
                        tuijian_not_zhiding_args = [Config, self.one_key_concern_not_zhiding_key,
                                                    rules_not_zhiding_baike_sql, self.dingyue_apphot_history_key_time]
                        tuijian_not_zhiding_result = yield gen.Task(one_key_concern, *tuijian_not_zhiding_args)
                        tuijian_not_zhiding_result = map(lambda s: s + self.sep + "0",
                                                         tuijian_not_zhiding_result) if tuijian_not_zhiding_result else []
                        t3 = time.clock()
                        access_log.debug("dingyue query res time: %.2f s" % (t3 - t2))
                        not_zhiding = map(lambda x: dict(zip(key, x.split("_"))), tuijian_not_zhiding_result)
                        # print not_zhiding
                        length = int(15) - int(Not_Indiv_count)
                        not_zhiding_result = not_zhiding[0:length]
                        result = tuijian_zhiding_result_value + not_zhiding_result
                        # result_value = map(lambda x: dict(zip(key, x.split("_"))), result)
                        row = {"row_data": result}
                        self.op_json_data(data=row)
                    else:
                        # print "b"
                        tuijian_zhiding_result_value_new_data = tuijian_zhiding_result_value[0:15]
                        row = {"row_data": tuijian_zhiding_result_value_new_data}
                        self.op_json_data(data=row)
                else:
                    tuijian_not_zhiding_args = [Config, self.one_key_concern_not_zhiding_key,
                                                rules_not_zhiding_baike_sql,
                                                self.offset, self.limit, self.dingyue_apphot_history_key_time]
                    tuijian_not_zhiding_result = yield gen.Task(select_data, *tuijian_not_zhiding_args)
                    tuijian_not_zhiding_result = map(lambda s: s + self.sep + "0",
                                                     tuijian_not_zhiding_result) if tuijian_not_zhiding_result else []
                    t3 = time.clock()
                    access_log.debug("dingyue query res time: %.2f s" % (t3 - t2))
                    result_value = map(lambda x: dict(zip(key, x.split("_"))), tuijian_not_zhiding_result)
 		    result_value_new_data = result_value[0:15]
                    row = {"row_data": result_value_new_data}
                    self.op_json_data(data=row)

    def op_json_data(self, data):
        out_str = json.dumps(self.correct_json(data), ensure_ascii=False)
        self.set_header("Content-Type", "application/json; charset=utf-8")
        self.write(out_str)

    def error_json(self, error_msg):
        return {"error_code": "1", "data": "0", "error_msg": error_msg}

    def correct_json(self, data):
        return {"data": data}

    def write_error(self, status_code, **kwargs):
        self.set_status(status_code)


if __name__ == "__main__":
    # Business(Config)
    pass
