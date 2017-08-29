# coding=utf-8
import datetime


# from base.consts import youhui


class Backend(object):
    """
    后端服务类
    """

    def __init__(self, config):
        self.config = config

        # 模块名
        self.recommend_name = config["guanzhu.recommend_name"]
        self.backstage_name = config["guanzhu.backstage_name"]
        self.recommend_backstage_name = config["guanzhu.recommend_backstage_name"]
        # json key
        self.tuijian_key = config["redis_key.tuijian_key"]
        self.tuijian_id_key = config["redis_key.tuijian_id_key"]
        self.tuijian_type_key = config["redis_key.tuijian_type_key"]
        self.data_type_key = config["redis_key.data_type_key"]
        # 关注动态key
        self.user_attention_page_user_key = config["redis_key.user_attention_page_user_key"]
        self.user_attention_page_not_user_key = config["redis_key.user_attention_page_not_user_key"]
        # 新增关注key
        self.new_attention_page_user_key = config["redis_key.new_attention_page_user_key"]
        self.new_attention_page_not_user_key = config["redis_key.new_attention_page_not_user_key"]
        # 一键关注key
        self.one_key_concern_zhiding_key = config["redis_key.one_key_concern_zhiding_key"]
        self.one_key_concern_not_zhiding_key = config["redis_key.one_key_concern_not_zhiding_key"]

        self.dingyue_apphot_table = config["mysql_table.dingyue_apphot_table"]
        self.rules_table = config["mysql_table.rules_table"]
        self.user_rules_table = config["mysql_table.user_rules_table"]
        self.dingyue_baike_table = config["mysql_table.dingyue_baike_table"]
        self.follow_relate_table = config["mysql_table.follow_relate_table"]
        self.gexinhua_table = config["mysql_table.gexinhua_table"]

        self.dingyue_apphot_history_key_time = config["redis_key_time.dingyue_apphot_history_key_time"]


    def get_dingyue_apphot_history_key_time(self):
        return self.dingyue_apphot_history_key_time

    # 表名
    def get_dingyue_apphot_table(self):
        return self.dingyue_apphot_table
    def get_dingyue_baikei_table(self):
        return self.dingyue_baike_table
    def get_gexinhua_table(self):
        return self.gexinhua_table
    def get_follow_relate_table(self):
        return self.follow_relate_table

    def get_user_rules_table(self):
        return self.user_rules_table
    def get_rules_table(self):
        return self.rules_table

    # 模块名
    def get_recommend_name(self):
        return self.recommend_name

    def get_backstage_name(self):
        return self.backstage_name

    def get_recommend_backstage_name(self):
        return self.recommend_backstage_name

    # json key
    def get_tuijian_key(self):
        return self.tuijian_key

    def get_tuijian_id_key(self):
        return self.tuijian_id_key

    def get_tuijian_type_key(self):
        return self.tuijian_type_key

    def get_data_type_key(self):
        return self.data_type_key

    # 关注动态key
    def get_user_attention_page_user_key(self):
        return self.user_attention_page_user_key

    def get_user_attention_page_not_user_key(self):
        return self.user_attention_page_not_user_key

    # 新增关注key
    def get_new_attention_page_user_key(self):
        return self.new_attention_page_user_key

    def get_new_attention_page_not_user_key(self):
        return self.new_attention_page_not_user_key

    # 一键关注key
    def get_one_key_concern_zhiding_key(self):
        return self.one_key_concern_zhiding_key

    def get_one_key_concern_not_zhiding_key(self):
        return self.one_key_concern_not_zhiding_key
