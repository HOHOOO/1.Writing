# coding=utf-8
import datetime

from base.consts import youhui


class Backend(object):
    """
    后端服务类
    """
    def __init__(self, config):
        self.config = config

        self.id_placeholder = config["redis_key.id_placeholder"]
        self.type_placeholder = config["redis_key.type_placeholder"]

        self.youhui_similary_article_key_format = config["redis_key.youhui_similary_article_key_format"]
        self.association_rule_article_key_format_yh = config["redis_key.association_rule_article_key_format_yh"]
        self.association_rule_article_key_format_yc = config["redis_key.association_rule_article_key_format_yc"]

        self.youhui_similary_article_key_time = config["redis_key_time.youhui_similary_article_key_time"]
        self.youhui_similary_history_article_key_time = config["redis_key_time.youhui_similary_history_article_key_time"]
        self.association_rule_article_key_time_yh = config["redis_key_time.association_rule_article_key_time_yh"]
        self.association_rule_article_key_time_yc = config["redis_key_time.association_rule_article_key_time_yc"]

        self.youhui_primary_table = config["mysql_table.youhui_primary_table"]
        self.youhui_similary_table = config["mysql_table.youhui_similary_table"]
        self.association_rule_table = config["mysql_table.association_rule_table"]
        self.youhui_similary_combine_table = config["mysql_table.youhui_similary_conbine_table"]

        self.youhui_module_name = config["youhui.module_name"]
        self.youhui_simi_yh_name = config["youhui.simi_yh_name"]
        self.youhui_asso_yh_name = config["youhui.asso_yh_name"]
        self.youhui_asso_yc_name = config["youhui.asso_yc_name"]

        self.yuanchuang_module_name = config["yuanchuang.module_name"]
        self.yuanchuang_asso_yc_name = config["yuanchuang.asso_yc_name"]
        self.yuanchuang_asso_yh_name = config["yuanchuang.asso_yh_name"]

        self.trace_id_map_file_prefix = config["file.trace_id_map_file_prefix"]

        self.insert_youhui_similary_combine_base_sql = """
                insert into {table_name} (src_article_id, rec_article_id, score_sum, score_pro, score_cate4, score_cate3,
                score_cate2, score_cate1, score_brand, score_title, score_sex, score_crowd, score_heat, score_simi, model_version)
                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """.format(table_name=self.youhui_similary_combine_table)

        self.insert_youhui_similary_base_sql = """
                insert into {table_name} (src_article_id, rec_article_id, score_sum, score_pro, score_cate4, score_cate3,
                score_cate2, score_cate1, score_brand, score_title, score_sex, score_crowd)
                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """.format(table_name=self.youhui_similary_table)

        self.query_newest_article_base_sql = """
                select id as article_id, pubdate, channel, mall, mall_id, brand, brand_id, digital_price,
                yh_type, pro_id, sum_collect_comment, level_1_id, level_1, level_2_id, level_2, level_3_id,
                level_3, level_4_id, level_4, title, subtitle
                from {table_name}
                where level_1_id=4811 and pubdate>date_add(now(), interval -7 day) and stock_status=0
                union
                select id as article_id, pubdate, channel, mall, mall_id, brand, brand_id, digital_price,
                yh_type, pro_id, sum_collect_comment, level_1_id, level_1, level_2_id, level_2, level_3_id,
                level_3, level_4_id, level_4, title, subtitle
                from {table_name}
                where pubdate>date_add(now(), interval -1 day) and level_1_id<>1523 and stock_status=0
                and channel in (1,2,5)
                order by pubdate desc limit 6500
            """.format(table_name=self.youhui_primary_table)

        self.query_spec_and_newest_article_base_sql = """
                select id as article_id, pubdate, channel, mall, mall_id, brand, brand_id, digital_price,
                yh_type, pro_id, sum_collect_comment, level_1_id, level_1, level_2_id, level_2, level_3_id,
                level_3, level_4_id, level_4, title, subtitle
                from {table_name}
                where id=%s
                union
                select id as article_id, pubdate, channel, mall, mall_id, brand, brand_id, digital_price,
                yh_type, pro_id, sum_collect_comment, level_1_id, level_1, level_2_id, level_2, level_3_id,
                level_3, level_4_id, level_4, title, subtitle
                from {table_name}
                where pubdate>date_add(now(), interval -1 day)
                and channel in (1,2,5) and stock_status=0
                order by pubdate desc limit 500
                """.format(table_name=self.youhui_primary_table)

        self.youhui_similary_combine_base_sql = """
                select rec_article_id, score_sum, score_simi, score_heat, score_pro, score_cate4, score_cate3,
                score_cate2, score_cate1, score_brand, score_title, score_sex, score_crowd
                from {table_name}
                where src_article_id=%s and ctime>date_add(now(), interval -1 day) and model_version='%s'
                order by score_sum desc limit 50
                """.format(table_name=self.youhui_similary_combine_table)

        # self.youhui_similary_base_sql = """
        #         select rec_article_id, score_sum, score_pro, score_cate4, score_cate3,
        #         score_cate2, score_cate1, score_brand, score_title, score_sex, score_crowd
        #         from {table_name}
        #         where src_article_id=%s and ctime>date_add(now(), interval -1 day)
        #         order by score_sum desc limit 10
        #         """.format(table_name=self.youhui_similary_table)

        self.association_rule_base_yh_sql = """
                select new_article_id, new_channel_id, rs_id2, rs_id3
                from {table_name}
                where old_article_id=%s and new_channel_id=1
                and score>0.005 and new_article_time>date_add(now(), interval -2 day)
                order by score desc limit 50
                """.format(table_name=self.association_rule_table)

        self.association_rule_base_yc_sql = """
                select new_article_id, new_channel_id, rs_id2, rs_id3
                from {table_name}
                where old_article_id=%s and new_channel_id=11
                and score>0.005
                order by score desc limit 50
                """.format(table_name=self.association_rule_table)

        # self.result_fields = ["article_id", "article_channel_id",
        #                       "rs_id2", "rs_id3", "rs_id4", "rs_id1", "rs_id5"]

    def get_trace_id_map_file(self):
        ymd = datetime.datetime.now().strftime("%Y%m%d")
        trace_id_map_file = self.trace_id_map_file_prefix + "." + ymd
        return trace_id_map_file

    def get_youhui_similary_article_key(self, youhui_article_id, model_version, channel_id="1"):
        """
        :param youhui_article_id: 优惠文章id
        :param channel_id: 该文章所在频道
        :return: 返回redis中根据优惠文章相似度模型计算出的该优惠文章的推荐结果的key
        """
        if channel_id in youhui:
            return ".".join((self.youhui_similary_article_key_format.replace(self.id_placeholder, youhui_article_id), model_version))

    def get_query_spec_and_newest_article_sql(self, youhui_article_id, channel_id):
        """
        :param youhui_article_id: 优惠文章id
        :param channel_id: 该文章所在频道
        :return: 返回查询指定文章以及最新的一些文章，用于根据基于相似度的模型进行计算该文章的推荐结果
        """
        if channel_id in youhui:
            return self.query_spec_and_newest_article_base_sql % youhui_article_id

    def get_youhui_similary_combine_article_sql(self, youhui_article_id, channel_id, model_version):
        """
        :param youhui_article_id: 优惠文章id
        :param channel_id: 该文章所在频道
        :return: 返回在mysql中查询 根据优惠文章相似度模型计算出的该优惠文章的推荐结果的sql
        """
        if channel_id in youhui:
            return self.youhui_similary_combine_base_sql % (youhui_article_id, model_version)

    def get_association_rule_article_key(self, article_id, channel_id, flag="youhui"):
        """
        :param article_id: 文章id
        :param channel_id: 该文章所在的频道id
        :param flag: 该文章推荐结果的文章类型
        :return: 返回redis中根据关联规则模型计算出的该文章的推荐结果的key
        """
        article_type = "yh" if channel_id in youhui else "yc"

        if flag not in ("youhui", "yuanchuang"):
            raise TypeError("flag must is 'youhui' or 'yuanchuang'!")

        if flag == "youhui":
            return self.association_rule_article_key_format_yh.\
                replace(self.type_placeholder, article_type).\
                replace(self.id_placeholder, article_id)
        else:
            return self.association_rule_article_key_format_yc. \
                replace(self.type_placeholder, article_type). \
                replace(self.id_placeholder, article_id)

    def get_association_rule_article_sql(self, article_id, flag="youhui"):
        """
        :param article_id: 文章id
        :param flag: 该文章推荐结果的文章类型
        :return: 返回在mysql中查询 根据关联规则模型计算出的该文章的推荐结果的sql
        """
        if flag not in ("youhui", "yuanchuang"):
            raise TypeError("flag must is 'youhui' or 'yuanchuang'!")

        if flag == "youhui":
            return self.association_rule_base_yh_sql % article_id
        else:
            return self.association_rule_base_yc_sql % article_id

    def get_youhui_similary_article_key_time(self):
        """
        :return: 返回redis中根据优惠文章相似度模型计算出的该优惠文章的推荐结果的key的过期时间
        """
        return self.youhui_similary_article_key_time

    def get_youhui_similary_history_article_key_time(self):
        """
        :return: 返回redis中根据优惠文章相似度模型计算出的历史优惠文章的推荐结果的key的过期时间
        """
        return self.youhui_similary_history_article_key_time

    def get_association_rule_article_key_time(self, flag="youhui"):
        """
        :param flag: 推荐结果的文章类型
        :return: 返回redis中根据关联规则模型计算出的该文章的推荐结果的key的过期时间
        """
        if flag not in ("youhui", "yuanchuang"):
            raise TypeError("flag must is 'youhui' or 'yuanchuang'!")
        if flag == "youhui":
            return self.association_rule_article_key_time_yh
        else:
            return self.association_rule_article_key_time_yc

    def get_from_now_to_dawn_time(self):
        """
        返回从现在到凌晨的时间
        :return:
        """
        current_datetime = datetime.datetime.now()
        cate_datetime = current_datetime + datetime.timedelta(days=1)
        cate_datetime = cate_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
        delta = cate_datetime - current_datetime  # 当前时间至明天凌晨的时间差
        return int(delta.total_seconds())
