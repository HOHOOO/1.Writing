# coding=utf-8

import json
import urllib
import urllib2
from datetime import datetime, timedelta

import tornado
from tornado import web, gen
from tornado.log import gen_log
import tornado.httpclient
from tornado.log import access_log

from base.basehandler import BaseHandler
from base.config import Config
from util.mysql import TorMysqlClient
from util.redis_client import RedisClient
from comm.consts import *
from biz.home_article_b import HomeArticleB
from biz.home_article_tools import HomeArticleTools


class EditorExcellenceHandler(BaseHandler):
    """
        http://twiki.team.bq.com:8081/twiki/Main/Homepage_api_homepage_data_showlist?sortcol=0;table=1;up=0#sorted_table
        article_ids： article_id字符串 否 默认-''（当get_by=article_ids时查询特定article_id 例：123,134,135）
        channel_name： 频道名称  否 默认-''取全部频道（频道名称：home=>0,youhui=>3,,news=>6,pingce=>8(取众测数据),qingdan=>9,zhuanti=>10,yuanchuang=>11,post=>11,wiki=>12,dianping=>13,wiki_topic=>14,quan=>15,duihuan=>15,2=>16,second=>16）
        get_by： 查询方式  否 默认-page,（page通过分页方式查找，timesort通过timesort查找,article_ids通过一个或多个特定article_id查找）
        not_channel_names： 屏蔽频道名称  否 默认-''不屏蔽频道（如：not_channel_names= home,youhui,faxian 则不会查询这些频道数据）
        nums： 本次查询总数  否 默认-10
        page： 分页  否 默认-1（get_by方式为page时填写）
        timesort： 时间戳 否 默认-time()（get_by方式为timesort时填写）
        top_nums： 本次查询包含头条数 否 默认-0
        with_content： 是否聚合内容表示  否 默认0 只显示id / 1 显示各个频道聚合的内容
        with_top： 本次查询是否包含头条  否 默认-0（0不包含，1包含，2所有都显示）
        device_id： 设备id 是 通过设备id查询用户的偏好等信息
        smzdm_id： 用户id 否 通过用户id查询用户的偏好等信息
    """
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        # 返回值
        result = {
            "error_code": 0,
            "error_msg": '',
            "data": [],
            "top_data": [],
        }
        
        # 获取参数
        # 没有设备id，不处理
        device_id = urllib.unquote(self.get_argument("device_id", '', False))
        if not device_id:
            result["error_code"] = 1
            result["error_msg"] = "device_id is None"
            self.jsonify(result)
            return
        
        # device_id 转换
        device_id = device_id.replace(" ", "+")

        article_ids = self.get_argument("article_ids", '')
        channel_name = self.get_argument("channel_name", '')
        get_by = self.get_argument("get_by", 'page')
        not_channel_names = self.get_argument("not_channel_names", '')
        nums = int(self.get_argument("nums", PAGE_TOTAL_SIZE))
        page = int(self.get_argument("page", 1))
        timesort = self.get_argument("timesort", datetime.now())
        top_nums = int(self.get_argument("top_nums", 0))
        with_content = int(self.get_argument("with_content", 0))
        with_top = int(self.get_argument("with_top", 0))
        smzdm_id = self.get_argument("user_id", '')
        shunt = self.get_argument("shunt", 'shunt_test')
        ab_test_type = self.get_argument("ab_test_type", "")
        
        redis_conn = yield RedisClient().get_redis_client()
        # 判断此设备是否测试设备
        # 是: 请求测试环境代码
        # 否: 请求线上环境代码
        key = "%s:%s" % (PREFER_SHUNT_DEVICE_KEY, device_id)
        if redis_conn.exists(key) and shunt == "shunt_test":
            try:
                body_dict = {
                    "device_id": device_id,
                    "nums": nums,
                    "page": page,
                    "with_top": with_top,
                    "user_id": smzdm_id,
                    "shunt": ""
                }
                url_data = urllib.urlencode(body_dict)
                url = Config["shunt.url"] + url_data.encode('utf-8')
                gen_log.info("device (%s) shunt to test env(url: %s)", device_id, url)
                request = urllib2.Request(url)
                response = urllib2.urlopen(request)
                shunt_result = response.read()
                gen_log.info("shunt device (%s) result len: %s", device_id, len(shunt_result))
            except Exception as e:
                shunt_result = str(e)
            self.jsonify(shunt_result)
            return

        ha = HomeArticleB(redis_conn, ab_test_type, device_id, smzdm_id, page, nums)
        result["data"] = yield ha.get_home_article_list()
        if int(with_top) == 1:
            now = datetime.now()
            start_time = (now - timedelta(days=365)).strftime("%Y-%m-%d %H:%M:%S")
            end_time = now.strftime("%Y-%m-%d %H:%M:%S")
            result["top_data"] = yield ha.get_top_article(start_time, end_time)
            gen_log.info("result top data: %s", result["top_data"])
        gen_log.info("device_id: %s, page: %s, nums: %s, smzdm_id: %s, ab_test_type: %s, result data size: %s",
                     device_id, page, nums, smzdm_id, ab_test_type, len(result))
        self.jsonify(result)
        

class EditorExcellenceToolsHandler(BaseHandler):
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        """
        action:
            editor: 原始小编数据
            editor_sort： 排序后的小编和推荐数据
            dislike: 不感兴趣文章
            prefer: 偏好
            
        version:
            a: a版本
            b: b版本
        device_id: 设备id
        smzdm_id: 用户id

        :return:
        """
        # 返回值
        result = {
            "data": [],
            "top_data": [],
        }
        
        # 获取参数
        # 没有设备id，不处理
        device_id = urllib.unquote(self.get_argument("device_id", '', False))
        if not device_id:
            result = {
                u"请求参数": {
                    u"action【必传】" : {
                        u"editor": u"原始小编数据",
                        u"editor_sort": u"排序后的小编数据",
                        u"editor_redis": u"排序后的小编推荐数据(线下除非白名单用户)",
                        u"dislike": u"不感兴趣文章",
                        u"prefer_user": u"用户偏好",
                        u"prefer_device": u"设备偏好",
                        u"shunt_add_device": u"增加测试设备",
                        u"shunt_cancel_device": u"取消测试设备",
                    },
                    u"device_id": u"设备ID【必传】 ",
                    u"user_id": u"用户ID【非必传】",
                    u"with_top": u"为1，则获取置顶置顶数据，否则不处理置顶数据",
                    u"timeout": u"失效时间,单位(秒)【非必传，默认60分钟】",
                    u"weight【非必传】": {
                        u"EDITOR_SYNC_TOTAL_WEIGHT": u"编辑同步到首页",
                        u"TIME_WEIGHT[TOTAL]": u"时间基础权值总占比",
                        u"TIME_WEIGHT[HALF_HOUR]": u"时间基础权值半小时",
                        u"TIME_WEIGHT[HOUR_1]": u"时间基础权值1小时",
                        u"TIME_WEIGHT[HOUR_3]": u"时间基础权值3小时",
                        u"TIME_WEIGHT[HOUR_12]": u"时间基础权值12小时",
                        u"PORTRAIT_WIGHT[TOTAL]": u"画像基础权值总占比",
                        u"PORTRAIT_WIGHT[BRAND]": u"画像基础权值品牌占比",
                        u"PORTRAIT_WIGHT[ACCURATE_CATE]": u"画像基础权值品类精确偏好占比",
                        u"PORTRAIT_WIGHT[BLUR_CATE]": u"画像基础权值品类模糊偏好占比",
                        u"PORTRAIT_WIGHT[TAG]": u"画像基础权值标签占比",
                        u"THRESHOLD": u"过滤阀值",
                        u"默认值": {
                            "EDITOR_SYNC_TOTAL_WEIGHT": 2.0,
                            "TIME_WEIGHT": {"TOTAL": 4.0,
                                            "HALF_HOUR": 4.0,
                                            "HOUR_1": 3.2,
                                            "HOUR_3": 2.4,
                                            "HOUR_12": 1.6,
                                            "HOUR_24": 0.8
                                            },
                            "PORTRAIT_WIGHT": {"TOTAL": 4.0,
                                               "BRAND": 0.8,
                                               "ACCURATE_CATE": 0.8,
                                               "BLUR_CATE": 1.6,
                                               "TAG": 0.8
                                               },
                            "THRESHOLD": 0.0
                        }
                    
                    }
                },
                u"配置host": [
                    u"120.132.70.128 recommend-itoamms.smzdm.com(offline)",
                    u"106.75.109.195 system-recommend.smzdm.com(online)"
                ],
                u"offline请求地址举例": {
                    u"帮助": """https://recommend-itoamms.smzdm.com/recommend_system/tools/""",
                    u"小编数据": "https://recommend-itoamms.smzdm.com/recommend_system/tools/?device_id=nkk4CzScYwfYMRk2qv6hCgDOFE2CRzCOwAfulukRgoRnxkwz7Adw3g==&with_top=1&action=editor",
                    u"加权排序小编数据": """https://recommend-itoamms.smzdm.com/recommend_system/tools/?device_id=nkk4CzScYwfYMRk2qv6hCgDOFE2CRzCOwAfulukRgoRnxkwz7Adw3g==&with_top=1&action=editor_sort&weight={
                                            "EDITOR_SYNC_TOTAL_WEIGHT": 2.0,
                                            "TIME_WEIGHT": {"TOTAL": 4.0,
                                                            "HALF_HOUR": 4.0,
                                                            "HOUR_1": 3.2,
                                                            "HOUR_3": 2.4,
                                                            "HOUR_12": 1.6,
                                                            "HOUR_24": 0.8
                                                            },
                                            "PORTRAIT_WIGHT": {"TOTAL": 4.0,
                                                            "BRAND": 0.8,
                                                            "ACCURATE_CATE": 0.8,
                                                            "BLUR_CATE": 1.6,
                                                            "TAG": 0.8
                                                            },
                                            "THRESHOLD": 0.0
                                        }""",
                    u"缓存中数据": "https://recommend-itoamms.smzdm.com/recommend_system/tools/?device_id=nkk4CzScYwfYMRk2qv6hCgDOFE2CRzCOwAfulukRgoRnxkwz7Adw3g==&with_top=1&action=editor_redis",
                    u"不感兴趣文章": "https://recommend-itoamms.smzdm.com/recommend_system/tools/?device_id=nkk4CzScYwfYMRk2qv6hCgDOFE2CRzCOwAfulukRgoRnxkwz7Adw3g==&action=dislike",
                    u"用户偏好": "https://recommend-itoamms.smzdm.com/recommend_system/tools/?device_id=nkk4CzScYwfYMRk2qv6hCgDOFE2CRzCOwAfulukRgoRnxkwz7Adw3g==&action=prefer_user",
                    u"设备偏好": "https://recommend-itoamms.smzdm.com/recommend_system/tools/?device_id=nkk4CzScYwfYMRk2qv6hCgDOFE2CRzCOwAfulukRgoRnxkwz7Adw3g==&action=prefer_device",
                    u"增加测试设备": "https://recommend-itoamms.smzdm.com/recommend_system/tools/?device_id=nkk4CzScYwfYMRk2qv6hCgDOFE2CRzCOwAfulukRgoRnxkwz7Adw3g==&action=shunt_add_device&timeout=3600",
                    u"取消测试设备": "https://recommend-itoamms.smzdm.com/recommend_system/tools/?device_id=nkk4CzScYwfYMRk2qv6hCgDOFE2CRzCOwAfulukRgoRnxkwz7Adw3g==&action=shunt_cancel_device"
        
                },
                u"online请求地址举例": {
                    u"帮助": "https://system-recommend.smzdm.com:809/recommend_system/tools/",
                    u"小编数据": "https://system-recommend.smzdm.com:809/recommend_system/tools/?device_id=nkk4CzScYwfYMRk2qv6hCgDOFE2CRzCOwAfulukRgoRnxkwz7Adw3g==&with_top=1&action=editor",
                    u"加权排序小编数据": """https://system-recommend.smzdm.com:809/recommend_system/tools/?device_id=nkk4CzScYwfYMRk2qv6hCgDOFE2CRzCOwAfulukRgoRnxkwz7Adw3g==&with_top=1&action=editor_sort&weight={
                                                        "EDITOR_SYNC_TOTAL_WEIGHT": 2.0,
                                                        "TIME_WEIGHT": {"TOTAL": 4.0,
                                                                        "HALF_HOUR": 4.0,
                                                                        "HOUR_1": 3.2,
                                                                        "HOUR_3": 2.4,
                                                                        "HOUR_12": 1.6,
                                                                        "HOUR_24": 0.8
                                                                        },
                                                        "PORTRAIT_WIGHT": {"TOTAL": 4.0,
                                                                        "BRAND": 0.8,
                                                                        "ACCURATE_CATE": 0.8,
                                                                        "BLUR_CATE": 1.6,
                                                                        "TAG": 0.8
                                                                        },
                                                        "THRESHOLD": 0.0
                                                    }""",
                    u"缓存中数据": "https://system-recommend.smzdm.com:809/recommend_system/tools/?device_id=nkk4CzScYwfYMRk2qv6hCgDOFE2CRzCOwAfulukRgoRnxkwz7Adw3g==&with_top=1&action=editor_redis",
                    u"不感兴趣文章": "https://system-recommend.smzdm.com:809/recommend_system/tools/?device_id=nkk4CzScYwfYMRk2qv6hCgDOFE2CRzCOwAfulukRgoRnxkwz7Adw3g==&action=dislike",
                    u"用户偏好": "https://system-recommend.smzdm.com:809/recommend_system/tools/?device_id=nkk4CzScYwfYMRk2qv6hCgDOFE2CRzCOwAfulukRgoRnxkwz7Adw3g==&action=prefer_user",
                    u"设备偏好": "https://system-recommend.smzdm.com:809/recommend_system/tools/?device_id=nkk4CzScYwfYMRk2qv6hCgDOFE2CRzCOwAfulukRgoRnxkwz7Adw3g==&action=prefer_device"
                }
            }
            self.jsonify(result)
            return
        
        # device_id 转换
        device_id = device_id.replace(" ", "+")
        smzdm_id = self.get_argument("user_id", '')
        action = self.get_argument("action", ACTION_EDITOR)
        weight = urllib.unquote(self.get_argument("weight", "{}"))
        timeout = int(self.get_argument("timeout", 3600))
        gen_log.info("input weight: %s", weight)
        weight = json.loads(weight)
        gen_log.info("device_id: %s, smzdm_id: %s", device_id, smzdm_id)
        gen_log.info("weight: %s", weight)

        redis_conn = yield RedisClient().get_redis_client()

        ha = HomeArticleTools(redis_conn, action, device_id, smzdm_id)

        if action == ACTION_EDITOR:
            result["data"] = yield ha.get_ori_editor_article_list()
            result["top_data"] = yield ha.get_top_article()
        elif action == ACTION_EDITOR_REDIS:
            result["data"] = yield ha.get_sort_or_mem_editor_article_list(ACTION_EDITOR_REDIS)
            result["top_data"] = yield ha.get_top_article()
        elif action == ACTION_EDITOR_SORT:
            result["data"] = yield ha.get_sort_or_mem_editor_article_list(ACTION_EDITOR_SORT, weight)
            result["top_data"] = yield ha.get_top_article()
        elif action == ACTION_DISLIKE:
            # 不喜欢,不区分A/B版本
            result["data"] = yield ha.get_dislike_article_list()
        elif action == ACTION_PREFER_USER:
            # 偏好,不区分A/B版本
            result["data"] = yield ha.get_user_prefer_info()
        elif action == ACTION_PREFER_DEVICE:
            # 偏好,不区分A/B版本
            result["data"] = yield ha.get_user_prefer_info()
        elif action == ACTION_SHUNT_ADD_DEVICE:
            result["data"] = yield ha.shunt_add_device(timeout)
        elif action == ACTION_SHUNT_CANCEL_DEVICE:
            result["data"] = yield ha.shunt_cancel_device()
        
        # gen_log.debug("return final result data: %s", result)
        self.jsonify(result)
        

class FeedBackHandler(BaseHandler):
    """
    curl -s -X POST 'http://localhost:8814/recommend_system/feedback/?device_id=46fe99ff7a76787d03086b44251ae7f3zzz&user_id=test_user&authenticity=0&data=%7B%22channel_id%22%3A1%2C+%22article_id%22%3A+123%2C+%22article_channel_name%22%3A+%22%E4%BC%98%E6%83%A0%22%2C+%22cate%22%3A+%22%E6%89%8B%E6%9C%BA%2C%E7%94%B5%E8%84%91%E6%95%B0%E7%A0%81%22%2C+%22brand%22%3A%22%E5%8D%8E%E4%B8%BA%22%2C%22tag%22%3A+%22%E7%A7%91%E6%8A%80%22%2C+%22app_version%22%3A+7.8+%7D'

    http://twiki.team.bq.com:8081/twiki/Main/Feed_back
    """

    @web.asynchronous
    @gen.coroutine
    def post(self):
        config = Config.flatten()
        user_dislike_content_table = config.get("mysql_table.user_dislike_content_table")
        user_dislike_content_full_table = config.get("mysql_table.user_dislike_content_full_table")
        smzdm_category_table = config.get("mysql_table.smzdm_category")

        device_id = str(urllib.unquote(self.get_argument("device_id", "", False)).encode("utf-8").replace(" ", "+"))  # 空格替换为+
        user_id = str(self.get_argument("user_id", ""))
        authenticity = str(self.get_argument("authenticity", "1"))
        channel_id = str(self.get_argument("channel_id", "-1"))
        article_id = str(self.get_argument("article_id", ""))
        cate = self.get_argument("cate", "").encode("utf-8")
        brand = str(self.get_argument("brand", "").encode("utf-8"))
        tag = str(self.get_argument("tag", "").encode("utf-8"))
        app_version = str(self.get_argument("app_version", ""))

        if channel_id == "":
            channel_id = "-1"

        if user_id == "0":
            user_id = ""

        msg = {
            "error_code": "0",
            "error_msg": "data",
            "data": "success!"
        }
        access_log.info(
            "device_id:%s,user_id:%s,channel_id:%s,article_id:%s,cate:%s,brand:%s,tag:%s,app_version:%s,authenticity:%s," % (
            device_id, user_id, channel_id, article_id, cate, brand, tag, app_version, authenticity,))

        # 测试探针 
        if authenticity == "0" and device_id == "111":
            self.jsonify(msg)
            return 
        try:
            # 将传来的不感兴趣四级品类转为对应的父三级品类
            cate_arr = cate.split(",")
            cate_str = ",".join(map(lambda x: "'" + x + "'", cate_arr))
            transform_cate4_to_cate3_sql = """
            select
            case title
            when level_1 then level_1
            when level_2 then level_2
            when level_3 then level_3
            else level_3 end as t
            from %s
            where title in (%s) and is_deleted=0;
            """ % (smzdm_category_table, cate_str.decode("utf-8"))
            res = yield TorMysqlClient().fetchall(transform_cate4_to_cate3_sql)
            trans_cate = ",".join([k[0] for k in res])

            insert_trans_sql = """
                            insert into {table_name} (device_id, user_id, channel_id, article_id, cate, brand, tag, app_version, authenticity)
                            values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """.format(table_name=user_dislike_content_table)
            data_tuples = (
                device_id, user_id, channel_id, article_id, trans_cate, brand, tag, app_version, authenticity)
            res = yield TorMysqlClient().execute(insert_trans_sql, data_tuples)
            access_log.debug(res)

            insert_original_sql = """
                insert into {table_name} (device_id, user_id, channel_id, article_id, cate, brand, tag, app_version, authenticity)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """.format(table_name=user_dislike_content_full_table)
            data_tuples = (
                device_id, user_id, channel_id, article_id, cate, brand, tag, app_version,authenticity)

            res = yield TorMysqlClient().execute(insert_original_sql, data_tuples)
            access_log.debug(res)
            redis_client = yield RedisClient().get_redis_client()
            key = ":".join(("dislike", user_id, device_id))
            content = ":".join((article_id, channel_id))
            redis_client.rpush(key, content)
            redis_client.expire(key, 3 * 24 * 60 * 60)

        except Exception as e:
            import traceback
            access_log.error(traceback.format_exc())
            msg["error_code"] = "1"
            msg["error_msg"] = str(e)
            msg["data"] = "0"
        self.jsonify(msg)


