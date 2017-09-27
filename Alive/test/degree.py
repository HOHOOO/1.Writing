# coding=utf-8
import json
from collections import Counter

from datetime import datetime
from base.config import load
from comm.consts import *
from util.rabbit_mq import RabbitMQPoolClient
from util.redis_client import RedisClient

round

logger = None
USER = "user_id"
DEVICE = "device_id"


# 首页千人千面不感兴趣在线降权程序
class HomeDegree(object):
    def __init__(self, redis_client, mysql_client, rabbit_mq_client, log):
        global logger
        self._redis_client = redis_client
        self._mysql_client = mysql_client
        self._rabbit_mq_client = rabbit_mq_client

        self._req_time_str = None
        self._user_id = None
        self._device_id = None
        self._channel_id = None
        self._article_id = None
        self._cates = None
        self._brands = None
        self._tags = None

        self._zero_time_str = None

        logger = log

    def _query_current_preference(self, base):
        preference_key = PREFER_USER_REDIS_KEY % self._user_id if base == USER \
            else PREFER_DEVICE_REDIS_KEY % self._device_id
        res = self._redis_client.get(preference_key)

        if res:
            self._preference_key = preference_key
            logger.info("redis's preference_key: %s" % preference_key)
            current_preference = json.loads(res)
            self._current_preference = current_preference
            return current_preference
        else:
            # 如果redis中不存在用户偏好值,则返回None
            logger.error("redis's key[%s] not exists!" % preference_key)
            return None




    def _query_dislike_tags(self, base):
        if base == USER:
            dislike_tag_sql = """
                    select group_concat(tag) from {table_name}
                    where user_id='%s'
                    and channel_id=%s
                    and ctime>='%s'
                    and authenticity=1
                    group by user_id
                    """.format(table_name=USER_DISLIKE_CONTENT_TABLE)
            dislike_tag_sql = dislike_tag_sql % (
                self._user_id, self._channel_id, self._zero_time_str)
        else:
            dislike_tag_sql = """
                    select group_concat(tag) from {table_name}
                    where device_id='%s'
                    and channel_id=%s
                    and ctime>='%s'
                    and authenticity=1
                    group by device_id
                    """.format(table_name=USER_DISLIKE_CONTENT_TABLE)
            dislike_tag_sql = dislike_tag_sql % (
                self._device_id, self._channel_id, self._zero_time_str)
        # logger.debug("dislike_tag_sql: %s" % dislike_cate_sql)
        res = self._mysql_client.fetchall(dislike_tag_sql, args=None)
        if not res:
            logger.warn("%s not exist tag information!" %
                        USER_DISLIKE_CONTENT_TABLE)
            return None
        # 计算不感兴趣的品牌的次数
        dislike_tags_and_num = Counter(res[0][0].split(DOT))
        # 只得到本次点击不感兴趣的品牌的次数
        dislike_tags_and_num = dict(
            [(k, v) for k, v in dislike_tags_and_num.iteritems() if k in self._tags])
        logger.debug("dislike_tags_and_num: %s" % dislike_tags_and_num)

        dislike_tags = dislike_tags_and_num.keys()
        if not dislike_tags:
            logger.info("dislike_tags is empty!")
            return None
        logger.debug("dislike_tags: %s" % dislike_tags)
        # 将不感兴趣的品牌列表转为字符串：['华为','小米'] -> "'华为','小米'"
        dislike_tags_str = DOT.join(
            map(lambda s: SINGLE_QUOTE + s + SINGLE_QUOTE, dislike_tags))
        logger.debug("dislike_tags_str: %s" % dislike_tags_str)
        dislike_tag_id_sql = """
                        select associate_title,ID from {table_name}
                        where associate_title in (%s) and is_deleted=0
                        """.format(table_name=SMZDM_BRAND)
        dislike_tag_id_sql = dislike_tag_id_sql % dislike_tags_str
        res = self._mysql_client.fetchall(dislike_tag_id_sql, args=None)
        tag_name_id_map = dict(res)
        logger.debug("tag_name_id_map: %s" % tag_name_id_map)

        # 将品牌名称转为id并得到对应的不感兴趣次数
        dislike_tags_id_and_num = {}
        for tag in tag_name_id_map.keys():
            key = tag_name_id_map[tag]
            dislike_tags_id_and_num[key] = dislike_tags_and_num[tag]


        logger.debug("dislike_cates_id_and_num: %s" % dislike_cates_id_and_num)
        return dislike_cates_id_and_num

        logger.debug("dislike_tags_id_and_num: %s" %
                     dislike_tags_id_and_num)
        return dislike_tags_id_and_num

        logger.debug("dislike_tags_id_and_num: %s" % dislike_tags_id_and_num)
        return dislike_tags_id_and_num

    def _query_dislike_content(self, base):
        dislike_tags = self._query_dislike_tags(base)
        # dislike_tags = None
        dislike_content = {PREFER_BRAND_KEY: dislike_tags
                           }
        return dislike_content


    def _cal_tag_preference(self, dis_cont_dict, cur_pre_list, preference_type, preference_attr):
        """
        :param dis_cont_dict: 不感兴趣品牌字典（包含了品牌id和次数）
        :param cur_pre_list: 当前品牌偏好列表（包含了品牌id和该品牌对应的偏好值）
        :param preference_type: 用于标识属于品类、品牌、标签偏好
        :param preference_attr: 用于标识属于优惠偏好或是原创偏好
        :return:
        """
        cur_pre_dict = dict() if cur_pre_list == EMPTY_PREFER_LIST else dict(
            map(lambda s: s.split(COLON), cur_pre_list))
        logger.debug("%s cur_pre_dict: %s" % (preference_type, cur_pre_dict))

        for tag, count in dis_cont_dict.iteritems():
            key = str(tag)  # cur_pre_dict里的key都是字符串
            pre_value = cur_pre_dict.get(key, None)
            # redis中没有存储该偏好
            if not pre_value:
                # 没有该id对应的偏好值，需要赋予一个初始值来计算
                pre_value = str(DEFAULT_PREFER_WEIGHT)
                logger.warn("%s not exists %s[%s] preference, \n\t\t\t\t\t give default value: %s" % (
                    self._preference_key, preference_type, key, DEFAULT_PREFER_WEIGHT))
            # 将得到的第一个值作为原始值
            cur_pre_value = pre_value.split(VERTICAL_LINE)[0]
            depress_weight = pow(REDUCE_WEIGHT, count)
            update_value = round(float(cur_pre_value) *
                                 depress_weight, REDUCE_DIGIT)
            cur_pre_dict[key] = VERTICAL_LINE.join(
                (cur_pre_value, str(update_value)))
        update_pre_list = map(lambda s: COLON.join(s),
                              cur_pre_dict.iteritems())
        logger.info("update_pre_list: %s" % update_pre_list)

        self._current_preference[preference_type][preference_attr] = update_pre_list



    def _cal_newest_preference(self, current_preference, dislike_content):
        """
        根据当前偏好和不感兴趣内容计算出降权后的值
        :param current_preference: 当前偏好
        :param dislike_content: 不感兴趣内容
        :return:
        """
        # preference_type用于标识属于品类、品牌、标签偏好
        # preference_attr用于标识属于优惠偏好或是原创偏好
        for preference_type in (PREFER_LEVEL_KEY, PREFER_BRAND_KEY, PREFER_TAG_KEY)
            # 不感兴趣内容
            dis_cont_dict = dislike_content.get(preference_type, None)
            if not dis_cont_dict:
                logger.warn("dislike_content not exists %s information" %
                            preference_type)

                preference_type == PREFER_BRAND_KEY:
                # 当前品牌偏好
                cur_pre_list = current_preference.get(
                    preference_type).get(preference_attr)
                logger.debug("%s cur_pre_list: %s" %
                             (preference_type, cur_pre_list))
                self._cal_tag_preference(
                    dis_cont_dict, cur_pre_list, preference_type, preference_attr)

        cur_prefer = self._current_preference
        logger.info("update_preference: %s" % cur_prefer)
        return cur_prefer

    def _save_newest_preference(self, base, newest_preference, expire):
        preference_key = PREFER_USER_REDIS_KEY % self._user_id if base == USER \
            else PREFER_DEVICE_REDIS_KEY % self._device_id

        self._redis_client.set(
            preference_key, json.dumps(newest_preference), expire)

    def _update_preference(self, base=USER):
        current_preference = self._query_current_preference(base)
        if not current_preference:
            # redis中不存在该用户或设备的偏好值则停止计算
            return
        logger.info("current_preference: %s" % current_preference)
        dislike_content = self._query_dislike_content(base)
        logger.info("dislike_content: %s" % dislike_content)
        update_preference = self._cal_newest_preference(
            current_preference, dislike_content)
        self._save_newest_preference(base, current_preference, PREFER_TIME)
        return update_preference

    def _build_data(self, body):
        """
        对接收的消息处理，得到请求时间，用户id、设备id和频道id
        消息格式：2017-06-14 21:03:17,1233,InzfaejYVxvgn1HGg6hQ02MBJA==++,1
        :param body: 接收的消息
        :return:
        """
        req_time_str, device_id, user_id, channel_id, article_id, cates, brands, tags = body.split(
            VERTICAL_LINE)
        self._req_time_str = req_time_str.strip()
        self._device_id = device_id.strip()
        self._user_id = user_id.strip()
        self._channel_id = int(channel_id.strip())
        self._article_id = article_id.strip()
        cates = cates.strip().decode("utf-8")
        self._cates = cates.split(DOT) if cates else []
        brands = brands.strip().decode("utf-8")
        self._brands = brands.split(DOT) if brands else []
        tags = tags.strip().decode("utf-8")
        self._tags = tags.split(DOT) if tags else []

        req_time_str = self._req_time_str
        d = datetime.strptime(req_time_str, TIME_FORMAT_1)
        zero_time = d.replace(hour=0, minute=0, second=0, microsecond=0)
        zero_time_str = zero_time.strftime(TIME_FORMAT_1)
        # zero_time_str = "2017-06-12 00:00:00"

        self._zero_time_str = zero_time_str

    def callback(self, ch, method, properties, body):
        logger.info("message: %s" % body)
        self._build_data(body)
        if self._channel_id not in YOUHUI_CHANNEL_MAP and self._channel_id not in YUANCHUANG_CHANNEL_MAP:
            logger.info("channel not youhui or yuanchuang, need't depress!")
        elif not self._cates and not self._brands and not self._tags:
            logger.info(
                "dislike cate and brand and tag both is empty, need't depress!")
        else:
            logger.info("update device preference...")
            self._update_preference(DEVICE)
            if self._user_id != "":
                logger.info("\n\nupdate user preference...")
                self._update_preference(USER)
        print " [x] Done"
        # print [][0]
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def degree(self):
        """
        对外提供的方法
        :return:
        """
        # pool = self._rabbit_mq_pool_client.get_rabbit_mq_pool().result()
        pool = self._rabbit_mq_client.get_rabbit_mq_pool()
        with pool.acquire() as cxn:
            cxn.channel.queue_declare(queue=DISLIKE_QUEUE, durable=True)
            cxn.channel.basic_qos(prefetch_count=1)
            cxn.channel.basic_consume(
                consumer_callback=self.callback, queue=DISLIKE_QUEUE)
            logger.debug("[*] Waiting for messages. To exit press CTRL+C")
            cxn.channel.start_consuming()


if __name__ == "__main__":
    import logging
    load("../config_test")
    redis_cli = RedisClient().get_redis_client().result()
    # mysql_cli = TorMysqlClient()
    mysql_cli = MySQL()
    rabbit_mq_cli = RabbitMQPoolClient()
    hd = HomeDegree(redis_cli, mysql_cli, rabbit_mq_cli, logging)
    hd.degree()
