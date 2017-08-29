# coding=utf-8
import multiprocessing

import pandas
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from requests import Timeout

from base.config import load
from comm.consts import YOUHUI_PRIMARY, YOUHUI_BASE_SIMI_RES_KEY, YOUHUI_BASE_SIMI_RES_KEY_EXPIRE
from comm.logger import logging, parser, init_logger, LOGGER_NAME

from util.mysql import MySQL
from util.redis_client import RedisClient

logger = None
kwargs = None
# url = "http://localhost:8817/recommend_system/model/ml_rule/youhui_base_simi"
url = "http://system-recommend.smzdm.com:809/recommend_system/model/ml_rule/youhui_base_simi"


def query_article_ids(start_day, end_day):
    mysql_client = MySQL()
    sql = """
    select cast(id as char) from {table_name}
    where pubdate > date_add(now(), interval -{start_day} day)
    and pubdate < date_add(now(), interval -{end_day} day)
    and yh_status=1 order by pubdate desc
    """.format(table_name=YOUHUI_PRIMARY, start_day=start_day, end_day=end_day)
    res = mysql_client.fetchall(sql, args=None)
    article_ids = [tup[0] for tup in res]
    return article_ids


def req_data(req_url, timeout, article_id):
    params = {
        "action": "update",
        "model_type": "base_simi",
        "article_id": article_id
    }
    response = requests.get(req_url, params=params, timeout=timeout)
    return response.text


def write_redis(context, key_format, key_expire, batch):
    """
    返回成功插入redis中结果的数量
    :param context: key为文章id，value为当前文章的推荐结果
    :param key_format: key格式
    :param key_expire: key过期时间
    :param batch: 批次插入的数量
    :return:
    """
    global logger
    redis_client = RedisClient().get_redis_client().result()
    pipe = redis_client.pipeline()

    counter = 0  # 记录成功更新的总数
    batch_num = 0  # 记录当前更新批次的数量

    for article_id, response in context.iteritems():

        try:
            res_str = response.get()
            res_dict = pandas.json.loads(res_str) if isinstance(res_str, (str, unicode)) else {}
            rec = res_dict.get("rec", None)

            if rec:
                key = key_format % article_id
                pipe.setex(key, res_str, key_expire)
                counter += 1
                batch_num += 1

            # if rec:
            #     key = key_format % article_id
            #
            #     r = ["_".join([str(d.get("article_id")), str(d.get("score_sum")), str(d.get("score_simi_sum"))]) for d
            #          in rec]
            #     pipe.delete(key)
            #     pipe.rpush(key, *r)
            #     pipe.expire(key, key_expire)
            #
            #     counter += 1
            #     batch_num += 1


        except Timeout, e:
            import traceback
            msg = traceback.format_exc()
            logger.error("article_id: %s generate simi data timeout!\nerror_msg: %s" % (article_id, msg))
        except Exception, e:
            import traceback
            msg = traceback.format_exc()
            logger.error("article_id: %s generate simi data failed!\nerror_msg: %s" % (article_id, msg))

        if batch_num >= batch:
            pipe.execute()
            batch_num = 0
    pipe.execute()

    return counter


def control(start_day=1, end_day=0, process=10, timeout=5, redis_batch=2000, parameters=None):
    """
    :param data_day: 原始文章的时效天数
    :param process: 并发请求接口时开启的进程数
    :param redis_batch: 结果写入redis时的批次数
    :return:
    """
    global logger, kwargs, url

    start_time = datetime.now()
    article_ids = query_article_ids(start_day, end_day)
    logger.info("article total num: %s" % len(article_ids))
    if len(article_ids) == 0:
        end_time = datetime.now()
        logger.info(
            "job execute finish, start_time: %s, end_time: %s, kwargs: %s" % (start_time, end_time, kwargs))
        return

    pool = multiprocessing.Pool(process)
    logger.info("run process num: %s" % args.process)

    context = {}
    for article_id in article_ids:
        context[article_id] = pool.apply_async(req_data, (url, timeout, article_id))

    pool.close()
    pool.join()

    logger.info("obtain article recommend data success")
    logger.info("write redis begin")
    num = write_redis(context, YOUHUI_BASE_SIMI_RES_KEY, YOUHUI_BASE_SIMI_RES_KEY_EXPIRE, redis_batch)
    end_time = datetime.now()

    logger.info("the number of original articles that have recommended articles: %s, rate: %s/%s" % (
        num, num, len(article_ids)))
    logger.info("write redis success")
    logger.info(
        "job execute finish, start_time: %s, end_time: %s, kwargs: %s" % (start_time, end_time, kwargs))


if __name__ == "__main__":
    """
    python -m crontab.run_simi_youhui --config=./config_online --log-file-prefix=/data/logs/model_service/run_simi_youhui.log --log-to-stderr=0 --logging=info --process=10 --interval_minutes=30 --timeout=10 --start_day=1 --end_day=0
    """
    args = parser.parse_args()
    init_logger(args)
    logger = logging.getLogger(LOGGER_NAME)
    load(args.config)

    scheduler = BlockingScheduler()
    kwargs = {"start_day": args.start_day,
              "end_day": args.end_day, "process": args.process,
              "timeout": args.timeout, "redis_batch": 2000}

    scheduler.add_job(control, trigger="interval", minutes=args.interval_minutes,
                      kwargs=kwargs)
    logger.info("start kwargs: %s" % kwargs)
    logger.info("job add success!")
    control(start_day=args.start_day, end_day=args.end_day, process=args.process,
            timeout=args.timeout, redis_batch=2000)
    scheduler.start()
