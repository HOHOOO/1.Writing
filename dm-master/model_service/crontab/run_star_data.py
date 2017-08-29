# coding=utf-8
import multiprocessing

import pandas
import requests
from apscheduler.schedulers.blocking import BlockingScheduler

from base.config import load
from comm.consts import YOUHUI_PRIMARY, STAR_DATA_RES_KEY, STAR_DATA_RES_KEY_EXPIRE
from comm.logger import logging, parser, init_logger, LOGGER_NAME
from model.star_data import COL_ARTICLE_ID, COL_REC
from util.mysql import MySQL
from util.redis_client import RedisClient

logger = None


def query_article_ids(date_time):
    mysql_client = MySQL()
    sql = """
    select cast(id as char) from {table_name}
    where pubdate > date_add(now(), interval -{date_time} day)
    and yh_status=1
    """.format(table_name=YOUHUI_PRIMARY, date_time=date_time)
    res = mysql_client.fetchall(sql, args=None)
    article_ids = [tup[0] for tup in res]
    return article_ids


def update_data(req_url, data):
    get_data = {
        "action": "update",
        "article_id": data
    }
    response = requests.get(req_url, get_data)
    return response.text


def write_redis(responses, key_format, key_expire, batch):
    """
    返回成功插入redis中结果的数量
    :param responses:
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

    for article_id, response in responses.iteritems():
        res_str = response.get()
        res_dict = pandas.json.loads(res_str) if isinstance(res_str, (str, unicode)) else {}
        rec = res_dict.get(COL_REC, None)

        if rec:
            key = key_format % article_id
            pipe.setex(key, res_str, key_expire)
            counter += 1
            batch_num += 1
        if batch_num >= batch:
            pipe.execute()
            batch_num = 0
    pipe.execute()

    return counter


def control(data_day=3, process=10, redis_batch=2000):
    """

    :param data_day: 原始文章的时效天数
    :param process: 并发请求接口时开启的进程数
    :param redis_batch: 结果写入redis时的批次数
    :return:
    """
    global logger
    article_ids = query_article_ids(data_day)
    logger.info("article total num: %s" % len(article_ids))
    if len(article_ids) == 0:
        return

    pool = multiprocessing.Pool(process)
    logger.info("run process num: %s" % args.process)

    # url = "http://localhost:8817/recommend_system/model/heuristic_rule/star_data"
    url = "http://system-recommend.smzdm.com:809/recommend_system/model/heuristic_rule/star_data"

    responses = {}
    for article_id in article_ids:
        responses[article_id] = pool.apply_async(update_data, (url, article_id))

    pool.close()
    pool.join()

    logger.info("obtain article recommend data success")
    logger.info("write redis begin")
    num = write_redis(responses, STAR_DATA_RES_KEY, STAR_DATA_RES_KEY_EXPIRE, redis_batch)
    logger.info("the number of original articles that have recommended articles: %s, rate: %s/%s" % (
    num, num, len(article_ids)))
    logger.info("write redis success")

if __name__ == "__main__":
    """
    python -m crontab.run_star_data --config=./config_online --log-file-prefix=/data/logs/model_service/run_star_data.log --log-to-stderr=0 --logging=info --process=10 --interval_minutes=10
    """
    args = parser.parse_args()
    init_logger(args)
    logger = logging.getLogger(LOGGER_NAME)
    load(args.config)

    scheduler = BlockingScheduler()
    scheduler.add_job(control, trigger="interval", minutes=args.interval_minutes,
                      kwargs={"data_day": 3, "process": args.process, "redis_batch": 2000})
    logger.info("job add success!")
    control(data_day=3, process=10, redis_batch=2000)
    scheduler.start()
