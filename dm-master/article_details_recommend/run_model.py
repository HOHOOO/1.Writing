# coding=utf-8
import argparse
import time

import sys

import logging.config
import pandas as pd
from logging.handlers import TimedRotatingFileHandler

from base.utils import create_mysql_master_connect, create_redis_connect
from com.backend import Backend
from base.config import load, Config
from model.yh_model import SimilaryModel, features_with_weight, CombineModel, combine_weights, model_version
from model.preprocessing import PreProcess


def cut_arr(arr, sep):
    """
    将一个列表arr分割为一个个长度为sep大小的列表
    example：
        cut_arr(range(0, 10), 5)  # [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]
        cut_arr(range(0, 11), 5)  # [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9], [10]]
    :param arr: 要分割的列表
    :param sep: 分割后每个小列表的长度
    :return:
    """
    # print "arr: %s" % arr
    concat = []
    # print "len: %s" % (len(arr) / sep)
    num = len(arr) / sep if len(arr) % sep == 0 else len(arr) / sep + 1
    # print "num: %s" % num
    for i in range(0, num):
        concat.append(arr[i * sep: (i + 1) * sep])

    return concat


def write_redis_and_mysql(res_slice, sql, model_version):
    def float2str(arr): return map(lambda x: unicode(x), arr)

    backend = Backend(Config)
    expire_time = backend.youhui_similary_article_key_time

    mysql_connect = create_mysql_master_connect(Config)
    cursor = mysql_connect.cursor()
    redis_connect = create_redis_connect(Config)
    pipe = redis_connect.pipeline()

    mysql_args = []
    for result in res_slice:
        src_article_id = unicode(result[0])  # 源文章id
        rec_list = result[1]  # 推荐列表

        # 操作redis
        key = backend.get_youhui_similary_article_key(src_article_id, model_version)  # redis存储推荐结果的key
        value_list = map(lambda arr: "_".join(float2str(arr)), rec_list)  # 将推荐列表里的每一项中的列表内容进行拼接
        pipe.delete(key)
        pipe.rpush(key, *value_list)
        pipe.expire(key, expire_time)

        # 为操作mysql做准备

        mysql_args += map(lambda s: (src_article_id + "_" + s).split("_"), value_list)
    mysql_args = map(lambda arr: arr + [model_version, ], mysql_args)
    t0 = time.clock()
    pipe.execute()
    t1 = time.clock()

    cursor.executemany(sql, mysql_args)
    cursor.close()
    mysql_connect.commit()
    mysql_connect.close()
    t2 = time.clock()

    logger.debug("save result: data.length: %s ,insert redis: %.2fs, insert mysql: %.2fs" % (len(res_slice), (t1 - t0), (t2 - t1)))


def save_result(results, sql, model_version):
    res_slices = cut_arr(results.items(), sep=100)
    for res_slice in res_slices:
        write_redis_and_mysql(res_slice, sql, model_version)
        time.sleep(1)


def run_model(model_version):
    backend = Backend(Config)
    youhui_similary_combine_base_sql = backend.insert_youhui_similary_combine_base_sql
    # youhui_similary_base_sql = backend.insert_youhui_similary_base_sql
    query_newest_article_base_sql = backend.query_newest_article_base_sql

    logger.debug("query_sql: %s" % query_newest_article_base_sql)
    mysql_connect = create_mysql_master_connect(Config)
    data = pd.read_sql(query_newest_article_base_sql, mysql_connect)
    logger.debug("data_length: %s" % data.shape[0])

    t0 = time.clock()
    pp = PreProcess()
    data = pp(data)
    title_fc_extra_weight = 8
    similary_model = SimilaryModel(data, features_with_weight, src_article_num=None,
                                   rec_article_num=9, title_fc_extra_weight=title_fc_extra_weight,
                                   ndigits=2)
    t1 = time.clock()
    logger.debug("start model time: %.2f s" % (t1 - t0))

    combine_model = CombineModel(data, combine_weights, similary_model,
                                 src_article_num=None,
                                 rec_article_num=9, ndigits=2)
    combine_res = combine_model.map_articles()

    t2 = time.clock()
    logger.debug("calculate time: %.2f s" % (t2 - t1))
    save_result(combine_res, youhui_similary_combine_base_sql, model_version)
    # save_result(r1, youhui_similary_base_sql)

    t3 = time.clock()
    logger.debug("save result total time: %.2f s" % (t3 - t2))


def init_logger(args):
    conf = {"version": 1,
            "disable_existing_loggers": True,
            "incremental": False,
            "loggers": {"root": {
                "handlers": ["consoleHandler"],
                "level": args.logging.upper()
            },
                "model": {
                    "handlers": ["consoleHandler", "fileHandler"],
                    "level": args.logging.upper()
                }
            },
            "handlers": {"consoleHandler": {"class": "logging.StreamHandler",
                                            "level": args.logging.upper(),
                                            "formatter": "default",
                                            },

                         "fileHandler": {"class": "logging.handlers.TimedRotatingFileHandler",
                                         "filename": args.log_file_prefix,
                                         "when": "midnight",
                                         "level": args.logging.upper(),
                                         "formatter": "default",
                                         }
                         },
            "formatters": {"default": {"class": "logging.Formatter",
                                       "format": "[%(levelname)s %(asctime)s %(filename)s:%(lineno)d] %(message)s",
                                       # "format": "[%(levelname)s %(asctime)s %(filename)s:%(lineno)d %(process)d %(pathname)s] %(message)s",
                                       "datefmt": "%Y-%m-%d %H:%M:%S"}
                           },
            }

    logging.config.dictConfig(conf)


if __name__ == "__main__":
    """
    test: python run_model.py --config=./config_test --log-file-prefix=/data/logs/article_details/model/model.log --logging=debug
    online: python run_model.py --config=./config_online --log-file-prefix=/data/logs/article_details/model/model.log --logging=info
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default="--config=./config_online", required=True,
                        help="配置文件路径")  # 配置文件
    parser.add_argument("--log-file-prefix", type=str, default="path", required=True,
                        help="日志文件路径前缀")  # 日志文件路径的前缀
    parser.add_argument("--logging", type=str, default="info", required=False, choices=["debug", "info", "warning", "error"],
                        help="日志文件路径前缀")  # 日志文件级别
    args = parser.parse_args()

    init_logger(args)
    load(args.config)
    logger = logging.getLogger("model")

    t0 = time.clock()
    run_model(model_version)
    t1 = time.clock()
    logger.info("total run time: %.2f s" % (t1 - t0))
