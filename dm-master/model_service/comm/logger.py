# coding=utf-8
import argparse
import logging
import logging.config

# logging = logging

# 针对普通程序，提供一个logger关联

LOGGER_NAME = "logger"

file_handler = "fileHandler"
console_handler = "consoleHandler"
default_formatter = "defaultFormatter"

parser = argparse.ArgumentParser()
parser.add_argument("--config", type=str, default="./config_online", required=False,
                    help="配置文件路径")  # 配置文件
parser.add_argument("--log-file-prefix", type=str,  default="", required=False,
                    help="配置文件路径")  # 日志文件
parser.add_argument("--log-to-stderr", type=int, default=1, required=False,
                    choices=[0, 1],
                    help="日志是否输出到在屏幕上， 默认输出，为1")  # 日志是否输出到屏幕上
parser.add_argument("--logging", type=str, default="info", required=False,
                    choices=["debug", "info", "warning", "error"],
                    help="日志文件路径前缀")  # 日志文件级别
parser.add_argument("--process", type=int, default=10, required=False,
                    help="启动的进程数量")  # 进程数量
parser.add_argument("--interval_minutes", type=int, default=40, required=False,
                    help="执行间隔")  # 执行间隔
parser.add_argument("--timeout", type=int, default=15, required=False,
                    help="请求超时时间")  # 请求超时时间
parser.add_argument("--start_day", type=int, default=1, required=False,
                    help="数据开始时间距离当前的天数")  # 请求超时时间
parser.add_argument("--end_day", type=int, default=0, required=False,
                    help="数据截止时间距离当前的天数")  # 请求超时时间

def init_logger(args):
    """
    :param args: argparse.Namespace
    :return:
    """
    level = args.logging.upper()
    log_to_stderr = args.log_to_stderr
    log_file_prefix = args.log_file_prefix.strip()
    handlers = {}
    file_handler_dict = {"class": "logging.handlers.TimedRotatingFileHandler",
                         "filename": log_file_prefix,
                         "when": "midnight",
                         "level": level,
                         "formatter": default_formatter}
    console_handler_dict = {
                            "class": "logging.StreamHandler",
                            "level": level,
                            "formatter": default_formatter}

    loggers = {
                LOGGER_NAME: {
                    "handlers": handlers,
                    "level": level},
                }
    default_formatters = {
                        default_formatter: {
                            "class": "logging.Formatter",
                            "format": "[%(levelname)s %(asctime)s %(filename)s:%(lineno)d] %(message)s",
                            "datefmt": "%Y-%m-%d %H:%M:%S"},
                        }
    if not log_to_stderr and log_file_prefix == "":
        # 关闭日志屏幕输出 & 未指定日志文件
        raise ValueError("no output")
    elif not log_to_stderr and log_file_prefix != "":
        # 关闭日志屏幕输出 & 指定日志文件
        handlers[file_handler] = file_handler_dict
    elif log_to_stderr and log_file_prefix != "":
        # 打开日志屏幕输出 & 指定日志文件
        handlers[file_handler] = file_handler_dict
        handlers[console_handler] = console_handler_dict
    else:
        # 打开日志屏幕输出 & 未指定日志文件
        handlers[console_handler] = console_handler_dict

    conf = {"version": 1,
            "disable_existing_loggers": True,
            "incremental": False,
            "loggers": loggers,
            "handlers": handlers,
            "formatters": default_formatters,
            }

    logging.config.dictConfig(conf)

if __name__ == "__main__":
    """
    python logger.py  --log-file-prefix=./abc.txt  --log-to-stderr=0 --logging=debug  # 日志文件为./abc.txt，日志不输出到屏幕上，日志级别为debug
    """
    args = parser.parse_args()
    init_logger(args)
    logger = logging.getLogger(LOGGER_NAME)
    logger.debug("%s: debug msg..." % __name__)
    logger.info("%s: info msg..." % __name__)