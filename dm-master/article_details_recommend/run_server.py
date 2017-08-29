# coding=utf-8
import random
import platform
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

import tornado.netutil
import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.process
from tornado.options import define, options
from tornado.log import access_log, app_log, gen_log

from base.config import load, Config
from com.backend import Backend
from server.server import AsyncReqDataHandler


class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            # 文章详情页数据获取接口
            (r"/v1/recommend_system/details_recommend/", AsyncReqDataHandler),
        ]

        settings = dict(
            compress_response=True,
            xsrf_cookies=True,
            debug=False,
        )

        tornado.web.Application.__init__(self, handlers, **settings)

        self.config = Config
        self.backend = Backend(Config)


if __name__ == "__main__":
    """
    run:
        test:
        python run_server.py --config=./config_test --port=8806 --process=0 --log_file_prefix=/data/logs/article_details/server.log --log-rotate-mode=time --logging=info  --version=new
        onlie:
        python run_server.py --config=./config_online --port=8806 --process=0 --log_file_prefix=/data/logs/article_details/server.log --log-rotate-mode=time --logging=info --version=new
    """
    # define("log_rotate_mode", type=str, default="time")  # 按照时间来存储日志
    # define("log_rotate_when", type=str, default="midnight")  # 单位自然天
    # define("log_rotate_interval", type=int, default=1)  # 1个单位，即1天
    # define("log_file_num_backups", type=int, default=10)  # 最多保留10天的日志
    # options.logging = "info"

    define("port", type=int, default=8806, help="run on the given port")
    define("config", type=str, default="config_online", help="config path")
    define("process", type=int, default=1, help="fork process num")

    if platform.system() != "Windows":  # 非 windows 平台
        options.parse_command_line(final=False)
        sockets = tornado.netutil.bind_sockets(options.port)
        task_id = tornado.process.fork_processes(options.process)  # task_id为fork_processes返回的子进程编号

        thread_log_file = options.log_file_prefix + ".%d" % task_id
        options.log_file_prefix = thread_log_file  # 定义每个进程日志文件的路径

        options.run_parse_callbacks()
        access_log.info("config: %s, port: %s, process: %s, log_file_prefix: %s" % (
            options.config, options.port, options.process, options.log_file_prefix))

        load(path=options.config)  # load configures
        server = tornado.httpserver.HTTPServer(Application())
        server.add_sockets(sockets)
        tornado.ioloop.IOLoop.current().start()

    else:
        options.parse_command_line()
        options.log_file_prefix = "/data/logs/article_details/server.log"
        # options.logging = "debug"
        # options.parse_command_line()
        print options.as_dict()
        access_log.info("local.........")
        access_log.info("config: %s, port: %s, process: %s, log_file_prefix: %s" % (
            options.config, options.port, options.process, options.log_file_prefix))
        access_log.debug("debug.........")
        load(path=options.config)  # load configures
        server = tornado.httpserver.HTTPServer(Application())
        server.listen(options.port)

        tornado.ioloop.IOLoop.instance().start()

