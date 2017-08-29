# coding=utf-8

import platform

import tornado.netutil
import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.process
from tornado.options import define, options
from tornado.log import access_log

from base.config import load, Config
import handler.heuristic_rule, handler.simi_youhui
import handler.test


class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            # debug
            (r"/recommend_system/test", handler.test.TestHandler),
            # 明星数据
            (r"/recommend_system/model/heuristic_rule/star_data", handler.heuristic_rule.StarDataHandler),
            (r"/recommend_system/tools/heuristic_rule/star_data", handler.heuristic_rule.StarDataHandler),
            # 基于相似度的混合模型
            (r"/recommend_system/model/ml_rule/youhui_base_simi", handler.simi_youhui.BaseSimilarityHandler),
            (r"/recommend_system/tools/ml_rule/youhui_base_simi", handler.simi_youhui.BaseSimilarityHandler),

        ]

        settings = dict(
            compress_response=True,
            xsrf_cookies=False,
            debug=False,
        )

        tornado.web.Application.__init__(self, handlers, **settings)


if __name__ == "__main__":
    """
    run:
        test:
        python server.py --config=./config_test --port=9999 --process=1 --log_file_prefix=/data/logs/model_service --logging=debug --log-rotate-mode=time
        onlie:
        python server.py --config=./config_online --port=9999 --process=20 --log_file_prefix=/data/logs/model_service --logging=info --log-rotate-mode=time
    """
    define("port", type=int, default=8817, help="run on the given port")
    define("config", type=str, default="config_online", help="config path")
    define("process", type=int, default=1, help="fork process num")

    if platform.system() != "Windows":  # 非 windows 平台
        options.parse_command_line(final=False)
        sockets = tornado.netutil.bind_sockets(options.port)
        task_id = tornado.process.fork_processes(options.process)  # task_id为fork_processes返回的子进程编号

        thread_log_file = options.log_file_prefix + ("_%d_%d.log" % (options.port, task_id))
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
        options.log_file_prefix = "/data/logs/model_service/service.log"
        options.logging = "info"
        options.parse_command_line()
        print options.as_dict()
        access_log.info("local.........")
        access_log.info("config: %s, port: %s, process: %s, log_file_prefix: %s" % (
            options.config, options.port, options.process, options.log_file_prefix))
        access_log.debug("debug.........")

        load(path=options.config)
        # config = load(path=options.config)  # load configures
        server = tornado.httpserver.HTTPServer(Application())
        server.listen(options.port)

        tornado.ioloop.IOLoop.instance().start()
