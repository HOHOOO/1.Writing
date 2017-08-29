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
import handler.recommend
import handler.user_portrait
import handler.test
import handler.details_recommend
import handler.subscribe_add_follow
import handler.subscribe_banner
import handler.find_simi


class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            # debug
            (r"/recommend_system/test", handler.test.TestHandler),
            # 首页编辑精选
            (r"/recommend_system/editor_excellence_recommend/", handler.recommend.EditorExcellenceHandler),
            # 首页编辑精选效果评估接口
            (r"/recommend_system/tools/", handler.recommend.EditorExcellenceToolsHandler),
            # 首页文章不感兴趣
            (r"/recommend_system/feedback/", handler.recommend.FeedBackHandler),
            # 用户画像服务接口
            (r"/recommend_system/user_portrait/cate/", handler.user_portrait.CatePreferenceHandler),
            # 详情页推荐
            (r"/recommend_system/details_recommend", handler.details_recommend.DetailsRecommendHandler),
            # 添加关注（包含登录或未登录状态）
            (r"/recommend_system/subscribe/add_follow", handler.subscribe_add_follow.AddFollowHandler),
            # 关注页banner（已登录状态）
            (r"/recommend_system/subscribe/banner", handler.subscribe_banner.BannerHandler),
            # 找相似
            (r"/recommend_system/find_similary", handler.find_simi.FindSimiHandler),
            # 兼容旧版详情页推荐调用方式
            (r"/v1/recommend_system/details_recommend/", handler.details_recommend.DetailsRecommendHandler),
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
        python server.py --config=./config_test --port=9999 --process=1 --log_file_prefix=/data/logs/smzdm_recommend --logging=debug --log-rotate-mode=time
        onlie:
        python server.py --config=./config_online --port=9999 --process=20 --log_file_prefix=/data/logs/smzdm_recommend --logging=info --log-rotate-mode=time
    """
    define("port", type=int, default=8813, help="run on the given port")
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
        options.log_file_prefix = "/data/logs/smzdm_recommend/service.log"
        # options.logging = "debug"
        # options.parse_command_line()
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
