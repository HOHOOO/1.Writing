# coding=utf-8
import json
from tornado import web, gen


class BaseHandler(web.RequestHandler):
    def __init__(self, application, request, **kwargs):
        super(BaseHandler, self).__init__(application, request, **kwargs)

    @gen.coroutine
    def jsonify(self, obj):
        self.set_header("Content-Type", "application/json; charset=utf-8")
        data = obj if isinstance(obj, str) else json.dumps(obj, encoding='UTF-8')
        self.write(data)
        self.flush()
        self.finish()

