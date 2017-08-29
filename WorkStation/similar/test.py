class Main(RequestHandler):


@gen.engine
@tornado.web.asynchronous
def get(self):
    if _DEBUG:
        pdb.set_trace()
            http_client = AsyncHTTPClient()
                response = yield gen.Task(http_client.fetch, "http://code.rootk.com")
                    self.write(str(response))
                        self.finish()
