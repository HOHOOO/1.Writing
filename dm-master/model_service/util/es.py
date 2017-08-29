# coding=utf-8
import json
from tornado import gen
from tornado.log import gen_log
from base.config import Config
from elasticsearch import Transport
from elasticsearch.connection import RequestsHttpConnection, Urllib3HttpConnection

CONNECTION_POOL = None


class InitConnectionPool(object):
    def __init__(self):
        global CONNECTION_POOL
        if not CONNECTION_POOL:
            
            hosts = Config["es.hosts"]
            host_list = []
            for h in hosts.split(";"):
                host_list.append({"host": h})
            gen_log.debug("host_list: %s", host_list)

            CONNECTION_POOL = Transport(hosts=host_list, connection_class=RequestsHttpConnection).connection_pool
            
            gen_log.debug("init ES_CONNECTION_POOL: %s", CONNECTION_POOL)
        super(InitConnectionPool, self).__init__()


class ES(InitConnectionPool):
    
    @gen.coroutine
    def search(self, index, params=None):
        conn = CONNECTION_POOL.get_connection()
        status, headers, search_data = conn.perform_request('GET', '/' + index + '/_search', body=json.dumps(params))
        gen_log.debug("ES search index: %s, params: %s, status: %s, headers: %s", index, str(params),
                      status, headers)
        json_data = json.loads(search_data)
        data = json_data.get("hits", []).get("hits", [])
        raise gen.Return(data)

