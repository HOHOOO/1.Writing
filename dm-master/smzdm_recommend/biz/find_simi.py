# coding=utf-8
import pandas
import tornado

from tornado import gen
from tornado.log import gen_log

from biz_config.find_simi import SIMI_SCORE_MIN, SIMI_KEY_FORMAT
from comm.consts import DOT

# SIMI_KEY_FORMAT = "yh.%s.yh_sim_rec.master"


class FindSimi(object):
    def __init__(self, redis_client, article_ids, scene):
        self.redis_client = redis_client
        self.article_ids = article_ids
        self.scene = scene

        self.fields = ["article_id", "rs_id1", "rs_id2", "rs_id3", "rs_id4", "rs_id5"]

    def _query_redis_data(self):
        article_id = self.article_ids
        key = SIMI_KEY_FORMAT % article_id
        gen_log.info("details key: %s" % key)

        redis_result = self.redis_client.get(key)
        if redis_result:
            redis_dict = pandas.json.loads(redis_result)
            # 对结果按照相似度得分进行排序
            redis_data = sorted(redis_dict.get("rec"), key=lambda item: float(item.get("score_simi_sum")), reverse=True)

            result = []
            for item in redis_data:
                simi_score = float(item.get("score_simi_sum"))
                if simi_score < SIMI_SCORE_MIN:
                    continue
                article_id = str(item.get("article_id"))
                values = [article_id, "", "", "", "", ""]
                result.append(dict(zip(self.fields, values)))

            return result

        # redis_result = self.redis_client.lrange(key, 0, -1)
        # # 对结果按照相似度得分进行排序
        # redis_result = sorted(redis_result, key=lambda item: float(item.split("_")[-1]), reverse=True)
        # result = []
        # for item in redis_result:
        #     # "7540081_2.69_0.0_1.0_0.8_0.6_0.4_0.2_0.14_0.0_0.0_0.02_2.66"
        #     arr = item.split("_")
        #     simi_score = float(arr[-1])
        #     if simi_score < SIMI_SCORE_MIN:
        #         continue
        #     article_id = arr[0]
        #     values = [article_id, "", "", "", "", ""]
        #     result.append(dict(zip(self.fields, values)))
        #
        # return result

    def _query_redis_data_num(self):
        pipe = self.redis_client.pipeline()
        article_ids = self.article_ids.split(DOT)
        gen_log.info("list article num: %s" % len(article_ids))

        for article_id in article_ids:
            key = SIMI_KEY_FORMAT % article_id
            gen_log.debug("list key: %s" % key)
            # pipe.lrange(key, 0, -1)
            pipe.get(key)
        redis_result = pipe.execute()
        # 计算相似度大于指定阈值的推荐文章个数
        # res_num = [len(filter(lambda item: float(item.split("_")[-1]) >= SIMI_SCORE_MIN, res)) for res in redis_result]

        res_num = []
        for res in redis_result:
            num = 0
            if res:
                redis_data = pandas.json.loads(res).get("rec")
                num = len(filter(lambda item: float(item.get("score_simi_sum")) >= SIMI_SCORE_MIN, redis_data))
            res_num.append(num)


        result = dict(zip(article_ids, res_num))
        return result

    @property
    @tornado.gen.coroutine
    def simi_data(self):
        result = None
        if self.scene == "list":
            result = self._query_redis_data_num()
        elif self.scene == "details":
            result = self._query_redis_data()
        raise gen.Return(result)

