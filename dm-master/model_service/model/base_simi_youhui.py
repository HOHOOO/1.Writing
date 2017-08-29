# coding=utf-8

import numpy as np

from biz.simi_youhui import OUTPUT_LEVEL_1
from model.simi_youhui import YouHuiSimilarity


class YouHuiBaseSimilarity(YouHuiSimilarity):
    def __init__(self, current_article_frame, recommend_pools_frame, base_weights, extra_weights,
                 recommend_data_min_num, recommend_data_max_num, threshold_simi_score, output_level,
                 **commbine_weights):
        super(YouHuiBaseSimilarity, self).__init__(current_article_frame, recommend_pools_frame, base_weights,
                                                   extra_weights,
                                                   recommend_data_min_num, recommend_data_max_num, threshold_simi_score,
                                                   output_level,
                                                   **commbine_weights)

        self.commbine_weights = commbine_weights

        self.rec_result = dict()
        self.rec_result["weights"] = dict()
        self.rec_result["weights"]["simi"] = {"base_weight": self.base_weights,
                                              "extra_weight": self.extra_weights}
        self.rec_result["weights"]["commbine"] = self.commbine_weights

    # 重写子类的该方法，加入对热度的支持
    def _process_score_sum(self, score_list):
        self.score_simi = score_list.copy()
        log_heat_list = self.recommend_pools["log_heat"].values
        score_list = score_list * self.commbine_weights.get("simi") + log_heat_list * self.commbine_weights.get("heat")
        return score_list

    def _construct_result_fields(self):
        result_fields = ["score_sum", "score_simi_sum"]  # 增加所有特征的相似度分数之和列表名称
        result_fields += self.score_fields  # 增加每个特征的相似度分数名称

        if self.output_level == OUTPUT_LEVEL_1:
            frame_fields = ["article_id", "pro_id", "brand", "level_1", "level_2", "level_3", "level_4", "title",
                            "title_cut", "log_heat", "heat"]
        else:
            frame_fields = ["article_id"]

        self.frame_fields = frame_fields
        result_fields += frame_fields

        return result_fields

    def _construct_result_data(self):
        """
        生成最终结果中每篇文章的信息
        :return:
        """
        result_data = [self.sum_score.tolist(), self.score_simi.tolist()]  # 增加加入热度后的得分及之前的相似度结果得分

        result_data += self.score_lists  # 增加每个特征的相似度分数列表

        ser = [ser.tolist() for col, ser in self.recommend_pools[self.frame_fields].iteritems()]
        result_data += ser
        # 每行结果为某条推荐结果
        result_data = zip(*result_data)
        return result_data

    def _construct_result(self):
        self._calculate_similarity()

        result_fields = self._construct_result_fields()
        result_data = self._construct_result_data()

        # 按照总得分、相似度得分进行降序排列
        rec_data_list = sorted(result_data, key=lambda item: (item[0], item[1]), reverse=True)
        # 筛选大于相似度总分数大于指定值的结果
        rec_data_list = filter(lambda item: item[1] > self.threshold_simi_score, rec_data_list)[0: self.max]
        # 为每个字段添加名称
        rec_data_list = map(lambda items: dict(zip(result_fields, items)), rec_data_list)

        rec_result = self.rec_result
        rec_result["info"] = self.current_article[self.frame_fields].to_dict("index").values()[0]
        rec_result["rec"] = rec_data_list

        return rec_result
