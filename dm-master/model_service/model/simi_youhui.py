# coding=utf-8
import sys

import jieba
import pandas

from scipy.sparse import csr_matrix
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from tornado.log import gen_log

from biz.simi_youhui import FEATURE_SCORE_NAME, OUTPUT_LEVEL_1, SEX_PRODUCT_LEVEL_ID
from extra.stop_words import STOP_WORDS

reload(sys)
sys.setdefaultencoding("utf-8")

import numpy as np

jieba.initialize()


stop_words = map(lambda x: unicode(x), STOP_WORDS)

class YouHuiSimilarity(object):
    def __init__(self, current_article_frame, recommend_pools_frame, base_weights, extra_weights,
                 recommend_data_min_num, recommend_data_max_num, threshold_simi_score, output_level, **kwargs):
        """
        :param current_article_frame: 当前文章信息，DataFrame
        :param recommend_pools_frame: 推荐池子文章信息，DataFrame
        :param base_weights: 基本各种权重，dict
        :param extra_weights: 额外权重，dict
        :param recommend_data_min_num: 推荐结果最小数量，int
        :param recommend_data_max_num: 推荐结果最大数量，int
        :param threshold_simi_score: 用于筛选相似度得分大于该阈值的结果
        :param output_level: 输出级别，str，表示输出结果的级别，1表示信息级别最高，包含信息最详细，2表示信息级别一般
        """
        self.current_article = current_article_frame
        if self.current_article.shape[0] == 0:
            raise ValueError("current article information not exists!")

        self.recommend_pools = recommend_pools_frame
        self.base_weights = base_weights
        self.extra_weights = extra_weights
        self.min = recommend_data_min_num
        self.max = recommend_data_max_num
        self.threshold_simi_score = threshold_simi_score
        self.output_level = output_level

        self.feature_cols = self.base_weights.keys()

        self.recommend_pools_num = self.recommend_pools.shape[0]
        gen_log.info("recommend_pools num: %s" % self.recommend_pools_num)
        self._use_rule()
        self.recommend_pools_num = self.recommend_pools.shape[0]
        gen_log.info("recommend_pools num after use_rule: %s" % self.recommend_pools_num)

        self.rec_result = dict()
        self.rec_result["weights"] = {"base_weight": self.base_weights,
                                      "extra_weight": self.extra_weights}

        self.ndigits = 4

    def _use_rule(self):
        recommend_pools = self.recommend_pools
        # 过滤掉本身
        recommend_pools = recommend_pools[
            recommend_pools["article_id"] != self.current_article["article_id"].values[0]]

        # 成人用品只能推荐成人用品，非成人用品无法推荐成人用品
        if self.current_article["level_2_id"].values[0] == str(SEX_PRODUCT_LEVEL_ID):
            self.recommend_pools = recommend_pools[recommend_pools["level_2_id"] == str(SEX_PRODUCT_LEVEL_ID)]
        else:
            self.recommend_pools = recommend_pools[recommend_pools["level_2_id"] != str(SEX_PRODUCT_LEVEL_ID)]

    def _calculate_similarity(self):
        """
        生成当前文章与推荐池子中文章的的所有特征相似度之和及每个特征的相似度列表

        :return:
        """
        recommend_pools = self.recommend_pools
        current_article = self.current_article
        num = self.recommend_pools_num

        vect = CountVectorizer(analyzer="word", lowercase=True, tokenizer=jieba.cut,
                               stop_words=stop_words,
                               binary=False)

        # 初始化用于用户包含每一个特征的相似度得分字典
        simi_part_score = {}
        simi_sum_score = np.zeros(num, dtype=np.float16)

        for feature_col in self.feature_cols:
            gen_log.info("fit feature: %s" % feature_col)
            try:
                # 根据推荐池子信息的各个特征进行fit,然后transform出推荐池子的矩阵
                recommend_pools_y = vect.fit_transform(recommend_pools[feature_col])
                # gen_log.debug("vocabulary_: %s" % vect.get_feature_names())
                # transform出当前文章的矩阵
                current_article_X = vect.transform(current_article[feature_col])
            except ValueError:
                gen_log.warn("empty vocabulary; perhaps the documents only contain stop words")

            # 计算出当前当前文章在该特征下与推荐池子中每篇文章的相似度
            simi_mat = cosine_similarity(current_article_X, recommend_pools_y, dense_output=False)

            # 针对标题相似度得分，如果三级分类缺失，则需要同时乘以额外的权重
            if feature_col == "title" and self.extra_weights:
                # level_3_id 为 "" 或 -1 都认为缺失
                base_array = np.where(recommend_pools["level_3_id"].isin(["", "-1"]),
                                      self.extra_weights.get("level_3_defect_title_extra"), 1)
                simi_mat = simi_mat.multiply(base_array).tocsr()

                # 最高输出日志级别，则需要获取切词后的文章标题
                if self.output_level == OUTPUT_LEVEL_1:
                    self.current_article = self.current_article.copy()
                    self.recommend_pools = self.recommend_pools.copy()
                    self.current_article.loc[:, "title_cut"] = map(lambda items: ",".join(items),
                                                                   vect.inverse_transform(current_article_X))
                    self.recommend_pools.loc[:, "title_cut"] = map(lambda items: ",".join(items),
                                                                   vect.inverse_transform(recommend_pools_y))
                    gen_log.debug(",".join(vect.get_feature_names()))
            # 保留指定位小数
            simi_mat = np.around(simi_mat.toarray()[0], self.ndigits)

            # 将得到的特征相似度得分乘以对应的权重
            simi_mat_added_weight = simi_mat * self.base_weights.get(feature_col)
            # gen_log.debug("current_article_X: %s" % current_article_X.toarray())
            # gen_log.debug("recommend_pools_y: %s" % recommend_pools_y.toarray())
            # gen_log.debug("simi_mat: %s" % simi_mat)
            # gen_log.debug("simi_mat_added_weight: %s" % simi_mat_added_weight)

            # 计算相似度总分时，采用加权后的结果
            simi_sum_score = simi_sum_score + simi_mat_added_weight

            # 返回每个特征相似度得分时，采用原始结果
            simi_part_score[FEATURE_SCORE_NAME[feature_col]] = simi_mat.tolist()

        self.score_fields = simi_part_score.keys()  # 每个特征的相似度分数名称
        self.score_lists = simi_part_score.values()  # 每个特征的相似度分数列表
        self.sum_score = self._process_score_sum(simi_sum_score)  # 所有特征的相似度分数之和列表

    def _process_score_sum(self, score_list):
        """
        对生成的相似度总分做处理
        :param score_list:
        :return:
        """
        return score_list

    def _construct_result_fields(self):
        """
        生成最终结果所需的字段名
        :return:
        """
        result_fields = ["score_simi_sum"]  # 增加所有特征的相似度分数之和列表名称
        result_fields += self.score_fields  # 增加每个特征的相似度分数名称

        if self.output_level == OUTPUT_LEVEL_1:
            frame_fields = ["article_id", "pro_id", "brand", "level_1", "level_2", "level_3", "level_4",
                            "title",
                            "title_cut"]
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
        result_data = [self.sum_score.tolist()]  # 增加所有特征的相似度分数之和列表
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

        # 按照相似度总得分进行降序排列
        rec_data_list = sorted(result_data, key=lambda item: item[0], reverse=True)
        # 筛选大于相似度总分数大于指定值的结果
        rec_data_list = filter(lambda item: item[0] > self.threshold_simi_score, rec_data_list)[0: self.max]
        # 为每个字段添加名称
        rec_data_list = map(lambda items: dict(zip(result_fields, items)), rec_data_list)

        rec_result = self.rec_result
        rec_result["info"] = self.current_article[self.frame_fields].to_dict("index").values()[0]
        rec_result["rec"] = rec_data_list

        return rec_result

    @property
    def rec_data(self):
        rec_result = self._construct_result()
        return rec_result


class YouHuiPreProcess(object):
    def __init__(self, frame):
        self.data = self.process(frame)

    def process(self, frame):
        # 补充缺失值，并转为str类型
        frame["pro_id"] = frame["pro_id"].fillna(value="").astype(str)
        frame["level_1_id"] = frame["level_1_id"].fillna(value="").astype(str).str.replace("-1", "0")
        frame["level_2_id"] = frame["level_2_id"].fillna(value="").astype(str).str.replace("-1", "0")
        frame["level_3_id"] = frame["level_3_id"].fillna(value="").astype(str).str.replace("-1", "0")
        frame["level_4_id"] = frame["level_4_id"].fillna(value="").astype(str).str.replace("-1", "0")
        frame["brand_id"] = frame["brand_id"].fillna(value="").astype(str)
        frame["title"] = frame["title"].fillna(value="").astype(str)

        # 从标题中提取特殊的特征
        frame["sex"] = frame["title"].map(lambda s: self._find_sex(s))
        frame["crowd"] = frame["title"].map(lambda s: self._find_crowd(s))

        frame["log_heat"] = np.log1p(frame["heat"])

        # 归一化
        frame["log_heat"] = (frame["log_heat"] - frame["log_heat"].min()) / (
            frame["log_heat"].max() - frame["log_heat"].min())
        frame["log_heat"] = frame["log_heat"].fillna(value=0)
        frame["log_heat"] = np.round(frame["log_heat"].values, 4)

        return frame

    def _find_sex(self, s):
        """
        找到字符串中包含的性别属性词，并返回。
        既包含男、又包含女和男、女都不包含返回空字符串；
        只包含男或只包含女则返回男或女字符串
        :param s:
        :return:
        """
        if "男" in s and "女" in s:
            return ""
        elif "男" in s:
            return "男"
        elif "女" in s:
            return "女"
        else:
            return ""

    def _find_crowd(self, s):
        """
        找到字符串中包含的人群属性词，并返回
        :param s:
        :return:
        """
        base = "婴" in s or "幼" in s
        if "童" in s and not base:
            return "童"
        if base and not ("幼犬" in s or "幼猫" in s):
            return "婴/幼"
        else:
            return ""
