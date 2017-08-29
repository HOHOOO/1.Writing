# coding=utf-8
import pstats
import array
from collections import OrderedDict, Mapping, Iterable
from collections import defaultdict

import jieba
import numpy as np
import scipy.sparse as sp
import pandas as pd
import os
import time
import copy
import logging

import scipy
from scipy.sparse import csr_matrix, spmatrix, coo_matrix
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from model_title_fenci_stop_words.preprocessing import PreProcess
from tools.stop_words import SIGNAL_STOP_WORDS

logger = logging.getLogger("root")


class SimilaryModel(object):
    """
    用于计算相似度的模型
    """
    def __init__(self, dataframe, features_with_weight, src_article_num=None,
                 rec_article_num=3, title_fc_extra_weight=None, ndigits=2):
        """
        :param dataframe: 类型为 pandas.DataFrame 类型
        :param word_features_with_weight: 基于word生成词汇表的特征，类型为 OrderedDict
        :param char_features_with_weight: 基于char生成词汇表的特征，类型为 OrderedDict
        :param src_article_num: 为多少篇原文章生成推荐结果，默认为None，表示为所有输入数据生成推荐结果，
                为int(需<=len(dataframe))时，表示为dataframe[0:src_article_num]文章生成推荐结果
        :param rec_article_num: 指定每篇文章的推荐文章数目
        :param ndigits: 指定生成相似度得分时保留的小数点位数
        """
        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError("dataframe must be pandas.DataFrame!")
        if not isinstance(features_with_weight, OrderedDict):
            raise TypeError("word_features_with_weight must be OrderedDict!")
        if src_article_num > len(dataframe):
            raise  ValueError("length of src_article_num should not exceed len(dataframe)")

        self.dataframe = dataframe

        self.features_with_weight = features_with_weight
        self.features = list(zip(*self.features_with_weight.items())[0])

        self.src_article_num = src_article_num
        self.rec_article_num = rec_article_num
        self.title_fc_extra_weight = title_fc_extra_weight
        self.ndigits = ndigits

        self.features_with_score = None
        self.feature_matrixs = None

    def _generate_vector(self):
        """
        创建词汇表并生成文本向量
        """
        data = self.dataframe
        vect_word = CountVectorizer(analyzer="word")
        vect_char = CountVectorizer(analyzer="char")
        # vect_text = CountVectorizer(analyzer="word", stop_words=SIGNAL_STOP_WORDS, tokenizer=jieba.cut)
        # vect1 = CountVectorizer(analyzer="word")
        # vect2 = CountVectorizer(analyzer="word", tokenizer=lambda x: x)
        feature_matrixs = {}
        vectorizers = {}
        # print "-----------: %s" % ",".join(jieba.cut(u"达斯登 速干T恤男"))
        tmp_features = self.features
        for feature in tmp_features:

            vect = vect_char if feature in (u"sex", u"crowd") else vect_word
            # if feature in (u"title_fc"):
            #     vect = vect1
            # else:
            #     vect = vect2
            try:
                feature_matrix = vect.fit_transform(data[feature])
                feature_matrixs[feature] = feature_matrix
                vectorizers[feature] = vect
                # if feature in ("level_3_id", "title_fc"):
                # print "feature_matrixs[%s]:" % feature
                # print "feature_matrixs:\n %s" % feature_matrixs[feature].toarray()
                # print "feature: %s" % ", ".join(vect_word.get_feature_names())

            except ValueError:
                feature_matrixs[feature] = csr_matrix((data.shape[0], 1), dtype=np.int8)
                vectorizers[feature] = None
                print "feature[%s] is empty vocabulary; perhaps the documents only contain stop words!" % feature

        self.feature_matrixs = feature_matrixs
        self.vectorizers = vectorizers
        return feature_matrixs

    def calculate_similary(self):
        """
        根据特征的文本向量生成相似度矩阵
        """
        # ser_csr = self._generate_vector()
        data = self.dataframe
        feature_matrixs = self._generate_vector()
        number = len(data)  # 数据的个数

        # 计算相似度矩阵
        src_score_list = []

        for feature in self.features:
            sim = cosine_similarity(feature_matrixs[feature], dense_output=False) * self.features_with_weight.get(
                feature)
            src_score_list.append(sim)

        # 生成内容为各个特征及对应的相似度得分的一个字典
        self.features_with_score = dict(zip(self.features, src_score_list))

        # 增加三级品类为空的文章的标题分词权重
        if self.title_fc_extra_weight is not None:
            base_array = np.where(data[u"level_3_id"] == "", self.title_fc_extra_weight, 1)
            weight_mat = np.array([base_array] * number)
            # weight_mat = np.array([data["level_3_id_is_null"] * self.title_fc_extra_weight] * number)
            self.features_with_score["title_fc"] = self.features_with_score["title_fc"].multiply(weight_mat).tocsr()

        # 初始化一个csr矩阵
        sum_score = csr_matrix((number, number), dtype=np.float16)

        for feature in self.features:
            sum_score = sum_score + self.features_with_score.get(feature)

        sum_score.setdiag([-1] * number)  # 将自身向量与自身向量的相似度设为-1，即文章本身之间的相似度设为-1

        return sum_score

    def map_articles(self, score_csr_matrix, res_format="json"):
        """
        将生成的相似度分数矩阵与原始文章数据的文章id进行关联
        :param score_csr_matrix: 相似度分数矩阵
        :param res_format: 生成推荐结果的格式，json or list
        :return:
        """
        src_article_num = self.src_article_num if self.src_article_num else len(self.dataframe)  # 数据的个数
        result = {} if res_format == "json" else []

        for index in xrange(src_article_num):  # 每一行的数据表示一篇文章跟其他文章的相似度分数列表

            sum_score_row = np.around(score_csr_matrix.getrow(index).toarray()[0], self.ndigits)
            # print sum_score_row
            single_score_row_list = [self.features_with_score.get(feature).getrow(index).toarray()[0]
                                     for feature in self.features]

            # 对分数为小数的进行位数取舍
            single_score_row_list = map(lambda arr: np.around(arr, self.ndigits), single_score_row_list)

            # 将推荐的文章id和相对应的相似度分数列表进行匹配
            rec_article_id_with_score = zip(self.dataframe["article_id"], sum_score_row, *single_score_row_list)

            # 按照总分来降序排序，选出排名 Top N
            recs = sorted(rec_article_id_with_score, key=lambda item: item[1], reverse=True)[0: self.rec_article_num]
            # 源文章id
            src_article_id = self.dataframe["article_id"].get(index)

            if res_format == "json":
                result[src_article_id] = recs
            else:
                tmp = [[src_article_id, ] + list(r) for r in recs]
                result.extend(tmp)

        return result


class CombineModel(object):
    def __init__(self, dataframe, similary, weights, ndigits=2, top=3):
        self.dataframe = dataframe
        self.similary = similary
        self.weights = weights
        self.score_csr_matrix = similary.calculate_similary()
        self.ndigits = ndigits
        self.top = top


    def map_articles(self):
        number = len(self.dataframe)  # 数据的个数
        result = {}

        # heat = scipy.matrix([self.dataframe["sum_collect_comment"].tolist()] * number)

        heat = self.weights.get("heat") * scipy.matrix(
            [np.around(self.dataframe["log_heat"].values, self.ndigits)] * number)

        score_simi = self.weights.get("similary") * self.score_csr_matrix
        # sum_score_mat = 0.7 * self.score_csr_matrix + 0.3 * heat
        sum_score_mat = score_simi + heat

        for index in xrange(number):  # 每一行的数据表示一篇文章跟其他文章的相似度分数列表

            sum_score_row = np.around(sum_score_mat[index].tolist()[0], self.ndigits)
            single_score_row_list = [self.similary.features_with_score.get(feature).getrow(index).toarray()[0]
                                     for feature in self.similary.features]
            single_score_row_list.append(np.array(heat[index].tolist()[0]))
            single_score_row_list.append(score_simi[index].toarray()[0])

            # 对分数为小数的进行位数取舍
            single_score_row_list = map(lambda arr: np.around(arr, self.ndigits), single_score_row_list)

            # 将推荐的文章id和相对应的相似度分数列表进行匹配
            rec_article_id_with_score = zip(self.dataframe["article_id"], sum_score_row, *single_score_row_list)

            # 按照总分来降序排序，选出排名 Top N
            recs = sorted(rec_article_id_with_score, key=lambda item: item[1], reverse=True)[0: self.top]

            # 源文章id
            src_article_id = unicode(self.dataframe["article_id"].get(index))

            result[src_article_id] = recs

        return result

    def map_articles_first(self):
        """
        将生成的相似度分数矩阵与原始文章数据的文章id进行关联，但是只匹配第一个文章id。
        :return:
        """
        number = len(self.dataframe)  # 数据的个数
        result = {}
        index = 0

        # heat = coo_matrix([np.around(self.dataframe[u"log_heat"].values, self.ndigits)] * number).tocsr()
        #
        # sum_score_mat = 0.7 * self.score_csr_matrix + 0.3 * heat
        #
        # sum_score_row = np.around(sum_score_mat.getrow(index).toarray()[0], self.ndigits)
        # single_score_row_list = [self.similary.features_with_score.get(feature).getrow(index).toarray()[0]
        #                          for feature in self.similary.features]
        # single_score_row_list.append(np.array(heat.getrow(index).toarray()[0]))

        heat = self.weights.get("heat") * scipy.matrix([self.dataframe["log_heat"].values] * number)

        score_simi = self.weights.get("similary") * self.score_csr_matrix
        # sum_score_mat = 0.7 * self.score_csr_matrix + 0.3 * heat
        sum_score_mat = score_simi + heat

        sum_score_row = np.around(sum_score_mat[index].tolist()[0], self.ndigits)

        single_score_row_list = [self.similary.features_with_score.get(feature).getrow(index).toarray()[0]
                                 for feature in self.similary.features]
        single_score_row_list.append(np.array(heat[index].tolist()[0]))
        single_score_row_list.append(score_simi[index].toarray()[0])

        # 对分数为小数的进行位数取舍
        single_score_row_list = map(lambda arr: np.around(arr, self.ndigits), single_score_row_list)

        # 将推荐的文章id和相对应的相似度分数列表进行匹配
        rec_article_id_with_score = zip(self.dataframe["article_id"], sum_score_row, *single_score_row_list)

        # 按照总分来降序排序，选出排名 Top N
        recs = sorted(rec_article_id_with_score, key=lambda item: item[1], reverse=True)[0: self.top]

        # 源文章id
        src_article_id = unicode(self.dataframe["article_id"].get(index))

        result[src_article_id] = recs

        return result


# cols_1 = [u"pro_id", u"level_4_id", u"level_3_id", u"level_2_id", u"level_1_id", u"brand_id", u"title_fc"]
# cols_2 = [u"sex", u"crowd"]
#
# weight_1 = [1.2, 1, 0.8, 0.6, 0.4, 0.2, 0.2]
# weight_2 = [0.2, 0.2]
#
# features_with_weight_1 = OrderedDict(zip(cols_1, weight_1))
# features_with_weight_2 = OrderedDict(zip(cols_2, weight_2))

cols = [u"pro_id", u"level_4_id", u"level_3_id", u"level_2_id", u"level_1_id", u"brand_id", u"title_fc", u"sex", u"crowd"]
weight = [1.2, 1, 0.8, 0.6, 0.4, 0.2, 0.2, 0.2, 0.2]

features_with_weight = OrderedDict(zip(cols, weight))
weights = {"similary": 0.85, "heat": 0.05}

if __name__ == "__main__":
    t0 = time.clock()
    # input_file = "../off_line_file/article_data_v4_2017-02-17-16-59_part.csv"
    input_file = "../off_line_file/title_fc_weight_part.csv"
    d = pd.read_csv(input_file, sep=",", encoding="utf_8")
    # d = pd.read_csv("../off_line_file/article_data_v5_2017-03-01-20-56.txt", sep="\t", encoding="utf8")
    print d.shape

    pp = PreProcess()
    data = pp(d)
    m1 = CountVectorizer(analyzer="word")
    m2 = CountVectorizer(analyzer="char")
    print m1.fit_transform(data.sex).toarray()
    print m1.get_feature_names()
