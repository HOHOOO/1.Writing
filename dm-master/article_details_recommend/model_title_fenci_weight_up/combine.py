# coding=utf-8
import pstats
from collections import OrderedDict

import numpy as np
import pandas as pd
import os
import time

import scipy
from scipy.sparse import csr_matrix, spmatrix, coo_matrix
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from model.preprocessing import PreProcess


class Similary(object):

    def __init__(self, dataframe, word_features_with_weight,
                 char_features_with_weight, top=3, ndigits=2):
        """
        :param dataframe: 类型为 pandas.DataFrame 类型
        :param word_features_with_weight: 基于word生成词汇表的特征，类型为 OrderedDict
        :param char_features_with_weight: 基于char生成词汇表的特征，类型为 OrderedDict
        :param top: 指定每篇文章的推荐文章数目
        :param ndigits: 指定生成相似度得分时保留的小数点位数
        """
        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError("dataframe must be pandas.DataFrame!")
        if not isinstance(word_features_with_weight, OrderedDict):
            raise TypeError("word_features_with_weight must be OrderedDict!")
        if not isinstance(char_features_with_weight, OrderedDict):
            raise TypeError("char_features_with_weight must be OrderedDict!")

        self.dataframe = dataframe
        self.word_features_with_weight = word_features_with_weight
        self.word_features = list(zip(*word_features_with_weight.items())[0])

        self.char_features_with_weight = char_features_with_weight
        self.char_features = list(zip(*char_features_with_weight.items())[0])

        middle = word_features_with_weight.items()
        middle.extend(char_features_with_weight.items())
        self.features_with_weight = OrderedDict(middle)
        self.features = list(zip(*self.features_with_weight.items())[0])

        self.top = top
        self.ndigits = ndigits

        self.features_with_score = None

    def _generate_vector(self):
        """
        创建词汇表并生成文本向量
        """
        vect_word = CountVectorizer(analyzer="word")
        vect_char = CountVectorizer(analyzer="char")

        ser_csr_1 = self.dataframe[self.word_features].apply(lambda ser: vect_word.fit_transform(ser))
        ser_csr_2 = self.dataframe[self.char_features].apply(lambda ser: vect_char.fit_transform(ser))
        ser_csr = pd.concat([ser_csr_1, ser_csr_2])

        return ser_csr

    def calculate_similary(self):
        """
        根据特征的文本向量生成相似度矩阵
        """
        ser_csr = self._generate_vector()

        number = len(self.dataframe)  # 数据的个数

        # 计算相似度矩阵
        src_score_list = [
            cosine_similarity(ser_csr[feature], dense_output=False)
            * self.features_with_weight.get(feature)
            for feature in self.features]

        # 生成内容为各个特征及对应的相似度得分的一个字典
        self.features_with_score = dict(zip(self.features, src_score_list))
        print self.features_with_score["pro_id"].toarray()
        # 初始化一个csr矩阵
        sum_score = csr_matrix((number, number), dtype=np.float16)

        for feature in self.features:
            print ">>> calculate feature(%s) similary score <<<" % feature
            sum_score = sum_score + self.features_with_score.get(feature)

        sum_score.setdiag([-1] * number)  # 将自身向量与自身向量的相似度设为-1，即文章本身之间的相似度设为-1

        return sum_score

    def map_articles(self, score_csr_matrix):
        """
        将生成的相似度分数矩阵与原始文章数据的文章id进行关联
        :param score_csr_matrix: 相似度分数矩阵
        :return:
        """
        number = len(self.dataframe)  # 数据的个数
        result = {}

        for index in xrange(number):  # 每一行的数据表示一篇文章跟其他文章的相似度分数列表

            sum_score_row = np.around(score_csr_matrix.getrow(index).toarray()[0], self.ndigits)
            # print sum_score_row
            single_score_row_list = [self.features_with_score.get(feature).getrow(index).toarray()[0]
                                     for feature in self.features]

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

    def map_articles_first(self, score_csr_matrix):
        """
        将生成的相似度分数矩阵与原始文章数据的文章id进行关联，但是只匹配第一个文章id。
        :param score_csr_matrix: 相似度分数矩阵
        :return:
        """
        result = {}
        index = 0

        sum_score_row = np.around(score_csr_matrix.getrow(index).toarray()[0], self.ndigits)

        single_score_row_list = [self.features_with_score.get(feature).getrow(index).toarray()[0]
                                 for feature in self.features]

        # print "single_score_row_list: %s" % single_score_row_list
        # 对分数为小数的进行位数取舍
        single_score_row_list = map(lambda arr: np.around(arr, self.ndigits), single_score_row_list)
        # 将推荐的文章id和相对应的相似度分数列表进行匹配
        rec_article_id_with_score = zip(self.dataframe["article_id"], sum_score_row, *single_score_row_list)

        # print "rec_article_id_with_score: %s" % rec_article_id_with_score

        # 按照总分来降序排序，选出排名 Top N
        recs = sorted(rec_article_id_with_score, key=lambda item: item[1], reverse=True)[0: self.top]

        # 源文章id
        src_article_id = unicode(self.dataframe["article_id"].get(index))

        result[src_article_id] = recs
        # print "src_article_id: %s" % src_article_id
        # print "recs: %s" % recs

        return result


class CombineModel(object):
    def __init__(self, dataframe, similary, weights, ndigits=2, top=3):
        self.dataframe = dataframe
        self.similary = similary
        self.weights = weights
        self.score_csr_matrix = similary.calculate_similary()
        self.ndigits = ndigits
        self.top = top
        # pp = PreProcess(dataframe)
        # self.dataframe = pp.process_data()
        # print "process data: %.2f" % (t2 - t1)
        # simi = Similary(self.dataframe, word_features_with_weight, char_features_with_weight, top=6)

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


cols_1 = [u"pro_id", u"level_4_id", u"level_3_id", u"level_2_id", u"level_1_id", u"brand_id", u"title_fc"]
cols_2 = [u"sex", u"crowd"]

weight_1 = [1.2, 1, 0.8, 0.6, 0.4, 0.2, 0.2]
weight_2 = [0.2, 0.2]

word_features_with_weight = OrderedDict(zip(cols_1, weight_1))
char_features_with_weight = OrderedDict(zip(cols_2, weight_2))

weights = {"similary": 0.85, "heat": 0.05}


class StartModel(object):
    def __init__(self, data=None, input_file=None):

        if data is not None:
            pp = PreProcess(data)
            self.data = pp.process_data()
        else:
            file = input_file if input_file else "./off_line_file/article_data_v5_2017-03-01-20-56.txt"
            data = pd.read_csv(file, sep="\t", encoding="utf8")
            data = data.head(15)

            pp = PreProcess(data)
            self.data = pp.process_data()

    @property
    def simi_features(self):
        features = []
        features += cols_1
        features += cols_2
        return features

    def run_similary(self):
        simi = Similary(self.data, word_features_with_weight, char_features_with_weight)
        res_csr_matrix = simi.calculate_similary()
        result = simi.map_articles(res_csr_matrix)
        return result

    def run_similary_first(self):
        simi = Similary(self.data, word_features_with_weight, char_features_with_weight)
        res_csr_matrix = simi.calculate_similary()
        result = simi.map_articles_first(res_csr_matrix)
        return result

    def run_combine(self):
        simi = Similary(self.data, word_features_with_weight, char_features_with_weight)
        cm = CombineModel(self.data, simi, weights)
        return cm.map_articles()

    def run_combine_first(self):
        simi = Similary(self.data, word_features_with_weight, char_features_with_weight)
        cm = CombineModel(self.data, simi, weights)
        result = cm.map_articles_first()
        return result


if __name__ == "__main__":
    t0 = time.clock()
    d = pd.read_csv("../off_line_file/article_data_v5_2017-03-01-20-56.txt", sep="\t", encoding="utf8")
    d = d.head(10)
    start_model = StartModel(d)
    # start_model = StartModel(input_file="../off_line_file/article_data_v5_2017-03-01-20-56.txt")

    # print start_model.data.head(10)
    r1 = start_model.run_similary()
    # r1 = start_model.run_similary_first()
    print "r1: %s" % r1
    print "r1.items: %s" % r1.items()

    # r2 = start_model.run_combine()
    # r2 = start_model.run_combine_first()
    # t1 = time.clock()
    # print "r2: %s" % r2
    # print "r2.items: %s" % r2.items()
    # print "run time: %.2f" % (t1 - t0)