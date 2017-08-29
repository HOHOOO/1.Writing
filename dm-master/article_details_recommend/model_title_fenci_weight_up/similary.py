# coding=utf-8
from collections import OrderedDict

import numpy as np
import pandas as pd
import os
import time
from scipy.sparse import csr_matrix, spmatrix
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from model.preprocessing import PreProcess
from base.config import load, Config


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

        middle = char_features_with_weight.items()
        middle.extend(word_features_with_weight.items())
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

        # 初始化一个csr矩阵
        sum_score = csr_matrix((number, number), dtype=np.float64)

        for feature in self.features:
            print ">>> calculate feature(%s) similary score <<<" % feature
            sum_score += self.features_with_score.get(feature)

        sum_score.setdiag([-1] * number)  # 将自身向量与自身向量的相似度设为-1，即文章本身之间的相似度设为-1

        return sum_score

    def _hold_digit(self, arr, ndigits):
        """
        将序列中的每一个小数的位数转为指定位数
        :param arr: 类型：array-like
        :param ndigits: 小数保留的位数
        :return: 转换后的序列
        """
        return map(lambda x: round(x, ndigits), arr)

    def map_articles(self, score_csr_matrix):
        """
        将生成的相似度分数矩阵与原始文章数据的文章id进行关联
        :param score_csr_matrix: 相似度分数矩阵
        :return:
        """
        number = len(self.dataframe)  # 数据的个数
        result = {}

        for index in xrange(number):  # 每一行的数据表示一篇文章跟其他文章的相似度分数列表

            sum_score_row = self._hold_digit(score_csr_matrix.getrow(index).toarray()[0], self.ndigits)

            single_score_row_list = [self.features_with_score.get(feature).getrow(index).toarray()[0]
                                     for feature in self.features]

            # 对分数为小数的进行位数取舍
            single_score_row_list = map(lambda arr: self._hold_digit(arr, self.ndigits), single_score_row_list)
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
        将生成的相似度分数矩阵与原始文章数据的文章id进行关联
        :param score_csr_matrix: 相似度分数矩阵
        :return:
        """
        number = len(self.dataframe)  # 数据的个数
        result = {}
        index = 0

        sum_score_row = self._hold_digit(score_csr_matrix.getrow(index).toarray()[0], self.ndigits)

        single_score_row_list = [self.features_with_score.get(feature).getrow(index).toarray()[0]
                                 for feature in self.features]

        # sum_score_row = self._hold_digit(score_csr_matrix.getrow(index).todense()[0], self.ndigits)
        #
        # single_score_row_list = [self.features_with_score.get(feature).getrow(index).todense()[0]
        #                          for feature in self.features]

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

cols_1 = [u"pro_id", u"level_4_id", u"level_3_id", u"level_2_id", u"level_1_id", u"brand_id", u"title_fc"]
cols_2 = [u"sex", u"crowd"]

weight_1 = [1.2, 1, 0.8, 0.6, 0.4, 0.2, 0.2]
weight_2 = [0.2, 0.2]

word_features_with_weight = OrderedDict(zip(cols_1, weight_1))
char_features_with_weight = OrderedDict(zip(cols_2, weight_2))


class StartModel(object):

    def __init__(self,):
        pass

    @property
    def features(self):
        features = []
        features += cols_1
        features += cols_2
        return features

    def run_recent(self):
        # input_file = "./off_line_file/article_data_v4_2017-02-17-16-59.txt"
        input_file = "../off_line_file/article_data_v5_2017-03-01-20-56.txt"
        data = pd.read_csv(input_file, sep="\t", encoding="utf8")
        data = data.head(10)

        pp = PreProcess(data)
        data = pp.process_data()

        simi = Similary(data, word_features_with_weight, char_features_with_weight, top=6)

        # print simi.features_with_weight
        res_csr_matrix = simi.calculate_similary()

        return simi.map_articles(res_csr_matrix)

    def run_historical(self):
        pass


if __name__ == "__main__":
    t0 = time.time()
    # input_file = "../off_line_file/article_data_v4_2017-02-17-16-59.txt"
    input_file = "../off_line_file/article_data_v5_2017-03-01-20-56.txt"
    print os.path.abspath(input_file)
    data = pd.read_csv(input_file, sep="\t", encoding="utf8")
    # data = data.head(2001)
    t1 = time.time()
    print "read data: %.2f" % (t1 - t0)
    print "data.length : %s" % len(data)
    pp = PreProcess(data)
    data = pp.process_data()
    t2 = time.time()
    print "process data: %.2f" % (t2 - t1)
    simi = Similary(data, word_features_with_weight, char_features_with_weight, top=3)

    res_csr_matrix = simi.calculate_similary()
    t3 = time.time()
    print "calculate matrix: %.2f" % (t3 - t2)
    simi.map_articles_first(res_csr_matrix)
    t4 = time.time()
    print "map time: %.2f" % (t4-t3)
    print "run time: %.2f" % (t4 - t0)
    # print data.pro_id
    # ser_csr = simi._generate_vector()
    # print ser_csr
    # print ser_csr[0].toarray()