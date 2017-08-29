# coding=utf-8

import array
import random
from collections import Mapping
from collections import defaultdict

import numpy as np
import scipy.sparse as sp
from sklearn.feature_extraction.text import CountVectorizer


def _make_int_array():
    """Construct an array.array of a type suitable for scipy.sparse indices."""
    return array.array(str("i"))


def transform(documents, vocabulary=None):
    """
    :param documents:需要向量化的文档列表，文档列表里的每个元素也是一个列表，表示每个文档的多个属性
    :param vocabulary:向量化时基于该vocabulary来进行，如果为None，则会从documents中生成
    :return: 返回向量化后的文档矩阵以及vocabulary
    """

    fixed_vocab = True
    if not vocabulary:
        # vocabulary = dict(zip(set(vocabulary), range(0, len(vocabulary))))
        vocabulary = defaultdict()
        vocabulary.default_factory = vocabulary.__len__
        fixed_vocab = False

    if isinstance(vocabulary, set):
        vocabulary = sorted(vocabulary)

    if not isinstance(vocabulary, Mapping):
        vocab = {}
        for i, t in enumerate(vocabulary):
            if vocab.setdefault(t, i) != i:
                msg = "Duplicate term in vocabulary: %r" % t
                raise ValueError(msg)
        vocabulary = vocab

    j_indices = _make_int_array()
    indptr = _make_int_array()
    indptr.append(0)

    for document in documents:
        # print "document: %s" % document
        for feature in document:
            # print "feature: %s" % feature
            try:
                j_indices.append(vocabulary[feature])
            except KeyError:
                # Ignore out-of-vocabulary items for fixed_vocab=True
                continue

        indptr.append(len(j_indices))

    frombuffer_empty = np.frombuffer

    j_indices = frombuffer_empty(j_indices, dtype=np.intc)
    indptr = np.frombuffer(indptr, dtype=np.intc)
    values = np.ones(len(j_indices))

    X = sp.csr_matrix((values, j_indices, indptr),
                      shape=(len(indptr) - 1, len(vocabulary)),
                      dtype=np.int64)

    X.sum_duplicates()

    return X, dict(vocabulary)


def calculate_score(raw_documents, preference, costom_vocabulary=None):
    """
    :param raw_documents: 需要排序的文章属性列表，文档列表里的每个元素也是一个列表，表示每个文档的多个属性
    :param preference: 用户偏好的的属性
    :param article_ids: 文章id列表，分别对应每一个文章raw_documents
    :param costom_vocabulary: 自定义向量化时所用的词库表
    :return: 返回排序后的文章id及其分数
    """

    X, costom_vocabulary = transform(raw_documents, costom_vocabulary)  # 将原始文档向量化
    y, costom_vocabulary = transform(preference, costom_vocabulary)  # 将用户偏好向量化

    print X.toarray(), X.shape
    print y.toarray(), y.shape
    score = X.dot(y.T)
    score = score.toarray().T[0]

    return score


def sort_article_ids(raw_documents, preference, article_ids, costom_vocabulary=None):
    """
    :param raw_documents: 需要排序的文章属性列表，文档列表里的每个元素也是一个列表，表示每个文档的多个属性
    :param preference: 用户偏好的的属性
    :param article_ids: 文章id列表，分别对应每一个文章raw_documents
    :param costom_vocabulary: 自定义向量化时所用的词库表
    :return: 返回排序后的文章id及其分数
    """
    score = calculate_score(raw_documents, preference, costom_vocabulary)
    d = np.column_stack((article_ids, score))
    print score
    print d

    #     df = pd.DataFrame(d)  # 排序
    #     return df.sort_values(by=1, ascending=False)
    return sorted(d, key=lambda item: item[1], reverse=True)

if __name__ == "__main__":
    article_num = 600  # 文章总数
    tag_num = 1500  # 标签总数

    per_article_associate_tag_min_num = 1  # 每篇文章关联的标签最小数量（至少为1）
    per_article_associate_tag_max_num = 5  # 每篇文章关联的标签最大数量

    user_preference_tag_min_num = 1  # 用户偏好的标签最小数量（至少为1）
    user_preference_tag_max_num = 10  # 用户偏好的标签最大数量

    # 文章id是顺序生成的，品牌id是随机生成的
    article_id_min = 3000  # 虚构的文章id最小值
    tag_id_min = 1  # 虚构的标签id最小值

    # 构造一个顺序的文章列表，总数为 article_num
    fake_article_ids = np.arange(article_id_min, article_id_min + article_num)
    # 构造一个顺序的标签列表,总数为 tag_num
    fake_tag_ids = np.arange(tag_id_min, tag_id_min + tag_num)
    # 为每篇文章关联 per_article_associate_tag_min_num ~ per_article_associate_tag_max_num 之间的标签个数，其中每个标签随机的从构造出的标签列表中选择
    fake_per_article_tag_ids = np.array([random.sample(fake_tag_ids, random.randint(per_article_associate_tag_min_num,
                                                                                    per_article_associate_tag_max_num))
                                         for i in range(article_num)])

    # 为每个用户生成 user_preference_tag_min_num ~ user_preference_tag_max_num 之间的数量的标签偏好
    fake_user_preference_tag_ids = np.array([random.sample(fake_tag_ids, random.randint(user_preference_tag_min_num, user_preference_tag_max_num))])
    # d = sort_article_ids(fake_per_article_tag_ids.tolist(), fake_user_preference_tag_ids.tolist(), fake_article_ids.tolist())
    # print d[0: 3]

    """
    测试正确性:
    根据用户偏好，匹配出的用户喜欢的文章应该是标签为[3,7]/[3,5]/[7]的文章，即文章114/112/113
    """
    raw_documents = [[3, 5], [7], [9, 10], [3, 7]]
    article_ids = [111, 112, 113, 114]
    preference = [[3, 7]]
    # preference = [[12, 13]]
    # costom_vocabulary = [3, 5, 7, 9, 10, 12]

    d = sort_article_ids(raw_documents, preference, article_ids)
    print d


