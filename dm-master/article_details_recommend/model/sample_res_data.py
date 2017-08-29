# coding=utf-8

import datetime
import sys
import pandas as pd

###############################################
##        此代码用于抽样离线评估数据使用        ##
##               不用于生产环境               ##
###############################################


def sample_data(input_file1, input_file2=None, sample_number=500):
    """
    用于从input_file1和input_file2中同时抽取出sample_number条的原始文章的信息，
    要求保证这些原始文章信息在input_file1和input_file2中都存在
    如果input_file2=None,则只需要从input_file1中抽取出sample_number条的原始文章的信息。
    :param input_file1: 文件路径1
    :param input_file2: 文件路径2
    :param sample_number: 抽样的原始文章的个数
    :return:
    """
    data1 = pd.read_excel(input_file1)
    article_ids = pd.Series(data1.src_article.unique()).sample(sample_number, random_state=1).tolist()
    sam_data1 = data1[data1.src_article.isin(article_ids)]

    input_file1_arr = input_file1.split("/")
    input_file1_arr[-1] = unicode(sample_number) + "_" + input_file1_arr[-1]
    output_file1 = "/".join(input_file1_arr)

    sam_data1.to_excel(output_file1, index=False, encoding='utf_8_sig')

    if input_file2:
        data2 = pd.read_excel(input_file2)
        sam_data2 = data2[data2.src_article.isin(article_ids)]
        input_file2_arr = input_file2.split("/")
        input_file2_arr[-1] = unicode(sample_number) + "_" + input_file2_arr[-1]
        output_file2 = "/".join(input_file2_arr)
        sam_data2.to_excel(output_file2, index=False, encoding='utf_8_sig')

if __name__ == "__main__":
    # D:\work\article_details_recommend\off_line_file\res_data
    import os

    print ", ".join(os.listdir("../off_line_file/res_data"))

    input_file1 = u"../off_line_file/res_data/分词未去除特殊符号2017-05-10-16-50.xlsx"
    input_file2 = u"../off_line_file/res_data/分词去除特殊符号2017-05-10-16-49.xlsx"
    input_file1 = u"../off_line_file/res_data/推荐结果条数扩展成8条[用于评估阈值]2017-05-11-15-37.xlsx"

    args = sys.argv
    if len(args) >= 3:
        input_file1 = args[1]
        input_file2 = args[2]

    # sample_data(input_file1, input_file2, sample_number=500)
    sample_data(input_file1, input_file2=None, sample_number=500)


