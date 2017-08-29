# coding=utf-8
import datetime
import pandas as pd
import time

from model_title_fenci_stop_words.model import features_with_weight, SimilaryModel
from model_title_fenci_stop_words.preprocessing import PreProcess

###############################################
##        此代码用于评估离线训练数据使用        ##
##               不用于生产环境               ##
###############################################


def construct_frame(res_list, res_cols):
    return pd.DataFrame(data=res_list, columns=res_cols)


def add_article_attrs(dataframe, associate_attrs, flag="src"):
    left_on_key = "src_article" if flag=="src" else "rec_article"
    src_col_list = dataframe.columns.tolist()
    src_col_list.extend(associate_attrs)
    dataframe = dataframe.merge(data, left_on=left_on_key, right_on="article_id")
    cols_map = dict(zip(associate_attrs, map(lambda s: flag + "_" + s, associate_attrs)))
    dataframe.rename_axis(cols_map, axis=1, inplace=True)
    return dataframe


def sort_frame(dataframe, sorted_cols):
    dataframe = dataframe[sorted_cols]
    dataframe = dataframe.sort_values(by=["src_article", "score_sum"], ascending=False)
    return dataframe


def output_frame(dataframe, file_name):
    dd = datetime.datetime.now()
    stamp = dd.strftime("%Y-%m-%d-%H-%M")

    file_name_xlsx = file_name + stamp + ".xlsx"
    file_name_csv = file_name + stamp + ".csv"
    dataframe.to_excel(file_name_xlsx, encoding="utf_8_sig", index=False, sheet_name=u"推荐结果")
    dataframe.to_csv(file_name_csv, encoding="utf_8_sig", index=False, sheet_name=u"推荐结果")

if __name__ == "__main__":
    # input_file = "../off_line_file/article_data_v5_2017-03-01-20-56.txt"
    # data = pd.read_csv(input_file, sep="\t", encoding="utf_8_sig")
    # input_file = "../off_line_file/title_fc_weight_part.csv"
    # data = pd.read_csv(input_file, sep=",", encoding="utf_8_sig")
    # input_file = "../off_line_file/title_fc_weight_part.xlsx"
    # data = pd.read_excel(input_file, encoding="utf_8_sig")
    input_file = "../off_line_file/train_data/article_data_yh_2017-05-09-18-36.txt"
    data = pd.read_csv(input_file, sep="\t", encoding="utf_8_sig")
    print data.shape
    # 保留一份 excel 格式文件
    # data.to_excel(input_file.replace("txt", "xlsx"), encoding="utf_8_sig")

    pp = PreProcess()
    data = pp(data)

    title_fc_extra_weight = None
    filename = u"../off_line_file/res_data/分词未去除特殊符号"
    # filename = u"../off_line_file/res_data/测试分词2"
    # filename = u"../off_line_file/res_data/原始标题权重结果"
    # title_fc_extra_weight = 8
    # filename = u"../off_line_file/res_data/加权(x8)标题权重结果"

    model = SimilaryModel(data, features_with_weight, src_article_num=None,
                          rec_article_num=3, title_fc_extra_weight=title_fc_extra_weight,
                          ndigits=2)

    res_csr_matrix = model.calculate_similary()
    res = model.map_articles(res_csr_matrix, res_format="list")

    # 推荐结果中对应的字段
    res_cols = [u"src_article", u"rec_article", u"score_sum",
                u"score_pro", u"score_level4", u"score_level3",
                u"score_level2", u"score_level1", u"score_brand",
                u"score_title_fc", u"score_sex", u"score_crowd"]

    # 需要关联的字段
    associate_cols = [u"pro_id", u"level_1", u"level_2", u"level_3", u"level_4", u"brand", u"title", u"title_fc", u"sex", u"crowd", u"pubdate"]

    # 排序后的字段
    sorted_cols = [u'src_article', u'rec_article', u'src_title', u'rec_title', u'score_sum', u'score_pro',
                   u'src_pro_id', u'rec_pro_id',
                   u'score_level4', u'src_level_4', u'rec_level_4', u'score_level3', u'src_level_3', u'rec_level_3',
                   u'score_level2', u'src_level_2', u'rec_level_2', u'score_level1', u'src_level_1', u'rec_level_1',
                   u'score_brand', u'src_brand', u'rec_brand', u'score_title_fc', u'src_title_fc', u'rec_title_fc',
                   u'score_sex', u"src_sex", u"rec_sex", u'score_crowd', u"src_crowd", u"rec_crowd"]

    recommend = construct_frame(res, res_cols)
    recommend = add_article_attrs(recommend, associate_cols, flag="src")
    recommend = add_article_attrs(recommend, associate_cols, flag="rec")
    recommend = sort_frame(recommend, sorted_cols)

    output_frame(recommend, filename)
