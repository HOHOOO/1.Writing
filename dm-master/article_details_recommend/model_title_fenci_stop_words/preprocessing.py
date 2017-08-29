# coding=utf-8
import platform

import jieba
import jieba.analyse
import re
import numpy as np

from tools.stop_words import SIGNAL_STOP_WORDS

# brand_dict_path = "../file/brand_dict.txt"
# cate_dict_path = "../file/cate_dict.txt"
# mall_dict_path = "../file/mall_dict.txt"
# jieba.load_userdict(brand_dict_path)
# jieba.load_userdict(cate_dict_path)
# jieba.load_userdict(mall_dict_path)

# stop_words_path = "../file/stop_words.txt"
# jieba.analyse.set_stop_words(stop_words_path)
jieba.initialize()


class PreProcess(object):

    def __call__(self, data):
        # data[u"pro_id"] = data[u"pro_id"]
        # data[u"level_1_id"] = data[u"level_1_id"].map(lambda id: str(id) if id != -1 else "")
        # data[u"level_2_id"] = data[u"level_2_id"].map(lambda id: str(id) if id != -1 else "")
        # data[u"level_3_id"] = data[u"level_3_id"].map(lambda id: str(id) if id != -1 else "")
        # data[u"level_4_id"] = data[u"level_4_id"].map(lambda id: str(id) if id != -1 else "")
        # data[u"brand_id"] = data[u"brand_id"].fillna(value=-1).map(lambda id: str(id) if id != -1 else "")
        # data[u"level_3_id_is_null"] = np.where(data[u"level_3_id"] == "", 1, 0)

        data[u"pro_id"] = data[u"pro_id"].fillna(value=-1).map(lambda id: str(id) if id != -1 else "")
        data[u"level_1_id"] = data[u"level_1_id"].map(lambda id: str(id) if id != -1 else "")
        data[u"level_2_id"] = data[u"level_2_id"].map(lambda id: str(id) if id != -1 else "")
        data[u"level_3_id"] = data[u"level_3_id"].map(lambda id: str(id) if id != -1 else "")
        data[u"level_4_id"] = data[u"level_4_id"].map(lambda id: str(id) if id != -1 else "")
        data[u"brand_id"] = data[u"brand_id"].fillna(value=-1).map(lambda id: str(id) if id != -1 else "")
        # TODO: 添加热度测试
        try:
            data[u"log_heat"] = np.log1p(data[u"sum_collect_comment"])
            # 归一化
            data[u"log_heat"] = (data[u"log_heat"] - data[u"log_heat"].min())/(data[u"log_heat"].max() - data[u"log_heat"].min())
            # data[u"sum_collect_comment"] = np.log1p(data[u"sum_collect_comment"])
            # data[u"sum_collect_comment"] = data["sum_collect_comment"].map(lambda x: np.math.log10(x + 1))
        except Exception as e:
            print e

        # if platform.system() == "Linux":
        #     jieba.enable_parallel(8)  # 并行执行分词任务，Linux环境支持、Windows环境不支持
        # data[u"title_fc"] = data[u"title"]

        data[u"title"] = data[u"title"].map(lambda s: re.sub("\n", " ", s.lower()) if s else "")
        sen = "\n".join(data[u"title"].values)

        res = (",".join(jieba.cut(sen)))
        # res = ",".join([v for v in jieba.cut(sen) if v not in SIGNAL_STOP_WORDS])
        data[u"title_fc"] = res.split("\n")  # 对标题进行分词

        data[u"sex"] = data[u"title"].map(lambda s: self._find_sex(unicode(s)))
        data[u"crowd"] = data[u"title"].map(lambda s: self._find_crowd(unicode(s)))
        return data

    def _find_sex(self, s):
        """
        找到字符串中包含的性别属性词，并返回。
        既包含男、又包含女和男、女都不包含返回空字符串；
        只包含男或只包含女则返回男或女字符串
        :param s:
        :return:
        """
        if u"男" in s and u"女" in s:
            return ""
        elif u"男" in s:
            return u"男"
        elif u"女" in s:
            return u"女"
        else:
            return ""

    def _find_crowd(self, s):
        """
        找到字符串中包含的人群属性词，并返回
        :param s:
        :return:
        """
        base = u"婴" in s or u"幼" in s
        if u"童" in s and not base:
            return u"童"
        if base and not (u"幼犬" in s or u"幼猫" in s):
            return u"婴/幼"
        else:
            return ""
