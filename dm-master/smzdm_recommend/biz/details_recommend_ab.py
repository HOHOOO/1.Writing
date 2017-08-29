# coding=utf-8
import hashlib

from tornado.log import gen_log

from biz_config.details_recommend import *
from biz_config.details_recommend_ab import *


class ABModule(object):
    """
    详情页推荐AB测试模块
    """

    def __init__(self, device_id, trace_id, channel_id):
        self.device_id = device_id
        self.trace_id = trace_id

        self.ab_base = ""
        self.bucket = -1

        self.algor_row1 = None
        self.algor_row2 = None
        self.algor_row3 = None

        if channel_id in YOU_HUI_CHANNEL_ID_LIST:
            self.ab_base = YOU_HUI_AB_BASE
            self.algor_row1 = self._generate_ab(YOU_HUI_AB_ROW1)
            self.algor_row2 = self._generate_ab(YOU_HUI_AB_ROW2)
            self.algor_row3 = self._generate_ab(YOU_HUI_AB_ROW3)

        elif channel_id == YUAN_CHUANG_CHANNEL_ID:
            self.ab_base = YUAN_CHUANG_AB_BASE
            self.algor_row1 = self._generate_ab(YUAN_CHUANG_AB_ROW1)
            self.algor_row2 = self._generate_ab(YUAN_CHUANG_AB_ROW2)
            self.algor_row3 = self._generate_ab(YUAN_CHUANG_AB_ROW3)
        else:
            self.ab_base = HAO_WU_AB_BASE
            self.algor_row1 = self._generate_ab(HAO_WU_AB_ROW1)
            self.algor_row2 = self._generate_ab(HAO_WU_AB_ROW2)
            self.algor_row3 = self._generate_ab(HAO_WU_AB_ROW3)

        self.algors = {"row1": self.algor_row1,
                       "row2": self.algor_row2,
                       "row3": self.algor_row3}

    def _cut_bucket(self, sts, bucket_num=100):
        """
        对sts生成16进制的MD5值，然后将该值转为10进制，将转换后的数字对bucket_num求余
        :param sts:
        :param bucket_num:
        :return:
        """
        hash = hashlib.md5()
        hash.update(sts)
        md5_sts = hash.hexdigest()
        # md5_sta_num = long("".join([str(ord(s)) for s in md5_sts]))
        md5_sta_num = int(md5_sts, 16)
        return md5_sta_num % bucket_num

    def _generate_ab(self, ab_row):
        num = 100
        algorithm_dict = {}

        for k, v in ab_row.iteritems():
            # 使用分流
            sts = self.trace_id if self.ab_base == "trace_id" else self.device_id
            bucket = self._cut_bucket(sts, num)
            self.bucket = bucket
            # print "k: %s" % k
            # print "v: %s" % v

            # ab开关打开
            if v.get("flag") is True:
                gen_log.info("bucket: %s" % bucket)
                ratio = v.get("ratio")
                divi_2 = int(num * ratio)
                divi_1 = divi_2 / 2

                # name标志算法名字，base用于标志算法名字及所处流量
                if bucket in xrange(divi_2, num):
                    # A算法主流量
                    name = v.get("A").get("name")
                    base = name
                elif bucket in xrange(divi_1, divi_2):
                    # A算法小流量
                    name = v.get("A").get("name")
                    base = name + "." + v.get("A").get("small_flow_suffix")

                else:
                    # xrange(0, divi_1)
                    # B算法小流量
                    name = v.get("B").get("name")
                    base = name

                algorithm_dict[k] = {"name": name, "base": base}

            # ab开关关闭，直接使用A算法
            elif v.get("flag") is False:
                name = v.get("A").get("name")
                base = name
                algorithm_dict[k] = {"name": name, "base": base}

            else:
                pass

        return algorithm_dict

    def __repr__(self):

        # text = "[ab_base]: %s, [ab_bucket]: %s, [algor_row1]: %s, [algor_row2]: %s, [algor_row3]: %s" % (
        #     self.ab_base, self.bucket, self.algor_row1, self.algor_row2, self.algor_row3)
        text = "[ab_base]: %s, [ab_bucket]: %s, [algors]: %s" % (
            self.ab_base, self.bucket, self.algors)
        return text


if __name__ == "__main__":
    print ABModule("eeef66cac314e48281d1d09789e0f645", "47KpLUGi-yewRNgSO-lx8-32wi", 11)
    print ABModule("eeef66cac314e48281d1d09789e0f645", "47KpLUGi-yewRNgSO-lx8-32wi", 1)
