# coding=utf-8


# 好价频道
HaoJiaFaXian = "2"
HaoJiaGuoNei = "1"
HaoJiaHaiTao = "5"


# 好文频道
ZuiXinYuanChuang = "11"

youhui = [HaoJiaFaXian, HaoJiaGuoNei, HaoJiaHaiTao]
yuanchaung = [ZuiXinYuanChuang, ]


####################################
# AB测试模块

AB_TEST_FLAG = True

# 根据trace_id或是根据device_id来分流
BASE_TRACE_ID = 1
BASE_DEVICE_ID = 2
AB_TEST_BASE = BASE_TRACE_ID


BUCKET_NUM = 100
# A属于小流量新算法
AB_TEST_RANGE_A = range(0, 0)
# B属于小流量当前算法
AB_TEST_RANGE_B = range(0, 0)
# C属于大流量当前算法
AB_TEST_RANGE_C = range(0, 100)

AB_TEST_RANGE_A_NAME = "new"
AB_TEST_RANGE_B_NAME = "master.small"
AB_TEST_RANGE_C_NAME = "master"


# A属于小流量新算法
AB_TEST_RANGE_A_2 = range(0, 100)
# B属于小流量当前算法
AB_TEST_RANGE_B_2 = range(20, 20)
# C属于大流量当前算法
AB_TEST_RANGE_C_2 = range(100, 100)

AB_TEST_RANGE_A_NAME_2 = "star.data"
AB_TEST_RANGE_B_NAME_2 = "asso.rule.small"
AB_TEST_RANGE_C_NAME_2 = "asso.rule"

####################################
# 历史文章计算
HISTORY_ARTICLE_FLAG = True








