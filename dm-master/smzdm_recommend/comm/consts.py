# -*- coding: utf-8 -*-

"""
	业务相关的常量定义
"""
YOUHUI_CHANNEL = 'yh'
ZIXUN_CHANNEL = 'zx'
PINGCE_CHANNEL = 'pc'
YUANCHUANG_CHANNEL = 'yc'
WIKI_CHANNEL = 'wk'
XINRUI_CHANNEL = 'xr'
SHIPIN_CHANNEL = 'sp'

YOUHUI_CHANNEL_ID = 3
ZIXUN_CHANNEL_ID = 6
PINGCE_CHANNEL_ID = 7
YUANCHUANG_CHANNEL_ID = 11
WIKI_CHANNEL_ID = 14
XINRUI_CHANNEL_ID = 31
SHIPIN_CHANNEL_ID = 38

EDITOR_CHANNEL = 'editor'

# ES中文章的索引
ES_HOME_ARTICLE_INDEX = "home_article_index"

# 优惠channel_id映射
YOUHUI_CHANNEL_MAP = [1, 2, 5]

# 延迟时间key
DELAY_TIME_KEY = "delay_time:%s"

# 不感兴趣key
DISLIKE_KEY = "dislike:%s:%s"
HISTORY_KEY = "history:%s"
HISTORY_B_KEY = "history:b:%s"
HISTORY_B_ARTICLE_FILTER_LIST_KEY = "history_a_f:b:%s"

# 推荐数据缓存KEY
YH_RECOMMEND_KEY = "yh_recommend:%s"
YC_RECOMMEND_KEY = "yc_recommend:%s"
YH_HAVE_RECOMMEND_KEY = "yh_have_recommend:%s"
YC_HAVE_RECOMMEND_KEY = "yc_have_recommend:%s"
HAVE_RECOMMEND_COUNT_KEY = 300
HISTORY_B_COUNT = 300

# 用户偏好在Redis中的KEY
PREFER_DEVICE_REDIS_KEY = "prefer_device:%s"
PREFER_USER_REDIS_KEY = "prefer_user:%s"
YOUHUI_KEY = "youhui"
YUANCHUANG_KEY = "yuanchuang"
ACCURATE_KEY = "accurate"
BLUR_KEY = "blur"
PREFER_USER_KEY = "user"
PREFER_DEVICE_KEY = "device_id"
PREFER_TAG_KEY = "tag"
PREFER_BRAND_KEY = "brand"
PREFER_LEVEL_KEY = "level"
PARA = "para"

# 分流设备前缀
PREFER_SHUNT_DEVICE_KEY = "shunt"

# 置顶文章key
TOP_ARTICLE_KEY = "top_article"
TOP_ARTICLE_B_KEY = "top_article_b"

# 一页中的记录数
PAGE_SIZE = 16
PAGE_RECOMMEND_SIZE = 4
PAGE_TOTAL_SIZE = 20
RECOMMEND_B_SIZE = 10
RECOMMEND_B_MAX_SIZE = 60


# 每次获取新数据的页数
PAGE_NUM = 6

# 每次获取新数据的总数
PAGE_ALL_SIZE = PAGE_SIZE * PAGE_NUM + 1

# 用户上拉的最近一次时间
PULL_UP_LAST_TIME_PREFIX_KEY = "pull_up_last_time:%s"

# 用户下拉的最近一次时间
PULL_DOWN_LAST_TIME_PREFIX_KEY = "pull_down_last_time:%s"
PULL_DOWN_LAST_TIME_PREFIX_B_KEY = "pull_down_last_time_b:%s"

# 24小时
HOUR_CONST = 24

#
DOT = ","
VERTICAL_LINE = "|"
COLON = ":"
WELL = "#"

########################################################################################################################
# 基础权值常量
W_HALF_HOUR = "HALF_HOUR"
W_HOUR_1 = "HOUR_1"
W_HOUR_3 = "HOUR_3"
W_HOUR_12 = "HOUR_12"
W_HOUR_24 = "HOUR_24"
W_BRAND = "BRAND"
W_ACCURATE_CATE = "ACCURATE_CATE"
W_BLUR_CATE = "BLUR_CATE"
W_TAG = "TAG"
W_THRESHOLD = "THRESHOLD"
W_PORTRAIT_TOTAL = "PORTRAIT_TOTAL"
W_TIME_WEIGHT = "TIME_WEIGHT"
W_PORTRAIT_WIGHT = "PORTRAIT_WIGHT"
W_EDITOR_SYNC_TOTAL_WEIGHT = "EDITOR_SYNC_TOTAL_WEIGHT"
W_TOTAL = "TOTAL"

TOTAL = 10.0
# 时间总权重
TIME_TOTAL_WEIGHT = TOTAL * 0.4
# 用户画像总权重
PORTRAIT_TOTAL_WEIGHT = TOTAL * 0.4
# 编辑同步到首页总权重
EDITOR_SYNC_TOTAL_WEIGHT = TOTAL * 0.2

W = {
    W_EDITOR_SYNC_TOTAL_WEIGHT: 2.0,
    W_TIME_WEIGHT: {W_TOTAL: 4.0,
                    W_HALF_HOUR: TIME_TOTAL_WEIGHT,
					W_HOUR_1: TIME_TOTAL_WEIGHT * 4.0 / 5.0,
                    W_HOUR_3: TIME_TOTAL_WEIGHT * 3.0 / 5.0,
                    W_HOUR_12: TIME_TOTAL_WEIGHT * 2.0 / 5.0,
                    W_HOUR_24: TIME_TOTAL_WEIGHT / 5.0},
    
    W_PORTRAIT_WIGHT: {W_TOTAL: 4.0,
        W_BRAND: PORTRAIT_TOTAL_WEIGHT * 2.0 / 10.0,
                            W_ACCURATE_CATE: PORTRAIT_TOTAL_WEIGHT * 2.0 / 10.0,
                            W_BLUR_CATE: PORTRAIT_TOTAL_WEIGHT * 4.0 / 10.0,
                            W_TAG: PORTRAIT_TOTAL_WEIGHT * 2.0 / 10.0},
    W_THRESHOLD: 0.0,
}

DEFAULT_PREFER_WEIGHT = 0.05

########################################################################################################################
# 成人用品时间段
SEX_PRODUCT_START_TIME = "23:00:00"
SEX_PRODUCT_END_TIME = "06:00:00"


########################################################################################################################
# 工具集常量
VERSION_A = "a"
VERSION_B = "b"

ACTION_EDITOR = "editor"
ACTION_EDITOR_SORT = "editor_sort"
ACTION_EDITOR_REDIS = "editor_redis"
ACTION_DISLIKE = "dislike"
ACTION_PREFER_USER = "prefer_user"
ACTION_PREFER_DEVICE = "prefer_device"
ACTION_SHUNT_ADD_DEVICE = "shunt_add_device"
ACTION_SHUNT_CANCEL_DEVICE = "shunt_cancel_device"

TOOLS_CHANNEL_MAP = {
    YOUHUI_CHANNEL_ID: u'优惠',
    ZIXUN_CHANNEL_ID: u'资讯',
    PINGCE_CHANNEL_ID: u'评测',
    YUANCHUANG_CHANNEL_ID: u'原创',
    WIKI_CHANNEL_ID: u'wiki',
    XINRUI_CHANNEL_ID: u'新锐品牌',
    SHIPIN_CHANNEL_ID: u'视频',
}

TOOLS_SOURCE_MAP = {
    0: u"小编",
    1: u"推荐"
}

UNKNOWN = u'未知'

PREFER_SOURCE = u'偏好来源'
PREFER_SOURCE_DEVICE = u'偏好来源于设备'
PREFER_SOURCE_USER = u'偏好来源于用户'
TOOLS_CATE_PREFER = u'品类偏好'
TOOLS_TAG_PREFER = u'标签偏好'
TOOLS_BRAND_PREFER = u'品牌偏好'
TOOLS_ACCURATE_PREFER = u'优惠精确偏好'
TOOLS_BLUR_PREFER = u'优惠模糊偏好'
TOOLS_VARIATION_COEFFICIENT = u'优惠一级品类变异系数偏好'
TOOLS_YOUHUI = u'优惠'
TOOLS_YUANCHUANG = u'原创'

TOOLS_LEVEL_CONST = u'level'
TOOLS_BRAND_CONST = u'brand'
TOOLS_TAG_CONST = u'tag'
