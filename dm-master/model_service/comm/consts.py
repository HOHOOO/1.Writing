# -*- coding: utf-8 -*-

"""
	业务相关的常量定义
"""

#
YOUHUI_CHANNEL = 'yh'
ZIXUN_CHANNEL = 'zx'
PINGCE_CHANNEL = 'pc'
YUANCHUANG_CHANNEL = 'yc'
WIKI_CHANNEL = 'wk'
XINRUI_CHANNEL = 'xr'
SHIPIN_CHANNEL = 'sp'

# 优惠channel_id映射
YOUHUI_CHANNEL_MAP = [1, 2, 5]
YUANCHUANG_CHANNEL_MAP = [11, ]


########################################################################################################################
# 特殊符号
SINGLE_QUOTE = "'"
DOT = ","
VERTICAL_LINE = "|"
COLON = ":"
TIME_FORMAT_1 = "%Y-%m-%d %H:%M:%S"

########################################################################################################################
# REDIS相关常量
STAR_DATA_RES_KEY = "star_data_res:%s"
STAR_DATA_RES_KEY_EXPIRE = 2 * 60 * 60
YOUHUI_BASE_SIMI_RES_KEY = "yh.%s.yh_sim_rec"
YOUHUI_BASE_SIMI_RES_KEY_EXPIRE =1 * 60 * 60


########################################################################################################################
# ES中文章的索引
ES_HOME_ARTICLE_INDEX = "home_article_index"

########################################################################################################################
# MYSQL中表名
# 首页用户不感兴趣内容回传表名
USER_DISLIKE_CONTENT_TABLE = "user_dislike_content"
# 品类信息表
SMZDM_CATEGORY = "sync_smzdm_product_category"
# 品牌信息表
SMZDM_BRAND = "sync_smzdm_brand"
# 标签信息表
SMZDM_TAG = "sync_smzdm_tag_type"
# 优惠主表
YOUHUI_PRIMARY = "sync_youhui_primary"
# 优惠标签表
YOUHUI_TAG = "sync_youhui_tag_type_item"

########################################################################################################################

