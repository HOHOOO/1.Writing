# coding=utf-8

REC_MIN_NUM = 3
REC_MAX_NUM = 10
REC_DATA_DAY = 1
REC_TOUR_DATA_DAY = 7
THRESHOLD_SIMI_SCORE = 0

OUTPUT_LEVEL_1 = 1  # 最高输出级别，输出信息最多
OUTPUT_LEVEL_2 = 2


# 默认不同特征的相似度权重
DEFAULT_SIMI_WEIGHTS = {
    "pro_id": 1.2,
    "level_4_id": 1.0,
    "level_3_id": 0.8,
    "level_2_id": 0.6,
    "level_1_id": 0.4,
    "brand_id": 0.2,
    "title": 0.2,
    "sex": 0.2,
    "crowd": 0.2,
}

# 默认的额外权重
DEFAULT_SIMI_EXTRA_WEIGHTS = {
    "level_3_defect_title_extra": 8  # 文章三级品类缺失时需要对其他文章
}


DEFAULT_COMMBINE_WEIGHTS = {
    "simi": 0.85,
    "heat": 0.15
}

# 默认的特征名与特征名分数映射关系
FEATURE_SCORE_NAME = {
    "pro_id": "score_pro",
    "level_4_id": "score_level_4",
    "level_3_id": "score_level_3",
    "level_2_id": "score_level_2",
    "level_1_id": "score_level_1",
    "brand_id": "score_brand",
    "title": "score_title",
    "sex": "score_sex",
    "crowd": "score_crowd",
}


# 成人用品分类id
SEX_PRODUCT_LEVEL_ID = 127


