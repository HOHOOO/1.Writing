# coding=utf-8
# 明星数据业务相关配置

HOUR_12 = 12
HOUR_24 = 24
DAY_3 = 3
DAY_7 = 7

REC_MIN_NUM = 3
REC_MAX_NUM = 8
REC_DATA_DAY = 7


YH_DANPIN_TYPE = "youhui"
COLLECT_GOOD_PRICE_MIN = 30.0
COLLECT_GOOD_TAG = "凑单品"

RULE_YH_TYPE = "yh_type"
RULE_TAG_1 = "tag_1"
RULE_TAG_2 = "tag_2"
RULE_TAG_3 = "tag_3"
RULE_COLLECT_GOOD = "collect_goods"
RULE_MALL_RANK_LIST = "mall_rank_list"
RULE_CATE_RANK_LIST = "cate_rank_list"


# RULE 包含了不同类型的规则，涉及到规则的名称和该规则对应的推荐模块名称
RULE = {
    RULE_YH_TYPE: {
        "huodong": "促销活动排行",
        "quan": "优惠券排行"
    },

    RULE_TAG_1: {
        # "奇葩物": "更多奇葩物",
        "高端秀": "更多高端秀",
        "游戏厅": "更多游戏厅",
        "新品发售": "更多新品发售"
    },

    RULE_TAG_2: {
        "京东PLUS会员": "京东PLUS会员合辑",
        "中亚Prime会员": "中亚Prime会员合辑",
        "日用品囤货": "日用品囤货合辑",
        "免费得": "免费得排行",
        "白菜党": "白菜党排行",
        "值友专享": "更多值友专享",
        "养车囤货": "养车囤货合辑"
    },

    RULE_TAG_3: {
        # "白菜汇总": "更多白菜汇总",
        "白菜包邮": "更多白菜包邮",
        "19块9包邮": "更多19块9包邮",
        "9块9包邮": "9块9包邮"
    },

    RULE_COLLECT_GOOD: {
        # 183: "京东凑单品",
        # 269: "中亚凑单品",
        41: "美亚凑单品",
        # 271: "日亚凑单品"
    },

    RULE_MALL_RANK_LIST: {
        183: "京东好价榜",
        247: "天猫好价榜",
        # 269: "中亚好价榜",
        41: "美亚好价榜",
        271: "日亚好价榜"
    },

    RULE_CATE_RANK_LIST: {
        "食品保健": "食品保健好价排行",
        "日用百货": "日用百货好价排行",
        "母婴用品": "母婴用品好价排行",
        "个护化妆": "个护化妆好价排行",
        "服饰鞋包": "服饰鞋包好价排行"
    }
}
