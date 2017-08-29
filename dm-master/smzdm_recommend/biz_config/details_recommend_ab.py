# coding=utf-8

################################
# first表示该模块首先采用的算法类型，second依次往后推
# flag表示AB分流的开关，True或False,False会直接采用A算法，否则会分流来选择A/B算法
# cut_base表示切分流量依据，trace_id或device_id
# ratio表示用于AB测试的流量占比，0.0~1.0
# A表示当前所用的主算法
# B表示需要测试效果的算法
# name表示算法的名字
# small_flow_suffix表示该算法分到小流量时的后缀（用于之后日志统计效果）
# 举例说明，ratio为0.4，表示对总流量的40%进行分流测试（其余的60%使用A算法），
# 其中总重中40%的流量中的50%使用B算法，50%使用A算法，对比效果时主要对比40%流量中的两种算法效果表现


YOU_HUI_AB_BASE = "trace_id"
YUAN_CHUANG_AB_BASE = "trace_id"
HAO_WU_AB_BASE = "trace_id"
# YOU_HUI_AB_BASE = "device_id"


ASSO_RULE = "asso.rule"
BASE_SIMI = "base.simi"
STAR_DATA = "star.data"
BASE_SIMI_AND_WIKI_YC = "base.simi&wiki.yc"


YOU_HUI_AB_ROW1 = {
    "first": {
        "flag": False,
        "ratio": 1.0,
        "A": {
            "name": BASE_SIMI,
            "small_flow_suffix": "small",
        },
        "B": {
            "name": BASE_SIMI_AND_WIKI_YC,
            "small_flow_suffix": "small",
        }
    },
    "second": {

    },

}

YOU_HUI_AB_ROW2 = {
    "first": {
        "flag": False,
        "ratio": 1.0,
        "A": {
            "name": STAR_DATA,
            "small_flow_suffix": "small",
        },
    },
    "second": {
        "flag": False,
        "ratio": 1.0,
        "A": {
            "name": ASSO_RULE,
            "small_flow_suffix": "small",
        }
    },
}

YOU_HUI_AB_ROW3 = {
    "first": {
        "flag": False,
        "ratio": 1.0,
        "A": {
            "name": ASSO_RULE,
            "small_flow_suffix": "small",
        },
    },
    "second": {

    },
}

YUAN_CHUANG_AB_ROW1 = {
    "first": {

    },
    "second": {

    },
}

YUAN_CHUANG_AB_ROW2 = {
    "first": {
        "flag": False,
        "ratio": 1.0,
        "A": {
            "name": ASSO_RULE,
            "small_flow_suffix": "small",
        },
    },
    "second": {

    },
}

YUAN_CHUANG_AB_ROW3 = {
    "first": {
        "flag": False,
        "ratio": 1.0,
        "A": {
            "name": ASSO_RULE,
            "small_flow_suffix": "small",
        },
    },
    "second": {

    },
}

HAO_WU_AB_ROW1 = {
    "first": {

    },
    "second": {

    },
}

HAO_WU_AB_ROW2= {
    "first": {

    },
    "second": {

    },
}

HAO_WU_AB_ROW3 = {
    "first": {

    },
    "second": {

    },
}

