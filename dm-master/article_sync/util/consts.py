# -*- coding: utf-8 -*-

YOUHUI_CHANNEL_ID = 3
ZIXUN_CHANNEL_ID = 6
PINGCE_CHANNEL_ID = 7
YUANCHUANG_CHANNEL_ID = 11
WIKI_CHANNEL_ID = 14
XINRUI_CHANNEL_ID = 31
SHIPIN_CHANNEL_ID = 38
ZHIBO_CHANNEL_ID = 48

YOUHUI_CHANNEL = 'yh'
ZIXUN_CHANNEL = 'zx'
PINGCE_CHANNEL = 'pc'
YUANCHUANG_CHANNEL = 'yc'
WIKI_CHANNEL = 'wk'
XINRUI_CHANNEL = 'xr'
SHIPIN_CHANNEL = 'sp'
ZHIBO_CHANNEL = 'zb'

YH_SYNC_LAST_TIME_PATH = "/tmp/%s_mysqlsync_last_sync_time" % YOUHUI_CHANNEL
YC_SYNC_LAST_TIME_PATH = "/tmp/%s_mysqlsync_last_sync_time" % YUANCHUANG_CHANNEL
ZX_SYNC_LAST_TIME_PATH = "/tmp/%s_mysqlsync_last_sync_time" % ZIXUN_CHANNEL
PC_SYNC_LAST_TIME_PATH = "/tmp/%s_mysqlsync_last_sync_time" % PINGCE_CHANNEL
WK_SYNC_LAST_TIME_PATH = "/tmp/%s_mysqlsync_last_sync_time" % WIKI_CHANNEL
SP_SYNC_LAST_TIME_PATH = "/tmp/%s_mysqlsync_last_sync_time" % SHIPIN_CHANNEL
XR_SYNC_LAST_TIME_PATH = "/tmp/%s_mysqlsync_last_sync_time" % XINRUI_CHANNEL
ON2OFF_LAST_TIME_PATH = "/tmp/%s_mysqlsync_last_sync_time_on2off"
HOME_SYNC_LAST_TIME_PATH = "/tmp/home_mysqlsync_last_sync_time"
MACHINE_REPORT_PID_FILE_PATH = "/tmp/machine_report_pid_file_path.pid"

ROW_COUNT = 500
DEFAULT_TIME = '1970-01-01 00:00:00'


EDITOR_STOCK_STATUS_MAP = {
	0: 0,   # 正常
	1: 11,  # 过期
	2: 12   # 售罄
}

MACHINE_STATUS_MAP = {
	0: 1,   # 涨价
	1: 2,   # 降价
	2: 3    # 售罄
}
