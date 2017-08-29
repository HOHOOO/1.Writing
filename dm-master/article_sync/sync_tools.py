# -*- coding: utf-8 -*-

import argparse
import logging
from datetime import datetime

from channel.channel_map import CHANNEL_MAP
from base.config import load

logger = logging.getLogger(__name__)

if __name__ == "__main__":
	logging.basicConfig(level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
	now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	parse = argparse.ArgumentParser()
	parse.add_argument("--start_time", type=str, default=now, help="start_time")
	parse.add_argument("--end_time", type=str, default=now, help="end_time")
	parse.add_argument("--pid", type=str, default=0, help="youhui_meta表的主键meta_id")
	parse.add_argument("--config", type=str, default="./config_local", help="config path")
	parse.add_argument("--func", choices=CHANNEL_MAP.keys(), default='youhui',
	                   help="sync mysql data. (youhui, yuanchuang, zixun, pingce, wiki, xinruipinpai, shipin, machine_report,home)")
	args = parse.parse_args()
	load(path=args.config)
	if args.func == "machine_report":
		CHANNEL_MAP[args.func](args.pid)
	elif args.func == "home":
		CHANNEL_MAP[args.func](args.start_time)
	else:
		CHANNEL_MAP[args.func](args.start_time, args.end_time)
