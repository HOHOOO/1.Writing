# -*- coding: utf-8 -*-

import argparse
import logging
import os

from apscheduler.schedulers.blocking import BlockingScheduler

from channel.channel_map import CHANNEL_MAP
from base.config import load

logger = logging.getLogger(__name__)

if __name__ == "__main__":
        """
        python sync_tools.py --config=./config_test/wiki --func=wiki --start_time="2014-07-17 17:01:07" --end_time="2017-07-29 17:01:07" 1>&/data/logs/article_sync_wiki.log
        python run.py --config=./config_test/wiki --func=wiki 1>&/data/logs/article_sync_wiki.log
        """
	logging.basicConfig(level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
	# scheduler = BlockingScheduler()
	# scheduler.add_job(tick, "interval", kwargs={"start_time": 1, "k2": 2}, seconds=3)
	#
	# logger.info('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
	#
	# try:
	# 	scheduler.start()
	# except (KeyboardInterrupt, SystemExit):
	# 	scheduler.shutdown()
	# 	logger.error("Exit the job")
	parse = argparse.ArgumentParser()
	parse.add_argument("--config", type=str, default="./config_local", help="config path")
	parse.add_argument("--interval", type=int, default=60, help="input sync interval time")
	parse.add_argument("--func", choices=CHANNEL_MAP.keys(), default='youhui',
	    help="sync mysql data. (youhui, yuanchuang, zixun, pingce, wiki, xinruipinpai, shipin, machine_report,home,online2offline)")
	args = parse.parse_args()
	load(path=args.config)
	# f = FUNCTION_MAP[args.func]()

	scheduler = BlockingScheduler()
	scheduler.add_job(CHANNEL_MAP[args.func], "interval", seconds=int(args.interval))
	
	logger.info('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
	
	try:
		scheduler.start()
	except (KeyboardInterrupt, SystemExit):
		scheduler.shutdown()
		logger.error("Exit the job")
