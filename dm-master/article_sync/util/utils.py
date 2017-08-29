# -*- coding: utf-8 -*-
import os
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def get_start_end_time(save_time_file, start_time=None, end_time=None):
	start_time = start_time
	end_time = end_time
	if not start_time:
		if os.path.exists(save_time_file):
			with open(save_time_file, "r") as f:
				start_time = f.read()
		else:
			start_time = datetime.now() - timedelta(hours=6)
			start_time = start_time.strftime("%Y-%m-%d %H:00:00")
	if not end_time:
		end_time = datetime.now().strftime("%Y-%m-%d %H:%M:00")
	
	if not os.path.exists(os.path.dirname(save_time_file)):
		os.mkdir(os.path.dirname(save_time_file))
	
	with open(save_time_file, "wb") as f:
		f.write(end_time)
	logger.info("start_time is: %s, end_time is: %s", start_time, end_time)
	
	return start_time, end_time


def get_pid(save_pid_file):
	logger.info("get_pid save_pid_file: %s", save_pid_file)
	t_pid = 0
	if not os.path.exists(os.path.dirname(save_pid_file)):
		os.mkdir(os.path.dirname(save_pid_file))
	
	try:
		if os.path.exists(save_pid_file):
			with open(save_pid_file, "rb") as f:
				t_pid = f.read()
				if not t_pid:
					t_pid = 0
	except:
		t_pid = 0
	
	logger.info("get_pid t_pid: %s", t_pid)
	return int(t_pid)


def write_pid(save_pid_file, pid=0):
	logger.info("write_pid save_pid_file: %s, pid:%s", save_pid_file, pid)
	if not os.path.exists(os.path.dirname(save_pid_file)):
		os.mkdir(os.path.dirname(save_pid_file))
		
	save_pid = get_pid(save_pid_file)
	logger.info("write_pid save_pid: %s", save_pid)
	
	if pid > int(save_pid):
		with open(save_pid_file, "wb") as f:
			f.write("%s" % pid)

