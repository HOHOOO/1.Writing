# -*- coding: utf-8 -*-
import logging
import urllib2
import urllib

logger = logging.getLogger("monitor")


def dingding_alarm_msg(msg):
	if not msg:
		return
	try:
		url = "http://hadoop004:1090/dingding/alarm?token"
		
		body_dict = {
			"token": "e118a4e75784e5d84fca3e89d4c9b1c6c7451774d72e33d672b2b8ff9e17e6e4",
			"touser": "robot",
			"msg": "%s" % msg
		}
		post_data = urllib.urlencode(body_dict)
		req = urllib2.urlopen(url, post_data)
		
		logger.info("dingding_alarm_msg req code: %s", req.getcode())
	except Exception as e:
		logger.error("monitor business dingding send msg err: %s", str(e))
