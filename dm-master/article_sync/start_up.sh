#!/bin/bash

ps -ef | grep "python run.py" | grep -v grep | awk '{print $2}' | xargs kill

nohup python run.py --config=./config_online/pingce --func=pingce >> /data/logs/pingce.log &
nohup python run.py --config=./config_online/shipin --func=shipin >> /data/logs/shipin.log &
nohup python run.py --config=./config_online/wiki --func=wiki >> /data/logs/wiki.log &
nohup python run.py --config=./config_online/xinrui --func=xinruipinpai >> /data/logs/xinruipinpai.log &
nohup python run.py --config=./config_online/youhui --func=youhui >> /data/logs/youhui.log &
nohup python run.py --config=./config_online/yuanchuang --func=yuanchuang >> /data/logs/yuanchuang.log &
nohup python run.py --config=./config_online/zixun --func=zixun >> /data/logs/zixun.log &
nohup python run.py --config=./config_online/youhui --func=machine_report >> /data/logs/machine_report.log &
nohup python run.py --config=./config_online/home --func=home >> /data/logs/home.log &
