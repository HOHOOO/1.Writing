#!/bin/bash

ps -ef | grep "python run.py" | grep -Ev 'online2offline|grep' | awk '{print $2}' | xargs kill

nohup python run.py --config=./config_test/pingce --func=pingce >> /data/logs/pingce.log &
nohup python run.py --config=./config_test/shipin --func=shipin >> /data/logs/shipin.log &
nohup python run.py --config=./config_test/wiki --func=wiki >> /data/logs/wiki.log &
nohup python run.py --config=./config_test/xinrui --func=xinruipinpai >> /data/logs/xinruipinpai.log &
nohup python run.py --config=./config_test/youhui --func=youhui >> /data/logs/youhui.log &
nohup python run.py --config=./config_test/yuanchuang --func=yuanchuang >> /data/logs/yuanchuang.log &
nohup python run.py --config=./config_test/zixun --func=zixun >> /data/logs/zixun.log &
nohup python run.py --config=./config_test/youhui --func=machine_report >> /data/logs/machine_report.log &
nohup python run.py --config=./config_test/home --func=home >> /data/logs/home.log &
