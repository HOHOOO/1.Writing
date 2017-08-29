#!/bin/bash
path="$(cd "`dirname "$0"`"; pwd)"
echo $path
cd $path

ps -ef | grep port=8813 | grep -v grep |awk '{print $2}' | xargs kill
echo "8813 port process is stop!"

#nohup python $path/run_servier_api.py --config=./config-line --port=8813 --process=8 --log_file_prefix=/data/logs/dingyue_tuijian/server/server.log --log-rotate-mode=time --logging=info &
echo "8813 port process is start!"
