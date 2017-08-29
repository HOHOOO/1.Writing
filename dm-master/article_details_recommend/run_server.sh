#!/bin/bash

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/data/anaconda2/bin"

source activate article_details_recommend

path="$(cd "`dirname "$0"`"; pwd)"
echo $path
cd $path

ps -ef | grep port=8806 | grep -v grep |awk '{print $2}' | xargs kill
echo "8806 port process is stop!"

nohup python /data/webroot/dm/article_details_recommend/run_server.py --config=./config_online --port=8806 --process=0 --log_file_prefix=/data/logs/article_details/server/server.log --log-rotate-mode=time --logging=info --log-to-stderr=False 2>&1 &
echo "8806 port process is start!"
