#!/bin/bash

# 此脚本用户监测千人千面首页文章数据同步进程是否存在
source ~/.bash_profile

token="e118a4e75784e5d84fca3e89d4c9b1c6c7451774d72e33d672b2b8ff9e17e6e4"
touser="robot"

home="$(cd "`dirname "$0"`"/; pwd)"
tmpFile="$home/home_sync_monitor.txt"

echo `date "+%Y-%m-%d %H:%M:%S"`
for i in home machine_report zixun yuanchuang youhui xinruipinpai wiki shipin pingce
do
    process_num=`ps -ef | grep  "func=$i" | grep -v grep | wc -l`
    if [ $process_num -ne 1 ]; then
        echo "$i sync article process died" >> $tmpFile
    else
        echo "$i sync article process normal"
    fi
done


#################
# 统一发送消息
###################################################################################################
#################
if [ -f $tmpFile ] ; then
    size=`ls -l $tmpFile | awk '{print $5}'`
    if [ $size -gt 0 ]; then
        curl -v -d "token=$token&touser=$touser&msg=`cat $tmpFile`" "http://hadoop004:1090/dingding/alarm"
    fi
fi

rm -rf $tmpFile
