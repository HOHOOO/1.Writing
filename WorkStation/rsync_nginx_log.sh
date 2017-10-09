#!/bin/bash

year=`date -d "$1 day ago" +"%Y"`
month=`date -d "$1 day ago" +"%m"`
day=`date -d "$1 day ago" +"%d"`
data=`date -d "$1 day ago" +"%Y%m%d"`
echo ${data}
home_path=$2
rm -rf $home_path/file/nginx/*
rsync -vrtl --progress --password-file=/home/hadoop/.http.access.log.password readAccessLogUser@10.9.42.154::http-access-log/*/*.http.access.log.${data}_* $home_path/file/nginx/

DIRECTORY="$home_path/file/nginx"
if [ "`ls -A $DIRECTORY`" != "" ]; then
    echo "rsync成功"
    hadoop dfs -mkdir -p /dataOffline/nginx/$year/$month/$day
    hadoop dfs -put $home_path/file/nginx/* /dataOffline/nginx/$year/$month/$day/
else
    sleep 300
    echo "重启rsync"
    rsync -vrtl --progress --password-file=/home/hadoop/.http.access.log.password readAccessLogUser@10.9.42.154::http-access-log/*/*.http.access.log.${data}_* $home_path/file/nginx/
    if [ "`ls -A $DIRECTORY`" != "" ]; then
        echo "rsync成功"
        hadoop dfs -mkdir -p /dataOffline/nginx/$year/$month/$day
        hadoop dfs -put $home_path/file/nginx/* /dataOffline/nginx/$year/$month/$day/
    else
        sleep 300
        echo "重启rsync"
        rsync -vrtl --progress --password-file=/home/hadoop/.http.access.log.password readAccessLogUser@10.9.42.154::http-access-log/*/*.http.access.log.${data}_* $home_path/file/nginx/
        if [ "`ls -A $DIRECTORY`" != "" ]; then
             echo "rsync成功"
             hadoop dfs -mkdir -p /dataOffline/nginx/$year/$month/$day
             hadoop dfs -put $home_path/file/nginx/* /dataOffline/nginx/$year/$month/$day/
        else
             sleep 300
             echo "重启rsync"
             rsync -vrtl --progress --password-file=/home/hadoop/.http.access.log.password readAccessLogUser@10.9.42.154::http-access-log/*/*.http.access.log.${data}_* $home_path/file/nginx/
        fi
   fi
fi
