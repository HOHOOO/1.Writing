#!/bin/bash
home_path=$(cd "`dirname $0`"; pwd)
quality_sync_time=`date -d "4 day ago" +"%Y-%m-%d %H:%M:%S"`
score_timestamp=`date -d "now" +%s`

#input para
zscore=$1
yuanchuang_para1=$2
yuanchuang_para2=$3
yuanchuang_para3=$4
yuanchuang_para4=$5
store_decrease=$6
echo $zscore
echo $yuanchuang_para1
echo $yuanchuang_para2
echo $yuanchuang_para3
echo $yuanchuang_para4
echo $store_decrease


hive -e '
use recommend;

SELECT "$zscore,$yuanchuang_para1,$yuanchuang_para2,$yuanchuang_para3,$yuanchuang_para4,$store_decrease";
 SELECT id, (pow($yuanchuang_para1,1) * collection_count from  ( SELECT id,collection_count,love_rating_count,comment_count,reward_count,$score_timestamp  as score_timestamp FROM sync_yuanchuang ) limit 5;
'
