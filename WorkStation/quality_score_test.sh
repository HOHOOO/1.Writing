#!/bin/bash
home_path=$(cd "`dirname $0`"; pwd)
quality_sync_time=`date -d "4 day ago" +"%Y-%m-%d %H:%M:%S"`
score_timestamp=`date -d "now" +%s`

hive -d zscore='1.96' -d  yuanchuang_para='1' -d yuanchuang_para2='1' -d yuanchuang_para3='1' -d yuanchuang_para4='1' -d store_decrease='0.8' -e '
use recommend;

SELECT "${zscore},$yuanchuang_para1,$yuanchuang_para2,$yuanchuang_para3,$yuanchuang_para4,$store_decrease";
 SELECT id, (pow( $yuanchuang_para ,1) * collection_count from  ( SELECT id,collection_count,love_rating_count,comment_count,reward_count,$score_timestamp  as score_timestamp FROM sync_yuanchuang ) limit 5;
'
