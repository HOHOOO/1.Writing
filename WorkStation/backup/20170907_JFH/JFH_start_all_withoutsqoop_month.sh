#!/bin/bash
source ~/.bash_profile


for (( i=1; i<=1; i++ ))
do
j=`expr 30 \* $i`
echo $j
month=`date -d "$(date -d "$j day ago" +"%Y%m01")" +"%Y%m%d"`
month_end=`date -d "$(date -d "$j day ago" +"%Y%m01") + 30day" +"%Y%m%d"`
echo $month
echo $month_end
done


sh /data/tmp/zhanshulin/JFH_spark_standar.sh $month $month_end JFH_article_level_num JFH_level_month_standardization
