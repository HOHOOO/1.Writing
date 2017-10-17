#!/bin/bash

source ~/.bash_profile


year=`date -d "$1 day ago" +"%Y"`
month=`date -d "$1 day ago" +"%m"`
day=`date -d "$1 day ago" +"%d"`
ds=`date -d "$1 day ago" +"%Y%m%d"`
master="spark://hadoop003:7077"
jar_dir01="/data/tmp/zhanshulin/jar/Data_Offline.jar"
input_hdfs="/dataOffline/nginx/$year/$month/$day/"
output_hdfs_1="/nginx_analysis/iPhone_yh"
output_hdfs_2="/nginx_analysis/iPhone_yc"
output_hdfs_3="/nginx_analysis/android_yh"
output_hdfs_4="/nginx_analysis/android_yc"
output_hdfs_5="/nginx_analysis/hdfs_iphone.txt"
output_hdfs_6="/nginx_analysis/hdfs_android.txt"
output_hdfs_9="/nginx_analysis/iPhone_and_android_$year_$month_$day.txt"
output_hdfs_7="/dataOffline/device_articles_request/$year/$month/$day/iPhone_and_android_$ds.txt"
hdfs dfs -rm -r -f -skipTrash /nginx_analysis
hdfs dfs -rm -r -f -skipTrash /dataOffline/device_articles_request/$year/$month/$day
hadoop dfs -mkdir /dataOffline/device_articles_request/$year/$month/$day/

#echo "spark-submit --master $master --class DataOffline --executor-memory 20g --total-executor-cores 30 $jar_dir01 --driver-memory 4g $input_hdfs $output_hdfs_1 $output_hdfs_2 $output_hdfs_3 $output_hdfs_4"
#spark-submit --master $master --class DataOffline  --executor-memory 10g --total-executor-cores 10 --driver-memory 4g $jar_dir01 $input_hdfs $output_hdfs_1 $output_hdfs_2 $output_hdfs_3 $output_hdfs_4
echo "spark-submit --master yarn --class DataOffline --executor-memory 7g --executor-cores 2 --driver-memory 8g --num-executors 6 --queue q_gmv $jar_dir01 $input_hdfs $output_hdfs_1 $output_hdfs_2 $output_hdfs_3 $output_hdfs_4"
spark-submit --master yarn --class DataOffline  --driver-memory 8g --executor-memory 7G --executor-cores 2 --num-executors 6 --conf "spark.default.parallelism=100" --queue q_gmv $jar_dir01 $input_hdfs $output_hdfs_1 $output_hdfs_2 $output_hdfs_3 $output_hdfs_4

hadoop fs -cat $output_hdfs_1/* | hadoop fs -appendToFile - $output_hdfs_5
hadoop fs -cat $output_hdfs_2/* | hadoop fs -appendToFile - $output_hdfs_5
hadoop fs -cat $output_hdfs_3/* | hadoop fs -appendToFile - $output_hdfs_6
hadoop fs -cat $output_hdfs_4/* | hadoop fs -appendToFile - $output_hdfs_6
hadoop dfs -cat $output_hdfs_5 | hadoop dfs -appendToFile - $output_hdfs_7
hadoop dfs -cat $output_hdfs_6 | hadoop dfs -appendToFile - $output_hdfs_7

hive -e "
alter table device_articles_request add if not exists partition(ds='$ds') location '/dataOffline/device_articles_request/$year/$month/$day'
"
