#!/bin/bash
source ~/.bash_profile


year=`date -d "$1 day ago" +"%Y"`
month=`date -d "$1 day ago" +"%m"`
day=`date -d "$1 day ago" +"%d"`
time1=`date -d "$1 day ago" +"%Y%m%d"`
home_path=$2
master="spark://hadoop003:7077"
jar_dir01="$home_path/file/jar/nginx_all_log.jar"
input_hdfs="/dataOffline/nginx/$year/$month/$day/"
output_hdfs_1="/dataOffline/nginx_all_log/$time1"
hadoop dfs -rmr /tmp/nginx_analysis
hadoop dfs -rmr /dataOffline/nginx_all_log/$time1

echo "spark-submit --master $master --class nginx_all_log --executor-memory 10g --total-executor-cores 10 $jar_dir01 --driver-memory 4g $input_hdfs $output_hdfs_1 "
spark-submit --master yarn --class nginx_all_log  --driver-memory 8g --executor-memory 7G --executor-cores 2 --num-executors 6 --conf "spark.default.parallelism=100"  --queue q_gmv $jar_dir01 $input_hdfs $output_hdfs_1 

hive -e "alter table nginx_all_log add if not exists partition(ds='$time1') location '/dataOffline/nginx_all_log/$time1'"
