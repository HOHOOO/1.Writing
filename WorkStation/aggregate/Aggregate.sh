#!/bin/bash
source ~/.bash_profile

echo "计算时间窗口设置"
time_window=$1
day_start=`date -d "$1 day ago" +"%Y-%m-%d"`
day_end=`date -d "1 day ago" +"%Y-%m-%d"`
echo $time_window
echo $day_start
echo $day_end

echo "输入函数类型 线性1 sigma2 指数3"
function_model=$2
echo $function_model

echo "输入函数参数"
function_para1=$3
function_para2=$4
echo $function_para1
echo $function_para2

start_time=`date`
echo "start at"$start_time

spark-sql --driver-class-path $PORTRAIT_JAR_HOME_PATH/mysql-connector-java-commercial-5.1.40-bin.jar --name 好价品牌品类偏好 --driver-memory 10g --executor-memory 8G --executor-cores 4 --num-executors 3 --conf "spark.default.parallelism=100" --master yarn --queue q_gmv -e "

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;
set mapred.job.queue.name=q_gmv;
use default;
set hive.map.aggr=true;
set hive.groupby.skewindata=true;
set hive.exec.parallel=true;
set mapred.reduce.tasks=10;


INSERT OVERWRITE TABLE DW_RECSYS_USER_RELATION_APPLIED_BAT PARTITION(SNAPSHOT_DATE='$day_end') AS
SELECT USER_PROXY_KEY,
       TAG_ID,
       (USER_TAG_ACTION_COUNT / SUM(USER_TAG_ACTION_COUNT)) AS USER_TAG_WEIGHT
FROM
  (SELECT USER_PROXY_KEY,
          TAG_ID,
          SUM(INDEX_FACTOR) AS USER_TAG_ACTION_COUNT
   FROM
     (SELECT USER_PROXY_KEY,
             TAG_ID,
             USER_TAG_ACTION_COUNT,
             USER_TAG_VALUE_SUM,
             USER_TAG_WEIGHT,
             CASE
                 WHEN $function_model=1 THEN USER_TAG_ACTION_COUNT * ($function_para1 * datadiff(SNAPSHOT, '$day_time') / $time_window * (-1) + $function_para2)
                 WHEN $function_model=2 THEN USER_TAG_ACTION_COUNT * (1 / (1 + $function_para1 * exp ((1/$function_para1) * ((2/$function_para1)*datadiff(SNAPSHOT, '$day_time') / $time_window - $function_para2))))
                 WHEN $function_model=3 THEN USER_TAG_ACTION_COUNT * (pow (($function_para2 * datadiff (SNAPSHOT, '$day_time') / $time_window), (1/$function_para1)))
                 ELSE USER_TAG_WEIGHT
             END AS INDEX_FACTOR
      FROM DW_RECSYS_USER_RELATION_DAILY
      WHERE SNAPSHOT >= '$day_start' ) GROUPBY USER_PROXY_KEY,
                                               TAG_ID);


CREATE TABLE DW_RECSYS_USER_RELATION_APPLIED_BAT_REDIS AS
SELECT USER_PROXY_KEY,
       CONCAT_WS(',',
                 COLLECT_SET(TAG_ID,':',USER_TAG_WEIGHT)) AS TAG_STR
FROM DW_RECSYS_USER_RELATION_APPLIED_BAT
WHERE SNAPSHOT_DATE='$day_end'
GROUP BY USER_PROXY_KEY;

"
end_time=`date`
echo "end at" $end_time
