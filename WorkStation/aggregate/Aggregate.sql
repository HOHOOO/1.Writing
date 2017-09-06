#!/bin/bash
source ~/.bash_profile

echo "计算结束时间设置"
day_end=`date -d "$1 day ago" +"%Y-%m-%d"`
echo $day_end

echo "计算时间窗口设置"
time_window=$2
day_start=`date -d "$2 day ago" +"%Y-%m-%d"`
echo $time_window
echo "计算开始时间"
echo $day_start


echo "输入函数类型 线性1 sigma2 指数3"
function_model=$3
echo $function_model

echo "输入函数参数"
function_para1=$4
function_para2=$5
echo $function_para1
echo $function_para2

echo "输入数据源的时间长度"
days_length=$6
echo $days_length

start_time=`date`
echo "the shell start at"$start_time

spark-sql --driver-class-path /data/tmp/zhanshulin/jar/mysql-connector-java-commercial-5.1.40-bin.jar  --name 标签偏好计算 --driver-memory 10g --executor-memory 8G --executor-cores 4 --num-executors 3 --conf "spark.default.parallelism=100" --master yarn --queue q_gmv -e "

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;
set mapred.job.queue.name=q_gmv;
use default;
set hive.map.aggr=true;
set hive.groupby.skewindata=true;
set hive.exec.parallel=true;
set mapred.reduce.tasks=10;

--时间衰减函数设置
SELECT "INSERT TABLE recommend.DW_RECSYS_USER_RELATION_APPLIED_BAT";
INSERT OVERWRITE TABLE recommend.DW_RECSYS_USER_RELATION_APPLIED_BAT PARTITION(ds='${hiveconf: DayEnd}')
WITH
T1 AS (SELECT user_proxy_key,
       tag_id,
       CASE
           WHEN ${hiveconf: FunctionModel}=1 THEN user_tag_action_count * (${hiveconf: FunctionPara1} * datediff(to_date('${hiveconf: DayEnd}'),to_date(ds)) / datediff(to_date('${hiveconf: DayEnd}'),to_date('${hiveconf: DayStart}')) * (-1) + ${hiveconf: FunctionPara2})
           WHEN ${hiveconf: FunctionModel}=2 THEN user_tag_action_count * (1 / (1 + ${hiveconf: FunctionPara1} * exp ((1/${hiveconf: FunctionPara1}) * ((2/${hiveconf: FunctionPara1})*datediff(to_date('${hiveconf: DayEnd}'),to_date(ds)) / datediff(to_date('${hiveconf: DayEnd}'),to_date('${hiveconf: DayStart}')) - ${hiveconf: FunctionPara2}))))
           WHEN ${hiveconf: FunctionModel}=3 THEN user_tag_action_count * (pow ((${hiveconf: FunctionPara2} * datediff(to_date('${hiveconf: DayEnd}'),to_date(ds)) / datediff(to_date('${hiveconf: DayEnd}'),to_date('${hiveconf: DayStart}'))), (1/${hiveconf: FunctionPara1})))
           ELSE user_tag_weight
       END AS INDEX_FACTOR
FROM recommend.DW_RECSYS_USER_TAG_RELATION_1_DAYS
WHERE length(user_proxy_key)>1 AND ds BETWEEN '${hiveconf: DayStart}' AND '${hiveconf: DayEnd}'),
--tf操作
T2 AS (SELECT user_proxy_key,
       tag_id,
       SUM(INDEX_FACTOR) AS user_tag_action_count
FROM T1
GROUP BY user_proxy_key,tag_id),

T3 AS (SELECT user_proxy_key,
       SUM( user_tag_action_count) AS user_tag_action_count_sum
FROM T2 GROUP BY user_proxy_key)

SELECT b.user_proxy_key,
       b.tag_id,
       (b.user_tag_action_count/c.user_tag_action_count_sum) AS user_tag_weight,
       '${hiveconf: Ds}' AS ds
FROM T2 b LEFT JOIN T3 c ON b.user_proxy_key=c.user_proxy_key;


--时间衰减函数设置

CREATE TABLE T1 AS (SELECT user_proxy_key,
       tag_id,
       CASE
           WHEN ${hiveconf: FunctionModel}=1 THEN user_tag_action_count * (${hiveconf: FunctionPara1} * datediff(to_date('${hiveconf: DayEnd}'),to_date(ds)) / datediff(to_date('${hiveconf: DayEnd}'),to_date('${hiveconf: DayStart}')) * (-1) + ${hiveconf: FunctionPara2})
           WHEN ${hiveconf: FunctionModel}=2 THEN user_tag_action_count * (1 / (1 + ${hiveconf: FunctionPara1} * exp ((1/${hiveconf: FunctionPara1}) * ((2/${hiveconf: FunctionPara1})*datediff(to_date('${hiveconf: DayEnd}'),to_date(ds)) / datediff(to_date('${hiveconf: DayEnd}'),to_date('${hiveconf: DayStart}')) - ${hiveconf: FunctionPara2}))))
           WHEN ${hiveconf: FunctionModel}=3 THEN user_tag_action_count * (pow ((${hiveconf: FunctionPara2} * datediff(to_date('${hiveconf: DayEnd}'),to_date(ds)) / datediff(to_date('${hiveconf: DayEnd}'),to_date('${hiveconf: DayStart}'))), (1/${hiveconf: FunctionPara1})))
           ELSE user_tag_weight
       END AS INDEX_FACTOR
FROM recommend.DW_RECSYS_USER_TAG_RELATION_1_DAYS
WHERE length(user_proxy_key)>1 AND ds BETWEEN '${hiveconf: DayStart}' AND '${hiveconf: DayEnd}');

--tf操作
CREATE TABLE T2 AS (SELECT user_proxy_key,
       tag_id,
       SUM(INDEX_FACTOR) AS user_tag_action_count
FROM T1
GROUP BY user_proxy_key,tag_id);

CREATE TABLE T3 AS (SELECT user_proxy_key,
       SUM( user_tag_action_count) AS user_tag_action_count_sum
FROM T2 GROUP BY user_proxy_key);

SELECT "INSERT TABLE recommend.DW_RECSYS_USER_RELATION_APPLIED_BAT";
INSERT OVERWRITE TABLE recommend.DW_RECSYS_USER_RELATION_APPLIED_BAT PARTITION(ds='${hiveconf: DayEnd}') SELECT b.user_proxy_key,
       b.tag_id,
       (b.user_tag_action_count/c.user_tag_action_count_sum) AS user_tag_weight,
       '${hiveconf: Ds}' AS ds
FROM T2 b LEFT JOIN T3 c ON b.user_proxy_key=c.user_proxy_key;














DROP TABLE DW_RECSYS_USER_TAG_RELATION_DAILY_TEMP;
DROP TABLE DW_RECSYS_USER_RELATION_APPLIED_BAT_TEMP;
DROP TABLE DW_RECSYS_USER_RELATION_APPLIED_BAT_TMP;
DROP TABLE dw_recsys_user_relation_applied_bat_redis;

CREATE TABLE T1 AS
SELECT user_proxy_key,
       tag_id,
       CASE
           WHEN $function_model=1 THEN user_tag_action_count * ($function_para1 * datediff(to_date('$day_end'),to_date(ds)) / $time_window * (-1) + $function_para2)
           WHEN $function_model=2 THEN user_tag_action_count * (1 / (1 + $function_para1 * exp ((1/$function_para1) * ((2/$function_para1)*datediff(to_date('$day_end'),to_date(ds)) / $time_window - $function_para2))))
           WHEN $function_model=3 THEN user_tag_action_count * (pow (($function_para2 * datediff(to_date('$day_end'),to_date(ds)) / $time_window), (1/$function_para1)))
           ELSE user_tag_weight
       END AS INDEX_FACTOR
FROM recommend.DW_RECSYS_USER_TAG_RELATION_2_DAYS
WHERE length(user_proxy_key)>1 AND ds BETWEEN '$day_start' AND '$day_end';


CREATE TABLE T2 AS
SELECT user_proxy_key,
       tag_id,
       SUM(INDEX_FACTOR) AS user_tag_action_count
FROM DW_RECSYS_USER_TAG_RELATION_DAILY_TEMP
GROUP BY user_proxy_key,tag_id;


CREATE TABLE T3 AS
SELECT user_proxy_key,
       SUM( user_tag_action_count) AS user_tag_action_count_sum
FROM DW_RECSYS_USER_RELATION_APPLIED_BAT_TEMP GROUP BY user_proxy_key;



INSERT OVERWRITE TABLE recommend.DW_RECSYS_USER_RELATION_APPLIED_BAT PARTITION(ds='$day_end')
SELECT b.user_proxy_key,
       b.tag_id,
       (b.user_tag_action_count/c.user_tag_action_count_sum) AS user_tag_weight
FROM DW_RECSYS_USER_RELATION_APPLIED_BAT_TEMP b LEFT JOIN DW_RECSYS_USER_RELATION_APPLIED_BAT_TMP c ON b.user_proxy_key=c.user_proxy_key;

CREATE TABLE DW_RECSYS_USER_RELATION_APPLIED_BAT_REDIS AS
SELECT user_proxy_key,
       CONCAT_WS(',', COLLECT_SET(concat(tag_id,':',user_tag_weight))) AS tag_str
FROM recommend.DW_RECSYS_USER_RELATION_APPLIED_BAT
WHERE ds='$day_end' GROUP BY user_proxy_key;


"
end_time=`date`
echo "the shell end at" $end_time
