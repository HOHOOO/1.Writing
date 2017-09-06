--recommend.DW_RECSYS_USER_RELATION_APPLIED_BAT表计算

SELECT "====================================";
SELECT "执行参数:";
SELECT "ds: ${hiveconf:ds}";
SELECT "DaysEnd: ${hiveconf: DaysEnd}";
SELECT "DaysStart: ${hiveconf: DaysStart}";
SELECT "FunctionModel: ${hiveconf: FunctionModel}";
SELECT "FunctionPara1: ${hiveconf: FunctionPara1}";
SELECT "FunctionPara2: ${hiveconf: FunctionPara2}";
SELECT "====================================";

DROP TABLE dw_recsys_user_relation_applied_bat_redis;

CREATE EXTERNAL TABLE IF NOT EXISTS  recommend.DW_RECSYS_USER_RELATION_APPLIED_BAT(
  user_proxy_key string,
  tag_id string,
  user_tag_weight float)
PARTITIONED BY (
  ds string)
LOCATION
  'hdfs://hadoopcluster/recommend/dw/dw_recsys_user_relation_applied_bat';



--时间衰减函数设置
SELECT "INSERT TABLE recommend.DW_RECSYS_USER_RELATION_APPLIED_BAT";
INSERT OVERWRITE TABLE recommend.DW_RECSYS_USER_RELATION_APPLIED_BAT PARTITION(ds='${hiveconf: DaysEnd}')
WITH T1 AS (SELECT user_proxy_key,
       tag_id,
       CASE
           WHEN ${hiveconf: FunctionModel}=1 THEN user_tag_action_count * (${hiveconf: FunctionPara1} * datediff(to_date('${hiveconf: DaysEnd}'),to_date(ds)) / datediff(to_date('${hiveconf: DaysEnd}'),to_date('${hiveconf: DaysStart}')) * (-1) + ${hiveconf: FunctionPara2})
           WHEN ${hiveconf: FunctionModel}=2 THEN user_tag_action_count * (1 / (1 + ${hiveconf: FunctionPara1} * exp ((1/${hiveconf: FunctionPara1}) * ((2/${hiveconf: FunctionPara1})*datediff(to_date('${hiveconf: DaysEnd}'),to_date(ds)) / datediff(to_date('${hiveconf: DaysEnd}'),to_date('${hiveconf: DaysStart}')) - ${hiveconf: FunctionPara2}))))
           WHEN ${hiveconf: FunctionModel}=3 THEN user_tag_action_count * (pow ((${hiveconf: FunctionPara2} * datediff(to_date('${hiveconf: DaysEnd}'),to_date(ds)) / datediff(to_date('${hiveconf: DaysEnd}'),to_date('${hiveconf: DaysStart}'))), (1/${hiveconf: FunctionPara1})))
           ELSE user_tag_weight
       END AS INDEX_FACTOR
FROM recommend.DW_RECSYS_USER_TAG_RELATION_DAILY
WHERE length(user_proxy_key)>1 AND ds BETWEEN '${hiveconf: DaysStart}' AND '${hiveconf: DaysEnd}'),
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
       (b.user_tag_action_count/c.user_tag_action_count_sum) AS user_tag_weight
FROM T2 b LEFT JOIN T3 c ON b.user_proxy_key=c.user_proxy_key;

--写入redis的表
SELECT "INSERT TABLE  DW_RECSYS_USER_RELATION_APPLIED_BAT_REDIS";
CREATE TABLE DW_RECSYS_USER_RELATION_APPLIED_BAT_REDIS AS
SELECT user_proxy_key,
       CONCAT_WS(',', COLLECT_SET(concat(tag_id,':',user_tag_weight))) AS tag_str
FROM recommend.DW_RECSYS_USER_RELATION_APPLIED_BAT
WHERE ds='${hiveconf: DaysEnd}' GROUP BY user_proxy_key;
