--recommend.DW_CP_USER_PREFERENCE_TEST_${hiveconf:V_DayLength}_DAYS计算

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;
set mapred.job.queue.name=q_gmv;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.mode.local.auto=true;
set hive.exec.reducers.max=99;
set hive.exec.reducers.bytes.per.reducer=2048000000;

SELECT "====================================";
SELECT "执行参数:";
SELECT "Ds:${hiveconf:V_Ds}";
SELECT "DayEnd:${hiveconf:V_DayEnd}";
SELECT "DayLength:${hiveconf:V_DayLength}";
SELECT "FunctionModel:${hiveconf:V_FunctionModel}";
SELECT "FunctionPara1:${hiveconf:V_FunctionPara1}";
SELECT "FunctionPara2:${hiveconf:V_FunctionPara2}";
SELECT "Version:${hiveconf:V_Version}";
SELECT "====================================";

DROP TABLE T1_test;
DROP TABLE T2_test;
DROP TABLE T3_test;
DROP TABLE T4_test;
DROP TABLE T5_test;
DROP TABLE T6_test;
DROP TABLE T6_test_TEMP;
DROP TABLE T7_test;
DROP TABLE T8_test;
DROP TABLE T9_test;

dfs -rm -r -f -skipTrash /recommend/dw/recommend.DW_CP_USER_PREFERENCE_TEST_${hiveconf:V_DayLength}_DAYS/ds='${hiveconf:V_Ds}';

CREATE EXTERNAL TABLE IF NOT EXISTS  recommend.DW_CP_USER_PREFERENCE_TEST_${hiveconf:V_DayLength}_DAYS(
  user_proxy_key string,
  tag_id string,
  user_tag_weight decimal(7,6))
PARTITIONED BY (
  ds string)
LOCATION
  'hdfs://hadoopcluster/recommend/dw/recommend.DW_CP_USER_PREFERENCE_TEST_${hiveconf:V_DayLength}_DAYS';

CREATE TABLE IF NOT EXISTS  recommend.DW_CP_USER_PREFERENCE_TEST_LONG_TERM(
  user_proxy_key string,
  tag_id string,
  user_tag_weight decimal(7,6),
  ds string )
LOCATION
  'hdfs://hadoopcluster/recommend/dw/recommend.DW_CP_USER_PREFERENCE_TEST_LONG_TERM';



--与recommend.DW_CP_DIC_ACTION_INFO 行为表进行聚合

CACHE TABLE CACHE_DW_CP_DIC_ACTION_INFO AS SELECT * FROM recommend.DW_CP_DIC_ACTION_INFO WHERE to_date(ds) = to_date('${hiveconf:V_Ds}');

CREATE TABLE T1_test as
  SELECT
  b.USER_PROXY_KEY,
  b.TAG_ID,
  (b.user_tag_action_count * c.ACTION_WEIGHT_ESTIMATE) as user_tag_action_count,
  b.ds as ds
  FROM recommend.DW_CP_USER_TAG_STATISTICS_DAILY AS b
  JOIN CACHE_DW_CP_DIC_ACTION_INFO AS c
  ON b.USER_ACTION_ID = c.ACTION_ID
  WHERE to_date(b.ds) BETWEEN to_date(DATE_SUB('${hiveconf:V_DayEnd}', ${hiveconf:V_DayLength})) AND to_date('${hiveconf:V_DayEnd}')
  AND NOT b.TAG_ID like 'tag%';

--时间衰减函数聚合

select '正在完成间衰减函数聚合';

  CREATE TABLE T2_test AS SELECT user_proxy_key,tag_id, CASE WHEN ${hiveconf:V_FunctionModel}=1 THEN (user_tag_action_count * (${hiveconf:V_FunctionPara1} * (0.9) * (-(datediff(to_date('${hiveconf:V_DayEnd}'),to_date(ds))) / ${hiveconf:V_DayLength}) + ${hiveconf:V_FunctionPara2}))
           WHEN ${hiveconf:V_FunctionModel}=2 THEN user_tag_action_count * (1 / (1 + 0.25 * ${hiveconf:V_FunctionPara1} * exp ((1/${hiveconf:V_FunctionPara1}) * ((2/${hiveconf:V_FunctionPara1})*datediff(to_date(ds), DATE_SUB('${hiveconf:V_DayEnd}', ${hiveconf:V_DayLength})) / ${hiveconf:V_DayLength} - ${hiveconf:V_FunctionPara2}))))
             WHEN ${hiveconf:V_FunctionModel}=3 THEN user_tag_action_count * (pow ((4*${hiveconf:V_FunctionPara2} * datediff(to_date(ds),DATE_SUB('${hiveconf:V_DayEnd}', ${hiveconf:V_DayLength})) / ${hiveconf:V_DayLength} ), ( 0.5 / ${hiveconf:V_FunctionPara1})))
             ELSE user_tag_action_count
         END AS INDEX_FACTOR
  FROM T1_test;

select '正在完成用户初始偏好计算';
  CREATE TABLE T3_test AS SELECT user_proxy_key,
         tag_id,
         SUM(INDEX_FACTOR) AS user_tag_action_count
  FROM T2_test
  GROUP BY user_proxy_key,tag_id;

--tf操作，用户偏好总和为1

  CREATE TABLE T4_test AS SELECT user_proxy_key,
         SUM( user_tag_action_count) AS user_tag_action_count_sum
  FROM T3_test GROUP BY user_proxy_key;

  CREATE TABLE T5_test AS SELECT b.user_proxy_key,
         b.tag_id,
        (b.user_tag_action_count/c.user_tag_action_count_sum) AS user_tag_weight

  FROM T3_test b LEFT JOIN T4_test c ON b.user_proxy_key=c.user_proxy_key;



--不感兴趣数据行转列

select "正在完成不感兴趣过滤操作";
CREATE TABLE T6_test as SELECT COALESCE(case when(a.user_id='') then NULL ELSE a.user_id END,a.device_id) as user_proxy_key, b.key, b.value
FROM user_dislike_content a
LATERAL VIEW explode (map(
  'cate', cate,
  'brand', brand,
  'tag', tag
)) b as key, value where a.channel_id IN (1,2,5,11)
AND a.authenticity = 1
AND to_date(a.ctime) > DATE_SUB('${hiveconf:V_DayEnd}',(30+${hiveconf:V_DayLength}));

CREATE TABLE T6_test_TEMP AS SELECT a.user_proxy_key, a.key, b.value
FROM T6_test a
LATERAL VIEW explode (split (value,',')) b AS value;


--关联标签ID


CREATE TABLE T7_test AS SELECT a.user_proxy_key, b.tag_id , '0' as user_tag_weight from T6_test_TEMP a LEFT JOIN recommend.dw_cp_dic_tag_info  b on a.value=b.tag_name where a.value<>'';


CREATE TABLE T8_test as Select b.user_proxy_key,b.tag_id,b.user_tag_weight from (
      select a.*,rank() over (partition by user_proxy_key,tag_id order by user_tag_weight asc,coalesce(user_tag_weight,0),rand()) as order_rank
      from (select * from T5_test
            union all
           select * from T7_test ) a
) b  where order_rank=1 ;

SELECT "已完成本周期用户画像计算，更新长期用户画像中";

INSERT OVERWRITE TABLE recommend.DW_CP_USER_PREFERENCE_TEST_${hiveconf:V_DayLength}_DAYS PARTITION(ds='${hiveconf:V_Ds}') select * from T8_test;

INSERT OVERWRITE TABLE recommend.DW_CP_USER_PREFERENCE_TEST_LONG_TERM_TEST
Select b.user_proxy_key,b.tag_id,b.user_tag_weight,ds from (
      select a.*,rank() over (partition by user_proxy_key,tag_id order by to_date(ds) desc) as order_rank
      from (select * from recommend.DW_CP_USER_PREFERENCE_TEST_LONG_TERM WHERE ds<>'${hiveconf:V_Ds}'
            union all
           select *,'${hiveconf:V_Ds}'as ds from T8_test ) a
) b  where order_rank=1 ;
