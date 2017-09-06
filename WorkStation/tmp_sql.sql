CREATE EXTERNAL TABLE `JFH_level_month_standardization`(`device_id` string,
                                                        `user_id` string,
                                                        `article_channel_name` string,
                                                        `level_type` int, `level_id` int, `level_num` int, `level_standardization` DOUBLE,
                                                                                                                                   `article_channel_id` int) PARTITIONED BY (`ds` string) LOCATION 'hdfs://hadoopcluster/dataOffline/JFH_level_month_standardization';


CREATE EXTERNAL TABLE `JFH_persona_output`(`level_id` int, `standar_freq` int ,`level_1_threshold` int) PARTITIONED BY (`ds` string) LOCATION 'hdfs://hadoopcluster/dataOffline/JFH_persona_output';


INSERT INTO TABLE JFH_persona_output partition(ds='20170901')
SELECT level_id,
       count(*) AS standar_freq,
       '0' AS level_1_threshold
FROM article_level_month_standardization
WHERE level_standardization>0
  AND level_type=1
  AND article_channel_id=1
  AND ds='20170904'
  AND level_id !=-1
GROUP BY level_id
ORDER BY level_id ASC;


INSERT INTO TABLE JFH_persona_output partition(ds='20170901')
SELECT level_id,
       count(*) AS standar_freq,
       '1' AS level_1_threshold
FROM article_level_month_standardization
WHERE level_standardization>1
  AND level_type=1
  AND article_channel_id=1
  AND ds='20170904'
  AND level_id !=-1
GROUP BY level_id
ORDER BY level_id ASC;


INSERT INTO TABLE JFH_persona_output partition(ds='20170901')
SELECT level_id,
       count(*) AS standar_freq,
       '2' AS level_1_threshold
FROM article_level_month_standardization
WHERE level_standardization>2
  AND level_type=1
  AND article_channel_id=1
  AND ds='20170904'
  AND level_id !=-1
GROUP BY level_id
ORDER BY level_id ASC;


INSERT INTO TABLE JFH_persona_output partition(ds='20170901')
SELECT level_id,
       count(*) AS standar_freq,
       '3' AS level_1_threshold
FROM article_level_month_standardization
WHERE level_standardization>3
  AND level_type=1
  AND article_channel_id=1
  AND ds='20170904'
  AND level_id !=-1
GROUP BY level_id
ORDER BY level_id ASC;


CREATE TABLE level_2_1 AS
SELECT DISTINCT (level_2_id),level_1_id
FROM t_dim_product_category
WHERE (level_2_id*level_1_id)> 0;


INSERT INTO TABLE JFH_persona_output partition(ds='20170901')
SELECT level_id,
       count(*) AS standar_freq,
       '21' AS level_2_threshold
FROM article_level_month_standardization
WHERE level_standardization>1
  AND level_type=2
  AND article_channel_id=1
  AND ds='20170904'
  AND level_id !=-1
GROUP BY level_id
ORDER BY level_id ASC;


LIMIT 50;


CREATE TABLE tmp_article_level_2_standar AS
SELECT t.sum_level_num,
       t.stddev,
       t.avg,
       t.article_channel_id,
       t1.level_1_id
FROM tmp_leve_2_avg t
LEFT JOIN level_2_1 t1 ON t.level_id=t1.level_2_id;
