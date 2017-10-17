#!/bin/bash

source ~/.bash_profile


data=`date -d "$1 day ago" +"%Y%m%d"`
echo $data
hdfs dfs -rm -r -f -skipTrash /dataOffline/JFH_persona_output/ds=$data

spark-sql --driver-class-path /data/tmp/zhanshulin/jar/mysql-connector-java-commercial-5.1.40-bin.jar --name 分类标准化 --driver-memory 8g --executor-memory 8G --executor-cores 4 --num-executors 3 --conf "spark.default.parallelism=100" --master yarn --queue q_gmv --master yarn -e"
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;
set mapred.job.queue.name=q_gmv;

use default;
set hive.map.aggr=true;
set hive.groupby.skewindata=true;
set hive.exec.parallel=true;
set mapred.reduce.tasks=10;



INSERT INTO TABLE JFH_persona_output partition(ds='$data')
SELECT level_id,
       count(*) AS standar_freq,
       '0' AS level_1_threshold
FROM JFH_level_month_standardization
WHERE level_standardization>0
  AND level_type=1
  AND article_channel_id=1
  AND ds='$data'
  AND level_id !=-1
GROUP BY level_id
ORDER BY level_id ASC;

INSERT INTO TABLE JFH_persona_output partition(ds='$data')
SELECT level_id,
       count(*) AS standar_freq,
       '1' AS level_1_threshold
FROM JFH_level_month_standardization
WHERE level_standardization>1
  AND level_type=1
  AND article_channel_id=1
  AND ds='$data'
  AND level_id !=-1
GROUP BY level_id
ORDER BY level_id ASC;

INSERT INTO TABLE JFH_persona_output partition(ds='$data')
SELECT level_id,
       count(*) AS standar_freq,
       '2' AS level_1_threshold
FROM JFH_level_month_standardization
WHERE level_standardization>2
  AND level_type=1
  AND article_channel_id=1
  AND ds='$data'
  AND level_id !=-1
GROUP BY level_id
ORDER BY level_id ASC;

INSERT INTO TABLE JFH_persona_output partition(ds='$data')
SELECT level_id,
       count(*) AS standar_freq,
       '3' AS level_1_threshold
FROM JFH_level_month_standardization
WHERE level_standardization>3
  AND level_type=1
  AND article_channel_id=1
  AND ds='$data'
  AND level_id !=-1
GROUP BY level_id
ORDER BY level_id ASC;

INSERT INTO TABLE JFH_persona_output partition(ds='$data')
SELECT level_id,
       count(*) AS standar_freq,
       '21' AS level_2_threshold
FROM article_level_month_standardization
WHERE level_standardization>1
  AND level_type=2
  AND article_channel_id=1
  AND ds='$data'
  AND level_id !=-1
GROUP BY level_id
ORDER BY level_id ASC;

"
