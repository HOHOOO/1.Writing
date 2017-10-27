  #!/bin/bash
  source ~/.bash_profile
  today=`date -d "$1 day ago" +"%Y%m%d"`
  echo "$today"
  jar_dir="/data/tmp/zhanshulin/jar/sparkToMysql.jar"
  jar_dir01="/data/tmp/zhanshulin/jar/ToMySQL.jar"
  mysql -hsmzdm_recommend_mysql_m01_184  -urecommendUser -ppVhXTntx9ZG recommendDB -e "drop table home_article_quality_score;"
  mysql -hsmzdm_recommend_mysql_m01_184   -urecommendUser -ppVhXTntx9ZG recommendDB -e "create table home_article_quality_score(id int(11) , score double(10,4) , last_status double(10,4),increase_rate double(10,4),order_rank  int(11) ,score_timestamp  int(11) ,class  int(11) );"
  mysql -hsmzdm_recommend_mysql_m01_184  -urecommendUser -ppVhXTntx9ZG recommendDB -e "TRUNCATE table home_article_quality_score;"

  spark-submit --master yarn --class sparkToMysql --name 写Mysql  --driver-memory 8g --executor-memory 7G --executor-cores 2 --num-executors 7 --conf "spark.default.parallelism=100" --queue q_gmv --jars /data/source/dm/ml_recsys2/file/jar/mysql-connector-java-commercial-5.1.40-bin.jar /data/source/dm/ml_recsys2/file/jar/sparkToMysql.jar  hdfs://hadoopcluster/user/hive/warehouse/recommend.db/quality_data_score/* home_article_quality_score;

    echo "spark-submit --master yarn --class ToMySQL --name 写Mysql  --driver-memory 8g --executor-memory 7G --executor-cores 2 --num-executors 7 --conf "spark.default.parallelism=100" --queue q_gmv --jars /data/tmp/zhanshulin/jar/mysql-connector-java-commercial-5.1.40-bin.jar $jar_dir  /dataOffline/article_level_week_standar/ds=$today/* article_level_standar_week_$today "

  mysql -hsmzdm_recommend_mysql_m01_184  -urecommendUser -ppVhXTntx9ZG recommendDB -e "
  CREATE TABLE article_level_standar_week_$today (
    id int(11) unsigned NOT NULL AUTO_INCREMENT,
    device_id varchar(200) COLLATE utf8_bin NOT NULL,
    user_id bigint(8) DEFAULT NULL,
    article_channel_name varchar(50) COLLATE utf8_bin DEFAULT NULL,
    level_type tinyint(1) NOT NULL,
    level_id int(8) NOT NULL,
    level_num int(8) NOT NULL,
    level_standardization float(9,4) DEFAULT NULL,
    article_channel_id tinyint(1) DEFAULT NULL,
    PRIMARY KEY (id),
    KEY ind_user_id (user_id),
    KEY ind_device_id (device_id(20)),
    KEY index_union (level_standardization,level_id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
  "
  mysql -hsmzdm_recommend_mysql_m01_184  -urecommendUser -ppVhXTntx9ZG recommendDB -e " TABLE article_level_standar_week_$today"






  echo "spark-submit --master yarn --class ToMySQL --name 写Mysql  --driver-memory 8g --executor-memory 7G --executor-cores 2 --num-executors 7 --conf "spark.default.parallelism=100" --queue q_gmv --jars /data/tmp/zhanshulin/jar/mysql-connector-java-commercial-5.1.40-bin.jar $jar_dir  /dataOffline/article_level_week_standar/ds=$today/* article_level_standar_week_$today "
  spark-submit --master yarn --class sparkToMysql --name 写Mysql  --driver-memory 8g --executor-memory 7G --executor-cores 2 --num-executors 7 --conf "spark.default.parallelism=100" --queue q_gmv --jars /data/tmp/zhanshulin/jar/mysql-connector-java-commercial-5.1.40-bin.jar $jar_dir  /dataOffline/article_level_week_standar/ds=$today/* article_level_standar_week_$today
