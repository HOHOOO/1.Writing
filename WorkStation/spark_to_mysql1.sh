#!/bin/bash
source ~/.bash_profile
today=`date -d "$1 day ago" +"%Y%m%d"`
echo "$today"
jar_dir="/data/tmp/zhanshulin/jar/sparkToMysql.jar"
jar_dir01="/data/tmp/zhanshulin/jar/ToMySQL.jar"
mysql -hsmzdm_recommend_mysql_s01_150  -urecommendUser -ppVhXTntx9ZG recommendDB -e "create table home_article_quality_score(id int(11) , score double(10,4) , last_status double(10,4),increase_rate double(10,4),order_rank  int(11) ,score_timestamp  int(11) ,class  int(11) );"
drop table home_article_quality_score;

sqoop export --connect jdbc:mysql://hsmzdm_recommend_mysql_s01_150/recommendDB --username recommendUser --password pVhXTntx9ZG --table quality_data_score --fields-terminated-by '\001' --export-dir 'hdfs://hadoopcluster/user/hive/warehouse/recommend.db/quality_data_score';

 
