#!/bin/bash
source ~/.bash_profile
today=`date -d "$1 day ago" +"%Y%m%d"`

mysql -hsmzdm_recommend_mysql_s01_150  -urecommendUser -ppVhXTntx9ZG recommendDB -e "
drop table home_article_quality_score;
create table home_article_quality_score(id int(11) , score double(10,4) , last_status double(10,4),increase_rate double(10,4),order_rank  int(11) ,score_timestamp  int(11) ,class  int(11) );
"
sqoop export --connect jdbc:mysql://smzdm_recommend_mysql_s01_150/recommendDB --username recommendUser --password pVhXTntx9ZG --table home_article_quality_score --fields-terminated-by '\001' --export-dir 'hdfs://hadoopcluster/user/hive/warehouse/recommend.db/quality_data_score'

mysql -hsmzdm_recommend_mysql_s01_150  -urecommendUser -ppVhXTntx9ZG recommendDB -e "update home_article_online a, home_article_quality_score b
set a.hot_score = b.score,
a.hot_increase_rate = b. increase_rate
where a.article_id= b.id
and ((a.channel='yh' and b.class=0)
or (a.channel='yc' and b.class=1));"
