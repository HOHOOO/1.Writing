#!/bin/bash
home_path=$(cd "`dirname $0`"; pwd)
three_month_ago=`date -d "90 day ago" +"%Y-%m-%d"`
two_week_ago=`date -d "14 day ago" +"%Y-%m-%d"`
one_day_ago=`date -d "1 day ago" +"%Y-%m-%d"`

hdfs dfs -rm -r -f -skipTrash /dataOffline/sync_smzdm_brand/*
hdfs dfs -rm -r -f -skipTrash /dataOffline/sync_smzdm_mall/*
hdfs dfs -rm -r -f -skipTrash /dataOffline/smzdm_product_category/*

#sqoop sync_smzdm_brand

sqoop import --connect 'jdbc:mysql://smzdm_recommend_mysql_m01_184/recommendDB?zeroDateTimeBehavior=convertToNull?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false' --username recommendUser --password pVhXTntx9ZG --query "select * from sync_smzdm_brand where \$CONDITIONS " --split-by id -m 4 --fields-terminated-by '\001' --null-string '' --null-non-string '' --hive-drop-import-delims --as-textfile --delete-target-dir --target-dir /dataOffline/sync_smzdm_brand --hive-table sync_smzdm_brand > $home_path/file/log/sync_smzdm_brand.log 2>&1
sqoop import --connect 'jdbc:mysql://smzdm_recommend_mysql_m01_184/recommendDB?zeroDateTimeBehavior=convertToNull?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false' --username recommendUser --password pVhXTntx9ZG --query "select * from sync_smzdm_mall where \$CONDITIONS " --split-by id -m 4 --fields-terminated-by '\001' --null-string '' --null-non-string '' --hive-drop-import-delims --as-textfile --delete-target-dir --target-dir /dataOffline/sync_smzdm_mall --hive-table sync_smzdm_mall > $home_path/file/log/sync_smzdm_mall.log 2>&1
sqoop import --connect 'jdbc:mysql://10.19.57.228/BasedataDB?zeroDateTimeBehavior=convertToNull?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false' --username smzdm_basedata --password CFezTQJ4KI --query "select * from smzdm_product_category where \$CONDITIONS " --split-by id -m 4 --fields-terminated-by '\001' --null-string '' --null-non-string '' --hive-drop-import-delims --as-textfile --delete-target-dir --target-dir /dataOffline/smzdm_product_category --hive-table smzdm_product_category  > $home_path/file/log//smzdm_product_category.log 2>&1


hive -e '
set mapred.job.queue.name=q_gmv;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;
set hive.groupby.skewindata=true;
set hive.exec.parallel=true;
set mapred.reduce.tasks=10;

drop table tabu_tag_list_mall;
drop table tabu_tag_list_brand;
drop table tabu_tag_list_cate;
drop table dw_cp_dic_tag_info_clean;
drop table tabu_tag_list;
drop table dw_cp_content_tag_info;
drop table tabu_tag_list_sift_1;
drop table tabu_tag_list_sift_2;
drop table dw_cp_dic_tag_info_clean_regexp;

CREATE TABLE IF NOT EXISTS tabu_tag_list_mall as SELECT b.key, b.value
FROM sync_smzdm_mall a
LATERAL VIEW explode (map(
`name_cn`, 0,
`name_cn_2`, 0,
`name_en`, 0,
`name_en_2`, 0
)) b as key, value;

CREATE TABLE IF NOT EXISTS tabu_tag_list_brand as SELECT b.key, b.value
FROM sync_smzdm_brand a
LATERAL VIEW explode (map(
`cn_title`,0,
`en_title`,0,
`common_title`,0,
`associate_title`,0,
`another_title`,0
)) b as key, value;

CREATE TABLE IF NOT EXISTS tabu_tag_list_cate as SELECT b.key, b.value
FROM smzdm_product_category a
LATERAL VIEW explode (map(
`title`,0,
`nicktitle`,0,
`search_nicktitle`,0,
`seo_nicktitle`,0
)) b as key, value;

CREATE EXTERNAL TABLE IF NOT EXISTS `dw_cp_tag_statistics_daily_long_term`(
  `tag_id` string,
  `user_action_id` string,
  `tag_action_count` double,
  `tag_action_uv_count` double,
  `tag_type_id` int,
  `ds` string)
  LOCATION
    "/recommend/dw/dw_cp_tag_statistics_daily_long_term";

CREATE EXTERNAL TABLE IF NOT EXISTS `dw_cp_dic_tag_info_long_term`(
`tag_id` string,
`tag_name` string,
`tag_type_id` string,
`tag_weight_estimate` float,
`tag_weight_machine` float,
`ds` string)
LOCATION
  "/recommend/dw/dw_cp_dic_tag_info_long_term";

select "tabu 73262";
create table tabu_tag_list as SELECT b.key, a.value from
(select key,value from tabu_tag_list_cate
where key<>"
union all
select key,value from tabu_tag_list_brand
where key<>"
union all
select key,value from tabu_tag_list_mall
where key<>") a
LATERAL VIEW explode (split (key,",")) b AS key;


select "tag 213693/231851";
CREATE TABLE dw_cp_dic_tag_info_clean as SELECT * FROM recommend.dw_cp_dic_tag_info a LEFT OUTER JOIN tabu_tag_list b ON a.tag_name = b.key and a.ds="$one_day_ago" and a.tag_type_id="400" where b.value IS null;

select "最近活跃日期为两周内 -85551 dw_cp_tag_statistics_daily_long_term为记录标签最后活跃日期的长期表 日 uv action>2 视为活跃";
INSERT OVERWRITE TABLE recommend.dw_cp_tag_statistics_daily_long_term
Select b.tag_id,b.user_action_id,b.tag_action_count,b.tag_action_uv_count,b.tag_type_id,b.ds from (
      select a.*,rank() over (partition by tag_id order by to_date(ds) desc) as order_rank
      from (select * from dw_cp_tag_statistics_daily_long_term WHERE ds<>"$one_day_ago"  and tag_action_uv_count > 2
            union all
           select * from dw_cp_tag_statistics_daily where ds="$one_day_ago" and tag_action_uv_count > 2) a
) b  where order_rank=1 ;


select "标签诞生日期为两周内 两周之内/两周之前 237/231618 dw_cp_dic_tag_info_long_term为统计标签诞生时间";
INSERT OVERWRITE TABLE recommend.dw_cp_dic_tag_info_long_term
Select b.tag_id,b.tag_name,b.tag_type_id,b.tag_weight_estimate,b.tag_weight_machine,b.ds from (
      select a.*,rank() over (partition by tag_id order by to_date(ds) asc) as order_rank
      from (select * from dw_cp_dic_tag_info_long_term WHERE ds<>"$one_day_ago"
            union all
           select * from dw_cp_dic_tag_info where ds="$one_day_ago" and tag_type_id="400") a
) b  where order_rank=1 ;



select "统计标签文章数 好价次数<5:197339  <6:203867  好文次数<1:4841";
create table dw_cp_content_tag_info as select tag_id,content_type_id, count(distinct(content_id)) as summary from dw_cp_content_tag_relation where ds>"$three_month_ago" and tag_id like "tag%" group by tag_id,content_type_id;


select "-77778";
CREATE TABLE tabu_tag_list_sift_1 as SELECT a.tag_id,a.tag_name,a.tag_weight_estimate,a.tag_weight_machine,b.user_action_id,b.tag_action_count,b.tag_action_uv_count, b.tag_type_id,a.ds,b.ds FROM recommend.dw_cp_dic_tag_info_long_term a LEFT OUTER JOIN recommend.dw_cp_tag_statistics_daily_long_term b ON a.tag_id = b.tag_id and a.ds<"$two_week_ago" and b.ds>"$two_week_ago" and a.tag_type_id="400" where b.tag_type_id IS null;


select "-189670";
CREATE TABLE tabu_tag_list_sift_2 as SELECT a.*,b.content_type_id,b.summary FROM tabu_tag_list_sift_1 a LEFT JOIN dw_cp_content_tag_info b ON a.tag_id = b.tag_id and ((b.content_type_id="300" and b.summary<6) or (b.content_type_id="500" and b.summary<2)) where b.summary IS not null;


select "剩余23173";
CREATE TABLE dw_cp_dic_tag_info_clean_regexp as select * from dw_cp_dic_tag_info_clean_sift where tag_name regexp "^[\\u4E00-\\u9FA5A-Za-z0-9]{2,9}$" and tag_name not regexp "^201(6|5)" limit 100 ;
'
