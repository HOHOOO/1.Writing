#!/bin/bash

create_table_and_partition()
{
    hive_db=${1}
    hive_tbl=${2}
    ds=${3}
    hive_location=${4}

    echo "===================input params start==================="
    echo "hive_db: ${hive_db}"
    echo "hive_tbl: ${hive_tbl}"
    echo "ds: ${ds}"
    echo "hive_location: ${hive_location}"
    echo "===================input params end====================="
    
    hive -e "
    CREATE EXTERNAL TABLE IF NOT EXISTS $hive_db.$hive_tbl(
    TAG_ID string COMMENT '标签ID',
    TAG_NAME string COMMENT '标签名称',
    TAG_TYPE_ID string COMMENT '标签类别(300:显式标签,500:隐式标签,700:机器标签)',
    TAG_WEIGHT_ESTIMATE float COMMENT '人为评估权重',
    TAG_WEIGHT_MACHINE float COMMENT '机器评估权重'
    )PARTITIONED BY (
    DS string)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\001'
    LOCATION '$hive_location';

    ALTER TABLE $hive_db.$hive_tbl DROP IF EXISTS PARTITION (ds='${ds}');

    ALTER TABLE $hive_db.$hive_tbl ADD IF NOT EXISTS PARTITION (ds='${ds}') LOCATION '$hive_location/$ds';
    "
}

query="
select concat_ws('_', 'tag', id) as tag_id, name as tag_name, 400 as tag_type_id, 1.0 as tag_weight_estimate, 0.0 as tag_weight_machine
from sync_smzdm_tag_type
where \$CONDITIONS
union
select concat_ws('_', 'cate', id) as tag_id, title as tag_name, 300 as tag_type_id, 1.0 as tag_weight_estimate, 0.0 as tag_weight_machine
from sync_smzdm_product_category
where is_deleted=0 and \$CONDITIONS
union
select concat_ws('_', 'brand', id) as tag_id, associate_title as tag_name, 300 as tag_type_id, 1.0 as tag_weight_estimate, 0.0 as tag_weight_machine
from sync_smzdm_brand
where is_deleted=0 and \$CONDITIONS"

boundary_query="
select min(id), max(id)
from
(select id from sync_smzdm_tag_type
union all
select id from sync_smzdm_product_category
where is_deleted=0
union all
select id from sync_smzdm_brand where
is_deleted=0) as t"
split_by="id"
model="overwrite"

ds=${1}
threads=${2}
queue=${3}

if [ ! $ds ]; then
    ds=`date -d "-1 day" +"%Y-%m-%d"`
fi

if [ ! $threads ]; then
    threads=10
fi   

if [ ! $queue ]; then
    queue="q_gmv"
fi

home_path=$(cd "`dirname $0`"/..; pwd)
db_fg="$home_path/pub/db.fg"

hive_db="recommend"
hive_tbl="dw_cp_dic_tag_info"
hive_location="/recommend/dw/dw_cp_dic_tag_info"

# 创建hive表和分区
create_table_and_partition $hive_db $hive_tbl $ds $hive_location

sh $home_path/pub/sqoop_from_mysql_import_hive.sh $db_fg $threads "$hive_location/$ds" "$query" "$boundary_query" $split_by $model $queue

