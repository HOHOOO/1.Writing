
#!/bin/bash
source ~/.bash_profile
home_path=$(cd "`dirname $0`"; pwd)
day_time=`date -d "1 day ago" +"%Y%m%d"`
day5_time=`date -d "5 day ago" +"%Y%m%d"`
version=$1
echo "user_preference_$version"


hive -e "set mapred.job.queue.name=q_gmv;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;
set hive.groupby.skewindata=true;
set hive.exec.parallel=true;
set mapred.reduce.tasks=10;
DROP TABLE DW_CP_USER_PREFERENCE_$version_REDIS;
DROP TABLE DW_CP_USER_PREFERENCE_$version_REDIS_TEMP;
DROP TABLE DW_CP_USER_PREFERENCE_$version_REDIS_temp2;
DROP TABLE T9_$version;

CREATE TABLE T9_$version AS SELECT user_proxy_key, channel, CONCAT_WS(',', COLLECT_SET(concat(tag_id,':',user_tag_weight))) AS tag_str_draft from ( select user_proxy_key, split(tag_id,'_')[0] as channel, split(tag_id,'_')[1] as tag_id , user_tag_weight FROM recommend.DW_CP_USER_PREFERENCE_$version_LONG_TERM ) t GROUP BY user_proxy_key, channel;
CREATE TABLE DW_CP_USER_PREFERENCE_$version_REDIS_TEMP AS SELECT user_proxy_key, CONCAT_WS('#', COLLECT_SET(concat(channel,'_',tag_str_draft))) as tag_str from T9_$version GROUP BY user_proxy_key;
CREATE TABLE DW_CP_USER_PREFERENCE_$version_REDIS_temp2 AS SELECT user_proxy_key ,regexp_extract(tag_str,'cate_(.*?)(#|$)', 1) AS cate,regexp_extract(tag_str,'brand_(.*?)(#|$)', 1) AS brand,regexp_extract(tag_str,'tag_(.*?)(#|$)', 1) AS tag from DW_CP_USER_PREFERENCE_$version_REDIS_TEMP;
CREATE TABLE DW_CP_USER_PREFERENCE_$version_REDIS AS SELECT user_proxy_key,CASE WHEN cate IS NULL OR cate=' THEN '-1:-1' ELSE cate END as cate,CASE WHEN brand IS NULL OR brand=' THEN '-1:-1' ELSE brand END as brand,CASE WHEN tag IS NULL OR tag=' THEN '-1:-1' ELSE tag END as tag from DW_CP_USER_PREFERENCE_$version_REDIS_temp2;

DROP TABLE DW_CP_USER_PREFERENCE_$version_REDIS_TEMP;
DROP TABLE DW_CP_USER_PREFERENCE_$version_REDIS_temp2;
DROP TABLE T9_$version;
"

rm -rf $home_path/file/dw_cp_user_preference_test_redis_$day_time.txt
rm -rf $home_path/file/dw_cp_user_preference_test_redis_$day5_time.txt
hdfs dfs -rm -r -f -skipTrash /user/hadoop/dw_cp_user_preference_test_redis_$day_time.txt
hdfs dfs -rm -r -f -skipTrash /user/hadoop/dw_cp_user_preference_test_redis_$day5_time.txt
hadoop dfs -rm -r -f -skipTrash /user/hive/warehouse/dw_cp_user_preference_test_redis/.hive*
hdfs dfs -cat /user/hive/warehouse/dw_cp_user_preference_test_redis/* | hdfs dfs -appendToFile - /user/hadoop/dw_cp_user_preference_test_redis_$day_time.txt
hdfs dfs -get /user/hadoop/dw_cp_user_preference_test_redis_$day_time.txt $home_path/file/


###################################################################################################
#################
# 设置报警
###################################################################################################
#################
token="e118a4e75784e5d84fca3e89d4c9b1c6c7451774d72e33d672b2b8ff9e17e6e4"
touser="robot"
tmpFile="$home_path/file/write_test_redis.txt"
hdfs dfs -test -s /user/hadoop/dw_cp_user_preference_test_redis_$day_time.txt
if [ $? -eq 1 ] ; then
        echo "dw_cp_user_preference_test.sh  failed" >> $tmpFile
else
        file_size_tmp=`hdfs dfs -du -s /user/hive/warehouse/dw_cp_user_preference_test_redis/ | awk '{print $1}'`
        file_size=$((file_size_tmp/1024/1024))
        if [ $(echo "$file_size > 2000"|bc) = 1 ] ; then
        echo "write redis"
        echo "`date`: start write test(the version) redis" >> $home_path/file/write_test_redis.log 2>&1
        python $home_path/write_test_redis.py $home_path/file/dw_cp_user_preference_test_redis_$day_time.txt
        echo "`date`: finish write test(the version) redis" >> $home_path/file/write_test_redis.log 2>&1
        echo "end"
	echo "THE End $(date)"
        else
        echo "user preference redis size($file_size M) less than 3000M and therefore not be written, using yesterday redis. Please deal with it in time!" >> $tmpFile
        fi
fi
