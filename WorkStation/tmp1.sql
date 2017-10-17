CREATE TABLE tabu_tag_list AS
SELECT COALESCE(CASE when(a.user_id='') THEN NULL
                    ELSE a.user_id
                END,
                a.device_id) AS user_proxy_key,
       b.key,
       b.value
FROM user_dislike_content a
CREATE TABLE tabu_tag_list_mall AS
SELECT b.key,
       b.value
FROM sync_smzdm_mall a LATERAL VIEW explode (map(`name_cn`, name_cn, `name_cn_2`, name_cn_2, `name_en`, name_en, `name_en_2`, name_en_2)) b AS KEY,
                                            value;


CREATE TABLE tabu_tag_list_mall AS
SELECT b.key,
       b.value
FROM sync_smzdm_mall a LATERAL VIEW explode (map(`name_cn`, 0, `name_cn_2`, 0, `name_en`, 0, `name_en_2`, 0)) b AS KEY,
                                            value;


CREATE TABLE tabu_tag_list_mall AS
SELECT b.key,
       b.value
FROM sync_smzdm_mall a LATERAL VIEW explode (map(`name_cn`, name_cn, `name_cn_2`, name_cn_2, `name_en`, name_en, `name_en_2`, name_en_2)) b AS KEY,
                                            value;

sqoop import --connect 'jdbc:mysql://smzdm_recommend_mysql_m01_184/recommendDB?zeroDateTimeBehavior=convertToNull?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false' --username recommendUser --password pVhXTntx9ZG --query "select * from smzdm_product_category where \$CONDITIONS " --split-by id -m 4 --fields-terminated-by '\001' --null-string '' --null-non-string '' --hive-drop-import-delims --as-textfile --delete-target-dir --target-dir /dataOffline/smzdm_product_category --hive-table smzdm_product_category > /data/source/dm/ml_recsys2/file/log/smzdm_product_category.log 2>&1
 sqoop import --connect 'jdbc:mysql://10.19.57.228/BasedataDB?zeroDateTimeBehavior=convertToNull?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false' --username smzdm_basedata --password CFezTQJ4KI --query "select * from smzdm_product_category where \$CONDITIONS " --split-by id -m 4 --fields-terminated-by '\001' --null-string '' --null-non-string '' --hive-drop-import-delims --as-textfile --delete-target-dir --target-dir /dataOffline/smzdm_product_category --hive-table smzdm_product_category  > /data/source/dm/ml_recsys2/file/log//smzdm_product_category.log 2>&1

WHERE a.channel_id IN (1,
                       2,
                       5,
                       11)
  AND a.authenticity = 1
  AND to_date(a.ctime) > DATE_SUB('${hiveconf:V_DayEnd}',(30+${hiveconf:V_DayLength}));


CREATE TABLE T6_test_TEMP AS
SELECT a.user_proxy_key,
       a.key,
       b.value
FROM T6_test a LATERAL VIEW explode (split (value,',')) b AS value;
