INSERT OVERWRITE TABLE DW_RECSYS_USER_RELATION_APPLIED_BAT PARTITION(SNAPSHOT_DATE='$day_end') AS
SELECT USER_PROXY_KEY,
       TAG_ID,
       SUM(INDEX_FACTOR) AS USER_TAG_WEIGHT
FROM (
SELECT USER_PROXY_KEY,
       TAG_ID,
       USER_TAG_ACTION_COUNT,
       USER_TAG_VALUE_SUM,
       USER_TAG_WEIGHT,
       CASE
           WHEN $function_model=1 THEN USER_TAG_WEIGHT * ($function_para1 * datadiff(SNAPSHOT, '$day_time') / $time_window * (-1) + $function_para2)
           WHEN $function_model=2 THEN USER_TAG_WEIGHT * (1 / (1 + $function_para1 * exp ((1/$function_para1) * ((2/$function_para1)*datadiff(SNAPSHOT, '$day_time') / $time_window - $function_para2))))
           WHEN $function_model=3 THEN USER_TAG_WEIGHT * (pow (($function_para2 * datadiff (SNAPSHOT, '$day_time') / $time_window), (1/$function_para1)))
           ELSE USER_TAG_WEIGHT
       END AS INDEX_FACTOR
FROM DW_RECSYS_USER_RELATION_DAILY
WHERE SNAPSHOT >= '$day_start';

) GROUPBY USER_PROXY_KEY,
  TAG_ID;
