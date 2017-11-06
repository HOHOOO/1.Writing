SELECT "====================================";
SELECT "执行参数:";
SELECT "ds: ${hiveconf:ds}";
SELECT "days_length: ${hiveconf:days_length}";
SELECT "====================================";

DROP TABLE IF EXISTS recommend.DW_CP_UPDATE_TAG_IDF_INFO;


SELECT "@@@ CREATE TABLE DW_CP_UPDATE_TAG_IDF_INFO";
CREATE TABLE recommend.DW_CP_UPDATE_TAG_IDF_INFO
AS

WITH
--计算所有标签的用户访问总数
T1 AS (SELECT
COUNT (DISTINCT USER_PROXY_KEY) AS DIS_USER_NUM
FROM recommend.DW_CP_USER_PREFERENCE_30_DAYS
WHERE ds > DATE_SUB('${hiveconf:ds}', ${hiveconf:days_length})
AND ds <= '${hiveconf:ds}'),

--计算每个标签的用户访问总数
T2 AS (SELECT
TAG_ID,
COUNT (DISTINCT USER_PROXY_KEY) AS DIS_TAG_USER_NUM
FROM recommend.DW_CP_USER_PREFERENCE_30_DAYS
WHERE ds > DATE_SUB('${hiveconf:ds}', ${hiveconf:days_length})
AND ds <= '${hiveconf:ds}'
GROUP BY TAG_ID),

--关联两张表结果
T3 AS (SELECT
T2.TAG_ID,
T2.DIS_TAG_USER_NUM,
T1.DIS_USER_NUM
FROM T1
JOIN T2
ON 1 == 1)
--
--计算每个标签的IDF值
SELECT
TAG_ID,
ROUND (LOG(10, (DIS_USER_NUM + 2) / (DIS_TAG_USER_NUM + 1)), 4) AS IDF
FROM T3;

SELECT "@@@ INSERT TABLE DW_CP_DIC_TAG_INFO_TEST";
INSERT OVERWRITE TABLE recommend.DW_CP_DIC_TAG_INFO_TEST
PARTITION (ds = '${hiveconf:ds}')
SELECT
a.TAG_ID,
a.TAG_NAME,
a.TAG_TYPE_ID,
a.TAG_WEIGHT_ESTIMATE,
b.IDF
FROM recommend.DW_CP_DIC_TAG_INFO AS a
LEFT JOIN recommend.DW_CP_UPDATE_TAG_IDF_INFO_TEST AS b
ON a.TAG_ID = b.TAG_ID
WHERE a.ds = '${hiveconf:ds}';
