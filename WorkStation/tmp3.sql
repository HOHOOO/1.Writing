CREATE TABLE wilson_data_source as SELECT id,(worthy+unworthy) as n,CASE WHEN (worthy+unworthy)=0 THEN -1.96 ELSE   (worthy/(worthy+unworthy)) END as phat,worthy,unworthy,source_from FROM sync_youhui;

CREATE TABLE wilson_data_lower  AS SELELCT id,(phat+zscore/n-zscore*sqrt(phat*(1-phat)/n+zscore/(4*pow(n,2))))/(1+zscore/n) as wilson_lower from wilson_data_source
CREATE TABLE wilson_data_lower  AS SELECT id, CASE WHEN phat=-1.96 THEN 0 ELSE (phat + 1.96/n - 1.96*sqrt((phat * (1- phat) /n)+1.96/(4*pow(n,2))))/(1+1.96/n) END as wilson_lower ,source_from from wilson_data_source;


SELECT AVG(wilson_lower),source_from FROM wilson_data_lower GROUP BY source_from LIMIT 100;

worthy              	int
unworthy            	int
source_from 1、5被降权
