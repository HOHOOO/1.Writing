#!/bin/bash
source ~/.bash_profile

 for (( i=36; i<=37; i++ ))
do
echo "i=$i"
year=`date -d "1 day ago" +"%Y%m%d"`
echo $year
sh /data/tmp/zhanshulin/JFH_spark_hive.sh $i


done
