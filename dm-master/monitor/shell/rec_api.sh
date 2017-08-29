#!/bin/bash

source ~/.bash_profile

token="e118a4e75784e5d84fca3e89d4c9b1c6c7451774d72e33d672b2b8ff9e17e6e4"
touser="robot"

tmpFile="/tmp/recommed_api_monitory_tmp"
rm -rf $tmpFile
##############################################################################################
# 接口监控
##############################################################################################


for h in smzdm_169 smzdm_17 smzdm_66
do
    http_code=`curl -XGET -I -m 10 -o /dev/null -s -w %{http_code} "http://$h:8816/recommend_system/editor_excellence_recommend/?device_id=nkk4CzScYwfYMRk2qv6hCgDOFE2CRzCOwAfulukRgoRnxkwz7Adw3g==1&with_top=1&page=1"`
    echo $h $http_code `date`

    if [ $http_code -ne 200 ] ; then
        echo "$h recommend api error" >> $tmpFile
    fi
done

http_code=`curl -XGET -I -m 10 -o /dev/null -s -w %{http_code} "http://system-recommend.smzdm.com:809/recommend_system/editor_excellence_recommend/?device_id=nkk4CzScYwfYMRk2qv6hCgDOFE2CRzCOwAfulukRgoRnxkwz7Adw3g==1&with_top=1&page=1"`
echo "system-recommend.smzdm.com $http_code" `date`

if [ $http_code -ne 200 ] ; then
    echo "system-recommend.smzdm.com api error" >> $tmpFile
fi

http_code=`curl -XPOST -s -m 10 -o /dev/null -w %{http_code} -H "'Content-type':'application/x-www-form-urlencoded','charset':'utf-8'" "http://system-recommend.smzdm.com:809/recommend_system/feedback/" -d "device_id=111&user_id=&authenticity=0&channel_id=1&article_id=123&cate=休闲零食&brand=三只松鼠&tag=&app_version=7.8"`
echo "system-recommend.smzdm.com $http_code" `date`

if [ $http_code -ne 200 ] ; then
    echo "system-recommend.smzdm.com feedback api error" >> $tmpFile
fi

##############################################################################################
# 业务监控程序
##############################################################################################
process_num=`ssh smzdm_169 ps -ef | grep consume_msg.py | grep -Ev 'grep|business' |wc -l`
echo "consume_msg.py process num: $process_num `date`"

if [ $process_num -ne 1 ]; then
    echo "consume_msg.py process down" >> $tmpFile
    ssh smzdm_169 "cd /data/webroot/dm/monitor/business && nohup python consume_msg.py ./config_online >> /dev/null 2>&1 &"
fi

##############################################################################################
# 报警
##############################################################################################

if [ -f $tmpFile ] ; then
    size=`ls -l $tmpFile | awk '{print $5}'`
    if [ $size -ge 0 ] ; then
   	curl -v -d "token=$token&touser=$touser&msg=`cat $tmpFile`" "http://hadoop004:1090/dingding/alarm" 
    fi
fi
rm -rf $tmpFile
