#!/bin/bash
# 一键更新、部署脚本
# update_local:更新当前服务器代码
# update_all:更新所有服务器代码
# restart_local:重启当前服务器代码
# restart_all:重启所有服务器代码
function useage()
{
    echo "first ensure you are the root or sudo"
    echo ""
    echo "usage:"
    echo "./update_server.sh <action>"
    echo ""
    echo "attention:"
    echo "   the action contain 'update_local', 'update_all', 'restart_local', 'restart_all'"
    echo ""
    exit 0
}

function update_local()
{
    if [ $# -ne 3 ]; then
        echo "usage:"
        echo "./update_server.sh update_local <git repository> <branch> <code path>"
        echo "example:"
        echo "./update_server.sh update_local https://gitlab-team.smzdm.com/smzdm/dm.git master /data/webroot/dm"
        exit 0
    fi
    git_repository=$1
    branch=$2
    code_path=$3
    echo "git_repository: $git_repository"
    echo "branch: $branch"
    echo "code_path: $code_path"
    # 不存在本地目录手动创建
    if [ ! -d $code_path ];then
        mkdir -p $code_path
    fi
    cd $code_path && pwd
    # 不存在.git目录    
    if [ ! -d "$code_path/.git" ];then
        echo "git clone $git_repository -b $branch -l $code_path"
        git clone $git_repository -b $branch -l $code_path
    else
        echo "git pull $git_repository $branch"
        git pull $git_repository $branch
    fi
    if [ $? == 0 ];then
       echo "127.0.0.1 update successed!" 
    else
       echo "127.0.0.1 update failed!" 
    fi 
}

function update_all()
{
    if [ $# -ne 4 ]; then
        echo "usage:"
        echo "./update_server.sh update_all <git repository> <branch> <code path> <ip1,ip2 or ip list file>"
        echo "example:"
        echo "./update_server.sh update_all https://gitlab-team.smzdm.com/smzdm/dm.git master /data/webroot/dm /data/webroot/dm/ips.txt"
        exit 0
    fi
    git_repository=$1
    branch=$2
    code_path=$3
    ips=$4
    echo ""
    echo "git_repository: $git_repository"
    echo "branch: $branch"
    echo "code_path: $code_path"
    echo "ips: $ips"
    echo ""
    #echo "update_all $git_repository $branch $code_path $ips"
    update_local $git_repository $branch $code_path
    echo ""
    if [ -f $ips ]; then
        while read line
        do
            local ip=$line
            echo "$ip update ...."
            rsync -av $code_path/ root@$ip:$code_path/
            if [ $? == 0 ];then
                echo "$ip update success!" 
            else
                echo "$ip update failed!" 
            fi
            echo ""
        done < $ips
    else
        for ip in `echo $ips | sed 's/,/ /g'`;
        do
            echo "$ip update ...."
            rsync -av $code_path/ root@$ip:$code_path/
            if [ $? == 0 ];then
                echo "$ip update success!" 
            else
                echo "$ip update failed!" 
            fi
            echo ""
        done
    fi
}

function restart_local()
{
    if [ $# -ne 3 ]; then
        echo "usage:"
        echo "./update_server.sh restart_local <code path> <port> 'python <file>.py'"
        echo "example:"
        echo "./update_server.sh restart_local /data/webroot/dm/article_details_recommend/ 8806 'python ./run_server.py --config=./config_online --port=8806 --process=0 --log_file_prefix=/data/logs/article_details/server/server.log --log-rotate-mode=time --logging=info --log-to-stderr=False'"
        exit 0
    fi
    
    local code_path=$1
    local port=$2
    local run_command=$3
    local self_name=`basename $0`
    #echo "code_path: $code_path"
    #echo "port: $port"
    #echo "run_command: $run_command"
    cd $code_path
    ps -ef | grep $port | grep -v grep | grep -v $self_name | awk '{print $2}' | xargs kill
    #echo $run_command
    nohup $run_command 2>&1 &
    if [ $? == 0 ]; then
        echo "127.0.0.1:$port process start successed!"
    else
        echo "127.0.0.1:$port process start failed!"
    fi
}

function restart_all()
{
    if [ $# -ne 4 ]; then
        echo "usage:"
        echo "./update_server.sh restart_all <code path> <port> 'python <file>.py' <ip1,ip2 or ip list file>"
        echo "example:"
        echo "./update_server.sh restart_all /data/webroot/dm/article_details_recommend/ 8806 'python ./run_server.py --config=./config_online --port=8806 --process=0 --log_file_prefix=/data/logs/article_details/server/server.log --log-rotate-mode=time --logging=info --log-to-stderr=False' /data/webroot/dm/ips.txt"
        exit 0
    fi
    
    local code_path=$1
    local port=$2
    local run_command=$3
    local ips=$4
    local self_name=`basename $0`
    echo ""
    echo "code_path: $code_path"
    echo "port: $port"
    echo "run_command: $run_command"
    echo "ips: $ips"
    echo ""
    #echo "restart_local $code_path $port $run_command"
    restart_local $code_path $port "$run_command"
    echo ""
    if [ -f $ips ]; then
        while read line
        do
            local ip=$line
            echo "login ip: $ip"
            ssh -f -n root@$ip "cd $code_path && ps -ef | grep $port | grep -v grep | awk '{print \$2}' |xargs kill && nohup $run_command 2>&1 &"
            #ssh -f -n root@$ip "cd $code_path && ps -ef | grep $port | grep -v grep | awk '{print \$2}' |xargs kill && nohup $run_command > /dev/null 2>&1 &"
            if [ $? == 0 ];then
                echo "$ip:$port process start successed!"
            else
                echo "$ip:$port process start failed!"
            fi

            # 3s后kill掉ssh登录进程
            sleep 3
            #echo `ps -ef | grep "ssh -f -n root@$ip cd $code_path" | grep -v "grep --color=auto" | awk '{print $2}'`
            ps -ef | grep "ssh -f -n root@$ip cd $code_path" | grep -v "grep --color=auto" | awk '{print $2}' | xargs kill
            echo ""
        done < $ips
    else
        for ip in `echo $ips | sed 's/,/ /g'`;
        do
            #ssh root@$ip "cd $code_path; nohup $run_command > /dev/null 2>&1 &"
            ssh -f -n root@$ip "cd $code_path && ps -ef | grep $port | grep -v grep | awk '{print \$2}' |xargs kill && nohup $run_command > /dev/null 2>&1 &"
            #echo "status:$?"
            if [ $? == 0 ];then
                echo "$ip:$port port process start successed!"
            else
                echo "$ip:$port port process start failed!"
            fi

            # 3s后kill掉ssh登录进程
            sleep 3
            #echo `ps -ef | grep "ssh -f -n root@$ip cd $code_path" | grep -v "grep --color=auto" | awk '{print $2}'`
            ps -ef | grep "ssh -f -n root@$ip cd $code_path" | grep -v "grep --color=auto" | awk '{print $2}' | xargs kill
        done
    fi
}

if [ $# -ge 1 ];then
    action=$1
    if [ $action == 'update_local' ]; then
        update_local $2 $3 $4
    elif [ $action == 'update_all' ]; then
        update_all $2 $3 $4 $5
    elif [ $action == 'restart_local' ]; then
        restart_local $2 $3 "$4"
    elif [ $action == 'restart_all' ]; then
        restart_all $2 $3 "$4" $5
    fi
else
    useage
fi

