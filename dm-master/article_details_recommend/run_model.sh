#!/bin/bash

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/data/anaconda2/bin"

source activate article_details_recommend

python /data/webroot/dm/article_details_recommend/run_model.py --config=/data/webroot/dm/article_details_recommend/config_online/ --log-file-prefix=/data/logs/article_details/model/model.log --logging=debug
