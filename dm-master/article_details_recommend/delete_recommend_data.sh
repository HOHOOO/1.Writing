# 删除2天前的数据
#mysql -hsmzdm_recommend_mysql_m01 -urecommendUser -ppVhXTntx9ZG recommendDB -e "
#delete from similary_combine_recommend where ctime < date_format(date_add(now(), interval -1 day),'%Y-%m-%d');
#"

# 删除过去24小时的数据
mysql -hsmzdm_recommend_mysql_m01 -urecommendUser -ppVhXTntx9ZG recommendDB -e "
delete from similary_combine_recommend where ctime < date_add(now(), interval -1 day);
"
