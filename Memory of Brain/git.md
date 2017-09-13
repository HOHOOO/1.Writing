#

#

####

#

#

#

> Search keymap

| key       | usage            |
| --------- | ---------------- |
| git fetch | Preview any file |
|           |                  |

git fetch
git checkout origin/master -- path/to/file

create branch

git show <commit-hashId> 便可以显示某次提交的修改内容
同样 git show <commit-hashId> filename 可以显示某次提交的某个内容的修改信息。

Git鼓励大量使用分支：</p>
查看分支：<code>git branch</code></p>
创建分支：<code>git branch \<name></code></p>
切换分支：<code>git checkout \<name></code></p>
创建+切换分支：<code>git checkout -b \<name></code></p>
合并某分支到当前分支：<code>git merge \<name></code></p>
删除分支：<code>git branch -d \<name></code></p>

Git中从远程的分支获取最新的版本到本地有这样2个命令：

1.  git fetch：相当于是从远程获取最新版本到本地，不会自动merge

Git fetch origin master
git log -p master..origin/master
git merge origin/master

    以上命令的含义：

   首先从远程的origin的master主分支下载最新的版本到origin/master分支上
   然后比较本地的master分支和origin/master分支的差别
   最后进行合并
   上述过程其实可以用以下更清晰的方式来进行：
 git fetch origin master:tmp
git diff tmp
git merge tmp

    从远程获取最新的版本到本地的test分支上

   之后再进行比较合并

2.  git pull：相当于是从远程获取最新版本并merge到本地

 Update the data table name

上述命令其实相当于git fetch 和 git merge
在实际使用中，git fetch更安全一些
因为在merge前，我们可以查看更新情况，然后再决定是否合并
结束

另一哥们将分支push到库中，我怎么获取到他的分支信息呢？

如果安装了git客户端，直接选择fetch一下，就可以获取到了。

如果用命令行，运行 git fetch，可以将远程分支信息获取到本地，再运行 git checkout -b local-branchname origin/remote_branchname  就可以将远程分支映射到本地命名为local-branchname  的一分支。

spark-sql  --queue q_gmv --name etl --driver-memory 8G --num-executors 4 --master yarn --executor-memory 8G -v -f /data/source/dm/ml_recsys2/job_transform/sql/dw_recsys_user_tag_relation_applied_bat.sql -hiveconf ds='2017-09-05' -hiveconf V_DayEnd='2017-09-05' -hiveconf V_DayStart='2017-09-05' -hiveconf V_FunctionModel=1 -hiveconf V_FunctionPara1=1 -hiveconf V_FunctionPara2=1
fi

Algorithm engineer
CAPTCHA
Data Analyst

git 本地覆盖远端
denyCurrentBranch = ignore
