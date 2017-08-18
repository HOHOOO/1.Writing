# 一文读懂SparkR综述
文\/侯志伟

## 1 前言



R是数据科学中最流行的语言之一，在刚刚发布的2017年度IEEE编程语言排行榜中名列第六。当然，R也并非一门尽善尽美的语言，比如其广受诟病的工程化能力较弱，以及其对深度学习支持能力较弱等缺点，让越来越多的R爱好者疾呼：“人生苦短，python是岸”。
然而，语言只是工具，数据和算法才是数据科学的核心，过分关注语言本身，反而南辕北辙。需要强调的是：
1. 要看到R的弱点正在不断被克服。吐槽缺点并不能改变现状，越来越多的R语言爱好者、社区、企业全力投入，才有了今天要重点说的将R语言工程化的SparkR和[Microsoft R Server](https://www.microsoft.com/en-us/cloud-platform/r-server)，以及那些未被提到的让R支持Tensorflow、Keras、Mxnet框架等项目。  
2. 多样化、差异化才是数据科学繁荣的真正愿景，每一种语言都能为使用它的人创造价值，学习自己喜欢的编程语言，而不是人云亦云，只学最流行的语言，而对其他语言嗤之以鼻。PHP是世界上最好的语言，然后呢？

不久前，Microsoft的朱晓勇在Strata Data Conference中介绍了SparkR的最新进展和大数据在Microsoft R Server上的最佳实践。让我们看到了微软在2015年收购[Revolution Analytics]()之后，在数据科学，特别是R的工程化方面做出的巨大努力和成果。借此机会，介绍下SparkR的背景和简单实践。

## 2 背景

### 2.1 Spark简介
>Apache Spark 是专为大规模数据处理而设计的快速通用的计算引擎。Spark是UC Berkeley AMP lab开源的类Hadoop MapReduce的通用并行框架。它拥有Hadoop MapReduce所具有的优点；但不同于MapReduce的是Job，中间输出结果可以保存在内存中，从而不再需要读写HDFS，也就是基于内存的计算，因此Spark能更好地适用于数据挖掘与机器学习等需要迭代的MapReduce的算法。

Spark基于弹性分布式数据集（Resilient Distributed Datasets，RDD），通过机器学习（MLlib）、图计算（GraphX）、流计算（Spark Streaming）和数据仓库（Spark SQL）等高效快捷的计算组件，以及多种编程语言API支持：
Spark Scala API (Scaladoc)
Spark Java API (Javadoc)
Spark Python API (Sphinx)
Spark R API (Roxygen2)
架构出一个全生态的海量数据分析应用平台。

![spark四大模块](https://i.loli.net/2017/08/05/5985765fa6d6d.png)


 ### 2.2 R语言简介
 >R是一套完整的数据处理、计算和制图软件系统。其功能包括：
 数据存储和处理系统；
 数组运算工具（其向量、矩阵运算方面功能尤其强大）；
 完整连贯的统计分析工具；
 优秀的统计制图功能；

![R语言的主要历史背景](https://i.loli.net/2017/08/05/5985c226c2fa3.png)
除了以上特点，R之所以能获得众多数据科学爱好者青睐的最重要的原因原因在于CRAN和社区融入。
拥有二十年历史，在全球上百个CRAN镜像站中收录着上万的包，覆盖了生物计算、经济计量、财经分析等各行各业。而且大多数包中不仅包含了算法的实现，还会包含一些对行业知识的理解和实践，这也是R在一定程度上被称为知识性语言的原因。
另外，R的社区融入也做的非常好，通过R帮助邮件列表可以轻松向R社区和R包作者提问交流，此外Stack Overflow和github以及国内的统计之都也有比较多的R语言爱好者进行问题交流。此外，线下在全球各地举办的R语言会议，也成为统计学和R语言爱好者的重要会议。
![](https://i.loli.net/2017/08/05/5985cb542ac93.png)

### 2.3 SparkR简介
>SparkR 是一个提供轻量级前端的R包，在R的基础上加入了Spark的分布式计算和存储等特性。 此外SparkR还可以通过连接MLlib、streaming、Spark SQL等组件完成诸如分布式的机器学习、流数据计算、数据仓库等操作。

SparkR，将Spark分布式扩展能力，以及R的统计功能完美的结合。
![SparkR是什么.png](http://upload-images.jianshu.io/upload_images/1515595-ce5b109862bbeb26.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

SparkR的出现，不仅是简单技术上的连接融合，也是传统以本地banR为主要工具的数据科学工作者者和数据工程师的思维碰撞。
![SparkR横空出世的原因](https://i.loli.net/2017/08/05/5985b2f29f396.png)



SparkR的架构：通过R-JVM桥接实现
![SparkR的架构.png](http://upload-images.jianshu.io/upload_images/1515595-835a87b690c08e2b.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

## 3 SparkR发展与近况
2013年，
- [AMP lab](https://amplab.cs.berkeley.edu/)开始内部孵化SparkR项目。

2014年，
- SparkR项目在[github](https://github.com/amplab-extras/SparkR-pkg)上开源。

2015年，
- Spark 1.4，SparkR并到Apache Spark中，并随着Spark 1.4一起发布。SparkR提供了Spark的两组API的R语言封装，即Spark Core的RDD API(隐藏)和基于Spark SQL 的DataFrame API。
![version1.4](https://i.loli.net/2017/08/05/59858de8b4ff7.png)
- Spark 1.5，SparkR 开始支持YARN，和一些优化，是的DataFrame API的处理过程与R语言自身的dataframe类似，完成选取、过滤和聚集等操作。但SparkR能够作用于更大规模的数据集。
![version1.5](https://i.loli.net/2017/08/05/59858db358cdc.png)

2016年，
- Spark2.0，伴随着Spark版本的重大更新，首先是Dataset API和DataFrame API的统一，除此之外，自定义函数的支持（R UDF support，主要是：dapply, gapply, and lapply）和封装更多算法，并提升性能（operational improvements）。
![version2.0](https://i.loli.net/2017/08/05/59858cd3a959a.png)

2017年，
- Spark2.1，最重大的更新莫过于可以直接在直接安装第三方包，而之前第三方的包仅能在RDD的API中安装并使用。
![version2.1](https://ooo.0o0.ooo/2017/08/05/59858c849a28d.png)
- Spark2.2，SprakR开始尝试支持流数据计算（试验版，详见[Structured Streaming Programming Guide](http://spark.apache.org/docs/2.2.0/structured-streaming-programming-guide.html)）
![version2.2](https://i.loli.net/2017/08/05/59858fe1ea46c.png)


## 4 SparkR当前支持功能概览与实践


目前SparkR的DataFrame API已经比较完善，支持的创建DataFrame的方式有：

从R原生data.frame和list创建
从SparkR RDD创建
从特定的数据源(JSON和Parquet格式的文件)创建
从通用的数据源创建
将指定位置的数据源保存为外部SQL表，并返回相应的DataFrame
从Spark SQL表创建
从一个SQL查询的结果创建
支持的主要的DataFrame操作有：

·数据缓存，持久化控制：cache(),persist(),unpersist()

数据保存：saveAsParquetFile(), saveDF() （将DataFrame的内容保存到一个数据源），saveAsTable() （将DataFrame的内容保存存为数据源的一张表）
集合运算：unionAll()，intersect(), except()
Join操作：join()，支持inner、full outer、left/right outer和semi join。
数据过滤：filter(), where()
排序：sortDF(), orderBy()
列操作：增加列- withColumn()，列名更改- withColumnRenamed()，选择若干列 -select()、selectExpr()。为了更符合R用户的习惯，SparkR还支持用$、[]、[[]]操作符选择列，可以用$<列名> <- 的语法来增加、修改和删除列
RDD map类操作：lapply()/map()，flatMap()，lapplyPartition()/mapPartitions()，foreach()，foreachPartition()
数据聚合：groupBy()，agg()
转换为RDD：toRDD()，toJSON()
转换为表：registerTempTable(),insertInto()
取部分数据：limit()，take()，first()，head()


## 5 竞品分析
### 5.1 支持单机并行计算的R包
- snow
- dopar
- forach
- supR


![Microsoft R Server](https://i.loli.net/2017/08/05/5985b2581fd23.png)

![sparklyr](https://i.loli.net/2017/08/05/5985a9cc02d28.png)
[sparklyr官方文档](https://spark.rstudio.com/index.html)
安装
[sparklyr+Docker](https://zhuanlan.zhihu.com/p/21574497)
docker安装
集群安装
3A（AWS/ALIYUN/Azure）云上安装
[AWS SparkR安装简介]（https://aws.amazon.com/cn/blogs/big-data/crunching-statistics-at-scale-with-sparkr-on-amazon-emr/）
[Supercharge R with Spark: Getting Apache's SparkR Up and Running on AWS](https://www.youtube.com/watch?v=ISsnKm2mAx4)




本文介绍 Azure HDInsight，Hadoop 技术堆栈的云发行版， 同时还介绍什么是 Hadoop 群集，以及何时使用该群集。Azure HDInsight 是 Hortonworks Data Platform (HDP) 提供的 Hadoop 组件的云发行版
Azure/Azure-MachineLearning-DataScience
[mrs](https://github.com/Azure/Azure-MachineLearning-DataScience/tree/master/Misc/KDDCup2016/Code/MRS)

[supR]()
[sparklyr]()
![sparklyr](https://i.loli.net/2017/08/05/5985a416a40a4.png)


![inrel BigDL](https://i.loli.net/2017/08/05/59859bfecf262.png)
![IntelbigDL](https://ooo.0o0.ooo/2017/08/05/5985ae823a248.png)


9.  基于Apache Spark扩展 H2O机器学习的不同策略
![sparkling water](https://i.loli.net/2017/08/05/5985a1ff48b4b.png)
主讲人：Jakub Hava
时长：32分12秒
链接：
https://www.youtube.com/embed/CGQAOL_M5c4?feature=oembed&width=500&height=750

H2O正因处理大数据而走红。视频中，Jakub讲述基于 H2O 和 Spark的机器学习基本内容。他阐释了如何用不同方法来拓展任务，这些方法包括 Spark数据修改、H2O 模型建立、兼用两者做数据修改和模型建立。

Sparkling Water整合 H2O和 Apache Spark的功能也允许我们借由Apache Spark应用，联合 Scala、Python、R或 H2O 的流式图形用户界面，来最大化利用H2O 机器学习算法，让 Sparkling Water 成为一种优异的企业解决方案。





视频简要介绍了 Sparkling Water的基础建设，简述不同的拓展策略并解释每个解决方案的优缺点。以一个样本视频阐明方案为结束，提供了实践经验，有助于你结合自身的情况配置并运用Sparkling Water。




[SystemML，IBM的开源人工智能系统，提供R接口](http://systemml.apache.org/)
![](https://i.loli.net/2017/08/05/5985b9a281783.png)
[renjin，直接用R连接JVM直接进行计算](http://www.renjin.org/)
![renjin](https://i.loli.net/2017/08/05/5985b96031783.png)


• Simplify/Unify Data Pipelines (SparkSQL)
• Mix Spark/Scala and R!
• R Graphics: raster images to data frame
• Performance Improvement: use MLlib
• Performance Improvement: move calculation to GPU








## 8 参考资料

[1] 教学资料（有实践环节）：
- 2016 The R User Conference（Stanford University）：**Shivaram Venkataraman - UC Berkeley; Hossein Falaki - Databricks**，[Introduction to SparkR](https://databricks.com/blog/2016/07/07/sparkr-tutorial-at-user-2016.html)
![](https://i.loli.net/2017/08/05/5985d8f695f19.png)

[2] 演讲资料(我有视频，你有梯子么？)：
- 2017 Strata Data Conference (beijing): **Xiaoyong Zhu - Microsoft**，
[使用R和Apache Spark处理大规模数据.](https://strata.oreilly.com.cn/strata-cn/public/schedule/detail/59480)
- 2016 第九届R语言会议(北京): **孙锐 - Intel**，
[SparkR的最新进展和趋势](https://cosx.org/2016/06/9th-china-r-beijing-summary)
- 2016 Spark Summit EU 经验分享：将SparkR用于生产环境下的数据科学应用中
- 2017 Spark Summit East R与Spark：如何使用RStudio的Sparklyr和H2O的Rsparkling分析数据
- 2017 Spark Summit East 在生产环境中的大规模数据应用中使用SparkR
- 2017 Spark Summit East 基于SparkR的可伸缩数据科学
- 2017 Spark Summit East BigDL：Spark上的分布式深度学习库

[3] 演讲资料(我有视频，你有梯子么？)
