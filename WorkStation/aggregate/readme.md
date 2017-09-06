## 1. 基于时间衰减的用户画像切片聚合

* * *

#### 输入数据：

DW_RECSYS_ USER_RELATION_DAILY（每日聚合数据 用户-标签 关联关系）
DW_RECSYS_ USER_RELATION_N_DAYS（N日聚合数据 用户-标签 关联关系）

#### 过程介绍：

本模块主要按照不同的时间窗口的聚合用户偏好天表，并在聚合过程中考虑时间衰减。
实现过程：
使用sparkSQL中的UDF函数完成衰减过程权重的计算。
输入参数为聚合的时间窗口（例如30天、60天etc.）
衰减过程使用的函数为可配置项(优先完成线性的衰减)。

用sql实现计算，执行脚本时需要输入以下变量：

| INDEX           | 变量名    | 解释                    | 默认值 |
| --------------- | ------ | --------------------- | --- |
| $time_window    | 时间窗口长度 | 计算日期区间长度，默认从前一天往前计算   | 1   |
| $function_model | 函数类型   | 支持三种函数，线性1 sigma2 指数3 | 1   |
| $function_para1 | 函数参数1  | 主要控制曲线的变化速率           | 1   |
| $function_para2 | 函数参数2  | 曲线修正参数，保证定义域和值域       | 1   |

线性函数（当前 a=1 b=1）
<img src="http://chart.googleapis.com/chart?cht=tx&chl= y=-x+1" style="border:none;">

sigma函数（当前 a=4 b=1）
<img src="http://chart.googleapis.com/chart?cht=tx&chl= y=\frac{1}{10.25\cdot e^{4\left( 2x-1 \right)}}" style="border:none;">
![](https://ooo.0o0.ooo/2017/08/28/59a3b0d534d10.png)

幂指数函数（当前 a=2 b=4）
<img src="http://chart.googleapis.com/chart?cht=tx&chl= y=\frac{1}{2}^{4x}" style="border:none;">
![](https://ooo.0o0.ooo/2017/08/28/59a3b1dbb9a35.png)

#### 输出数据：

DW_RECSYS_ USER_RELATION_APPLIED_BAT（批量数据 用户-标签 关联关系）
若未特殊说明，本表中的默认聚合的时间窗口为60天。

同时，会由当天数据的快照得到：
DW_RECSYS_ USER_RELATION_APPLIED_RT（实时数据 用户-标签 关联关系）

另外，脚本执行时会在log日志中写入脚本的开始`start at date`和结束时间`end at date`。

## 2. 用户偏好标签和推荐列表内容标签相似度计算

* * *

1.  1） 前置工作1
    被推荐内容列表（存储在ES里）
    2）	前置工作2
    DW_RECSYS_ USER_RELATION_APPLIED_BAT（批量数据 用户-标签 关联关系）
    DW_RECSYS_ USER_RELATION_APPLIED_RT（实时数据 用户-标签 关联关系）、
2.  本模块过程介绍：
    本模块主要计算用户标签与被推荐内容列表之间的相似度计算。
    实现过程：
    方案一：
    编写相似度计算的函数，输入为用户的偏好向量和推荐内容的标签向量，输出为其相似度得分。
    此方案在用户访问内容的接口中实现，可能产生较长时间延时。
    方案二
    直接把相似度计算写在封装好的接口中，定期（高频刷新）访问推荐列表中的数据，计算得到排序结果表，并保存供其它接口访问。
    此方案需要判断文章是否重复、更新文章内容。同时为非实时计算。
    以上两种方案的最终选择，需要测试结果的支持。
    相似度计算中，同一用户的相似度得分是必须有序的，相似度越高，说明用户对于该内容越感兴趣。
    相似度计算可使用多种算法，当前版本下优先实现余弦相似度的计算。
