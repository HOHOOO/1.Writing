---
layout: post
title: "uber 定价模型"
category: 推荐系统
tags: [推荐系统]
published: false
---

##

-   测试相对中立（统计、收益率 归因 统计显著）
-   效果的实时监控，值班运营中的 ticket
-   优惠券的投放

Q:产品的角色？

1.  控制项目进度；
2.  模型的细节？

Q:优化算法

1.  动态规划
2.  整数规划
3.  非线性优化

Q:知识库

1.  代码测试uni-test、
2.  文档标准化
3.  知识沉淀、留存

add_boxplot(y = top3[1:1000,1], name = "top3", boxpoints = 'top3')

p \<- plot_ly(type = 'box')%>%
    add_boxplot(y = subset(top1[1:1000,1]\<1), name = "top1", boxpoints = 'top1') %>% layout(title='123')%>%
add_boxplot(y = top2[1:1000,1], name = "top2", boxpoints = 'top2')%>%
add_boxplot(y = top3[1:1000,1], name = "top3", boxpoints = 'top3')

GMV数据  能关联到 url→百科→sku 实时性较差 交付的问题？脱敏  跟踪用户 id>30%  跟踪到设备 id>40%
应用：   购买后降权、商城偏好、品类购买偏好（亚马逊比较例外）

直达连接   累积值（30min 时间窗口取状态值）

pv    累积值

交互走接口

经验分享： 质量度模型  文章质量？ 值分

点值

40%商家爆料，商家在刷？商家普遍差
编辑自荐  产生 gmv 最高，

用户爆料 3 倍于商家爆料

 数据(全平台)

搜索
PV（详情页的 screen）/电商点击
