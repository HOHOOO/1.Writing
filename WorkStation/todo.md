1.基于时间衰减的用户画像切片聚合 2.在线相似度计算

输入是一个函数类型，输出的是一种模式

线性函数 $$wight(t)=a\*(t-t_0)+b$$
sigma型函数 $$wight(t)=\\frac{1}{1+a{e^{-\\frac{t-t_0}{a}}}}$$ ![](https://i.loli.net/2017/08/25/599f9ab651d49.png)

指数型函数 $$wight(t)=a^{t-t_0}$$ ![](https://i.loli.net/2017/08/25/599f9bfb4bf68.png)

$y=-x+1$
a=-1 b=1

$y=\\frac{1}{1+0.25\\cdot e^{4\\left( 2x-1 \\right)}}$
a=4 b=1

$y=\\frac{1}{2}^{4x}$
a=2 b=4

![](https://ooo.0o0.ooo/2017/08/28/59a3b1dbb9a35.png)

选择x∈[0,2]区间范围内的函数进行计算。
特点：
