## #基于时间衰减的用户画像切片聚合

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

另外，脚本执行时会在log日志中写入脚本的开始`start at date`和结束时间`end at date`。
