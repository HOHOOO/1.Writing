select user_id,count(1) as user_name from user_dislike_content group by user_id order by user_name desc limit 5;
user_id user_name
        887
3469991844      203
5284975700      199
9443248028      193
8819987613      175


林艳的用户ID
select * from user_dislike_content where user_id='9443248028'

20      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7288228 童车童床            8.0      2017-05-23 18:03:40.0   1
21      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7291440 泳衣                8.0      2017-05-23 18:03:53.0   1
22      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      5       7291765                 一键海淘    8.0     2017-05-23 18:04:08.0   1
23      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7291426 妈咪包              8.0      2017-05-23 18:04:16.0   1
24      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      5       7291847                     8.0      2017-05-23 18:04:29.0   1
25      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7291449         iRobot      8.0      2017-05-23 18:04:57.0   1
26      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      5       7291445 心率表              8.0      2017-05-23 18:05:09.0   1
27      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      5       7291750                     8.0      2017-05-23 18:05:18.0   1
28      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7291751 床垫                8.0      2017-05-23 18:30:31.0   1
29      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      5       7291792 微单镜头            8.0      2017-05-23 18:30:35.0   1
30      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7291887 安全座椅            8.0      2017-05-23 18:30:39.0   1
31      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      2       7290775 普通暖奶器          8.0      2017-05-23 18:31:11.0   1
32      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7291040 轮胎                8.0      2017-05-23 18:31:22.0   1
33      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7289384                     8.0      2017-05-23 18:31:29.0   1
34      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7290648 婴幼儿DHA           8.0      2017-05-23 18:31:34.0   1
35      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7290931 室内健身玩具        8.0      2017-05-23 18:31:57.0   1
36      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7291562 早教启智            8.0      2017-05-23 18:32:04.0   1
37      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7291964 学步车              8.0      2017-05-23 18:32:12.0   1
40      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7291478 立柜式空调          8.0      2017-05-23 18:33:06.0   1
41      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7288230 绘画工具            8.0      2017-05-23 18:43:33.0   1
42      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7291208 炒锅                8.0      2017-05-23 18:44:33.0   1
43      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7291405 婴儿玩具            8.0      2017-05-23 18:44:42.0   1
44      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      2       7289498 早教启智            8.0      2017-05-23 18:45:19.0   1
64      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7296702 洗碗机              8.0      2017-05-24 20:03:20.0   1
80      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7302573 汽车贴膜            8.0      2017-05-26 14:27:53.0   1
81      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7304990 显卡                8.0      2017-05-26 14:27:58.0   1
82      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7304512 显卡                8.0      2017-05-26 14:28:28.0   1
91      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7306621 冰箱                8.0      2017-05-26 21:02:27.0   1
92      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      11      563281  其它电脑外设        8.0      2017-05-26 21:03:19.0   1
93      W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      6       31800                       8.0      2017-05-26 21:03:32.0   1
115     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      11      493760  汽车整车            8.0      2017-06-01 15:46:03.0   1
132     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7329620 显卡                8.0      2017-06-01 16:14:10.0   1
162     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7325035                     8.0      2017-06-01 18:33:49.0   1
163     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7324397                     8.0      2017-06-01 18:34:04.0   1
178     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7334440 车载吸尘器          8.0      2017-06-02 16:53:08.0   1
188     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7332645 安全座椅            8.0      2017-06-02 17:27:18.0   1
189     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7334593 安全座椅            8.0      2017-06-02 17:27:23.0   1
190     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      5       7334006                     8.0      2017-06-02 17:27:42.0   1
191     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7334449 床垫                8.0      2017-06-02 17:28:00.0   1
192     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7334015 机油                8.0      2017-06-02 17:28:41.0   1
193     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      5       7335250 男款机械表          8.0      2017-06-02 17:29:03.0   1
194     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      5       7315908 母婴用品            8.0      2017-06-02 17:29:08.0   1
198     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7335741 机油                8.0      2017-06-02 17:31:40.0   1
199     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7335458 卫浴用品            8.0      2017-06-02 17:31:54.0   1
200     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7332270 鼠标                8.0      2017-06-02 17:38:34.0   1
201     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7332130 鼠标                8.0      2017-06-02 17:53:13.0   1
202     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7334797 电动吸奶器          8.0      2017-06-02 17:53:25.0   1
203     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7333252 波轮洗衣机          8.0      2017-06-02 17:53:35.0   1
204     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7332783 门厅家具            8.0      2017-06-02 17:53:44.0   1
206     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7320245 国外跟团目的地-肯尼亚   8.0     2017-06-03 10:52:40.0   1
207     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7336747 双门冰箱            8.0      2017-06-03 15:10:13.0   1
208     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7336658 滚筒洗衣机          8.0      2017-06-03 15:10:17.0   1
209     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7336620 咖啡具              8.0      2017-06-03 15:10:26.0   1
210     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7339319 燃气热水器          8.0      2017-06-03 15:43:16.0   1
211     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7337858 路由器              8.0      2017-06-03 15:43:20.0   1
212     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7338789 豆浆机              8.0      2017-06-03 15:43:28.0   1
220     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7343579 机油                8.0      2017-06-05 07:46:41.0   1
221     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7343820 马桶洁具            8.0      2017-06-05 07:46:53.0   1
222     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7342539 花洒                8.0      2017-06-05 07:47:00.0   1
223     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7343646 车用润滑油          8.0      2017-06-05 07:47:10.0   1
224     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7343842 机箱                8.0      2017-06-05 08:29:24.0   1
225     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7343773 散热器              8.0      2017-06-05 08:29:30.0   1
226     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7343809 机箱                8.0      2017-06-05 08:29:38.0   1
227     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7343798 移动硬盘            8.0      2017-06-05 08:29:54.0   1
228     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7355840 对开门冰箱          8.0      2017-06-07 07:42:00.0   1
229     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7355831 燃气热水器          8.0      2017-06-07 07:42:04.0   1
230     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7355755 电饭煲              8.0      2017-06-07 07:42:49.0   1
231     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7355841 滚筒洗衣机          8.0      2017-06-07 07:42:53.0   1
232     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7355612 全自动咖啡机        8.0      2017-06-07 07:43:38.0   1
233     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7355738 滚筒洗衣机          8.0      2017-06-07 07:43:48.0   1
234     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7355586 燃气热水器          8.0      2017-06-07 07:43:55.0   1
235     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7355597 多门冰箱            8.0      2017-06-07 07:44:00.0   1
236     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7355690 滚筒洗衣机          8.0      2017-06-07 07:44:04.0   1
237     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7355616 立柜式空调          8.0      2017-06-07 07:44:10.0   1
238     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7355427 乐高                8.0      2017-06-07 07:44:19.0   1
239     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7355699 半自动咖啡机        8.0      2017-06-07 07:44:23.0   1
240     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7355625 吸油烟机            8.0      2017-06-07 07:44:25.0   1
241     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7355649 洗碗机              8.0      2017-06-07 07:46:49.0   1
242     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7355669 滚筒洗衣机          8.0      2017-06-07 07:47:02.0   1
243     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7355646 全自动咖啡机        8.0      2017-06-07 07:47:06.0   1
244     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7354581 壁挂式空调          8.0      2017-06-07 07:47:15.0   1
245     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7355392 3段奶粉             8.0      2017-06-07 07:49:06.0   1
246     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7355858 电动剃须刀          8.0      2017-06-07 08:25:02.0   1
248     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7360927 对开门冰箱          8.0      2017-06-07 23:04:54.0   1
249     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7356958 母婴用品            8.0      2017-06-07 23:05:19.0   1
250     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      11      566285  鼠标                8.0      2017-06-07 23:05:44.0   1
251     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7355319 安卓手机            8.0      2017-06-07 23:06:05.0   1
252     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      6       31967                       8.0      2017-06-07 23:06:21.0   1
253     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7360627 沙发                8.0      2017-06-07 23:07:14.0   1
254     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7360729 保温杯              8.0      2017-06-07 23:07:25.0   1
255     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7354848 固态硬盘            8.0      2017-06-07 23:07:31.0   1
259     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7361478 洗烘一体机          8.0      2017-06-08 09:38:22.0   1
260     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7361112 猫砂                8.0      2017-06-08 09:38:29.0   1
261     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7361568 主板                8.0      2017-06-08 09:38:40.0   1
262     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7361810 燃气热水器          8.0      2017-06-08 09:38:58.0   1
263     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7361617 母婴用品            8.0      2017-06-08 09:39:15.0   1
264     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7362216 防溢乳垫            8.0      2017-06-08 09:40:07.0   1
315     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7415855 电吹风              8.0.1    2017-06-17 22:16:45.0   1
316     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7401981 对开门冰箱          8.0.1    2017-06-17 22:16:52.0   1
317     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7415922 吸顶灯              8.0.1    2017-06-17 22:17:03.0   1
332     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7437106 车用润滑油          8.0.1    2017-06-20 16:25:24.0   1
334     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7441259 洁身器              8.0.1    2017-06-21 14:16:08.0   1
362     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7451195 卧室家具            8.0.1    2017-06-23 22:37:04.0   1
363     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7451110 冰箱                8.0.1    2017-06-23 22:37:35.0   1
364     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7451089 洗衣机              8.0.1    2017-06-23 22:37:40.0   1
365     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7449308 卧室家具            8.0.1    2017-06-23 22:40:25.0   1
366     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7450970 车用脚垫            8.0.1    2017-06-23 22:40:41.0   1
370     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7451640 安全座椅            8.0.1    2017-06-24 10:06:43.0   1
371     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7451573         Midea/美的  8.0.1    2017-06-24 10:07:10.0   1
372     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7451625 机油                8.0.1    2017-06-24 10:07:22.0   1
756     W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7490433 卫浴用品            8.0.1    2017-07-05 17:25:53.0   1
1365    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7520311 卫浴用品            8.0.2    2017-07-12 13:13:11.0   1
1366    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7520357 卫浴用品            8.0.2    2017-07-12 13:13:14.0   1
1367    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7524018 卧室家具            8.0.2    2017-07-12 13:13:23.0   1
1738    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7540169 宠物主粮爱宠用品,宠物囤货       8.1.0.9 2017-07-17 14:01:40.0   1
1739    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7541817 宠物主粮爱宠用品,宠物囤货       8.1.0.9 2017-07-17 14:01:45.0   1
1740    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7541571 冰箱                8.1.0.9  2017-07-17 14:01:58.0   1
1741    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7540517 洗碗机              8.1.0.9  2017-07-17 14:02:05.0   1
1742    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7541884                     8.1.0.9  2017-07-17 14:02:10.0   1
1743    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7542503 童车童床            8.1.0.9  2017-07-17 14:02:17.0   1
1744    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7542370 宠物营养爱宠用品,宠物囤货       8.1.0.9 2017-07-17 14:02:25.0   1
1745    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7540166 宠物主粮爱宠用品,宠物囤货       8.1.0.9 2017-07-17 14:02:29.0   1
1746    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7542105 宠物主粮爱宠用品,宠物囤货       8.1.0.9 2017-07-17 14:02:33.0   1
1747    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7540162 宠物主粮宠物囤货,爱宠用品       8.1.0.9 2017-07-17 14:02:36.0   1
1858    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7544793 键鼠                8.1.0.9  2017-07-18 08:50:44.0   1
1859    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7544675 键鼠                8.1.0.9  2017-07-18 08:50:48.0   1
1860    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7545191 笔记本电脑          8.1.0.9  2017-07-18 08:51:00.0   1
1861    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7544875 键鼠                8.1.0.9  2017-07-18 08:51:03.0   1
1862    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7545478 台式机              8.1.0.9  2017-07-18 08:51:08.0   1
1863    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7545306 笔记本电脑          8.1.0.9  2017-07-18 08:51:12.0   1
1864    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7544349 主板                8.1.0.9  2017-07-18 08:51:15.0   1
1865    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7544820 硬盘                8.1.0.9  2017-07-18 08:51:22.0   1
1866    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7544809 散热器              8.1.0.9  2017-07-18 08:51:37.0   1
4094    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7622248 耳机                8.0.2    2017-08-06 16:54:01.0   1
4095    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7622417 空调                8.0.2    2017-08-06 16:54:04.0   1
4096    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7622016 吸尘器              8.0.2    2017-08-06 16:54:09.0   1
4186    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7623681         CHEERS/芝华 8.0.2    2017-08-07 08:32:52.0   1
4187    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7623683         SLEEMON/喜临门              8.0.2   2017-08-07 08:33:01.0   1
4188    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7624406 洗浴用品            8.0.2    2017-08-07 08:33:14.0   1
4341    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7627049 卫浴用品            8.0.2    2017-08-07 22:35:42.0   1
4346    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7628902 卫浴用品            8.0.2    2017-08-07 22:53:42.0   1
4347    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7628587 洗碗机              8.0.2    2017-08-07 22:53:48.0   1
4380    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7628813 卫浴用品            8.0.2    2017-08-08 08:36:29.0   1
4381    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7628526 卫浴用品            8.0.2    2017-08-08 08:36:33.0   1
4382    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7628334 冰箱                8.0.2    2017-08-08 08:36:43.0   1
4383    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7628274 冰箱                8.0.2    2017-08-08 08:36:46.0   1
4384    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7607084 电视                8.0.2    2017-08-08 08:38:08.0   1
4385    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7616579 美发小家电          8.0.2    2017-08-08 08:38:15.0   1
4386    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7628703 热水器              8.0.2    2017-08-08 08:38:20.0   1
4387    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7628412 热水器              8.0.2    2017-08-08 08:38:29.0   1
4388    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7628393 热水器              8.0.2    2017-08-08 08:38:37.0   1
4488    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7634770 电视                8.0.2    2017-08-09 08:30:10.0   1
4489    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7629835 吸顶灯              8.0.2    2017-08-09 08:31:17.0   1
4490    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7634624 卸妆产品            8.0.2    2017-08-09 08:31:33.0   1
4491    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7629861 吸顶灯              8.0.2    2017-08-09 08:31:42.0   1
4492    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7635062 相机                8.0.2    2017-08-09 08:32:27.0   1
4493    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7634272 智能手机            8.0.2    2017-08-09 08:33:59.0   1
4495    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7634144 积木拼插            8.0.2    2017-08-09 08:44:33.0   1
4496    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7634120 积木拼插            8.0.2    2017-08-09 08:44:36.0   1
4497    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7634029 卡通周边            8.0.2    2017-08-09 08:44:40.0   1
4498    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7634199 婴儿尿裤            8.0.2    2017-08-09 08:44:43.0   1
4499    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7634048 积木拼插            8.0.2    2017-08-09 08:44:49.0   1
4500    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7633816 电动玩具            8.0.2    2017-08-09 08:44:52.0   1
4511    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7633911 洗衣机              8.0.2    2017-08-09 09:09:44.0   1
4512    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7633954 豆浆机              8.0.2    2017-08-09 09:09:47.0   1
4513    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7634658 净水设备            8.0.2    2017-08-09 09:10:15.0   1
4514    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7634543 冰箱                8.0.2    2017-08-09 09:10:33.0   1
4515    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7633284 卫浴用品            8.0.2    2017-08-09 09:10:47.0   1
4691    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7635817 洗衣机              8.0.2    2017-08-10 08:58:50.0   1
4692    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7638744 洗衣机              8.0.2    2017-08-10 08:58:53.0   1
4693    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7638116 洗衣机              8.0.2    2017-08-10 08:58:57.0   1
4694    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7638720 冰箱                8.0.2    2017-08-10 08:59:12.0   1
4695    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7637863 冰箱                8.0.2    2017-08-10 08:59:15.0   1
4696    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7637851 空调                8.0.2    2017-08-10 08:59:17.0   1
4697    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7638793 咖啡机              8.0.2    2017-08-10 08:59:22.0   1
4698    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7637026                     8.0.2    2017-08-10 08:59:29.0   1
4699    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7635919 冰箱                8.0.2    2017-08-10 08:59:48.0   1
4700    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7639441 冰箱                8.0.2    2017-08-10 09:00:23.0   1
4701    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7639452 冰箱                8.0.2    2017-08-10 09:00:30.0   1
5177    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7651218 婴儿尿裤            8.0.2    2017-08-13 14:56:07.0   1
5178    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7650506 婴儿尿裤            8.0.2    2017-08-13 14:56:11.0   1
5312    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7653434 美发小家电          8.0.2    2017-08-14 08:41:59.0   1
5313    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7652929 主板                8.0.2    2017-08-14 08:42:18.0   1
5314    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7653458 电饭煲              8.0.2    2017-08-14 08:42:26.0   1
5315    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      5       7652809 剃须除毛            8.0.2    2017-08-14 08:42:40.0   1
5316    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7652922 相机                8.0.2    2017-08-14 08:42:44.0   1
5317    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7653298 童车童床            8.0.2    2017-08-14 08:42:50.0   1
5318    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7652886 亲子装              8.0.2    2017-08-14 08:42:54.0   1
5319    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7652853 相机                8.0.2    2017-08-14 08:43:02.0   1
5320    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7653411 卫浴用品            8.0.2    2017-08-14 08:43:13.0   1
5321    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7652758                     8.0.2    2017-08-14 08:45:02.0   1
5322    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7652758                     8.0.2    2017-08-14 08:45:02.0   1
5323    W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==        9443248028      1       7652758                     8.0.2    2017-08-14 08:45:02.0   1

select b.tag_id,b.tag_standar,b.tag_standar_new,a.name from smzdm_tag_type a right join user_youhui_dislike_tag b  on a.id=b.tag_id where b.user_id='9443248028' and b.ds='20170813';
415884  0.2     0.2     日用品囤货
405421  0.0333  0.0333  2016双11
304607  0.0333  0.0333  蒙台梭利
422281  0.0167  0.0167  母婴0807
405505  0.0167  0.0167  2016双11预售
404254  0.0167  0.0167  爱看书
416099  0.0667  0.0667  时尚种草记
54989   0.0333  0.0333  手慢无
421137  0.0167  0.0167  全球PrimeDay母婴日百
15135   0.2     0.2     直邮
421138  0.0833  0.0833  全球PrimeDay服饰运动
415898  0.1167  0.1167  个护囤货
64221   0.05    0.05    白菜汇总
416734  0.0333  0.0333  新品发售
406729  0.0167  0.0167  淘值选
416728  0.25    0.25    限尺码
415900  0.0333  0.0333  宠物囤货
421597  0.0333  0.0333  718耳机
223871  0.7     0.7     贝窝
416516  0.1167  0.1167  养车囤货
418910  0.0333  0.0333  2017618日百母婴
416763  0.0667  0.0667  生活种草记
421136  0.0833  0.0833  全球PrimeDay3c家电
418264  0.9     0.9     国内凑单品
149611  0.0167  0.0167  成人向
418380  0.6667  0.6667  拼单
2289    0.05    0.05    免费得
387     0.0333  0.0333  优惠券码
9149    0.0167  0.0167  奇葩物
422459  0.0833  0.0833  中亚店庆母婴日百
422284  0.05    0.05    807欧洲厨卫
380411  0.0167  0.0167  白菜凑单
322277  0.1833  0.1833  日本馆
405573  0.0167  0.0167  2016双11预售食品家居
404249  0.0333  0.0     爱宠用品
418909  0.0333  0.0333  2017618食品家居
9239    0.0333  0.0333  高端秀
422457  0.0167  0.0167  中亚店庆食品家居
261839  0.1667  0.1667  值友专享
60753   0.05    0.05    白菜包邮
39467   0.8333  0.8333  历史新低
337871  0.0333  0.0333  临期品
60751   0.2667  0.2667  天猫白菜
404253  1.0     1.0     运动派
416767  0.0333  0.0333  亲子教育
322337  0.3     0.3     每日白菜
421575  0.0167  0.0167  手绘插画
388605  0.0333  0.0333  新西兰馆
406493  0.8     0.8     中亚Prime会员
6175    0.1333  0.1333  促销活动
381538  0.1     0.1     移动专享
14995   0.2     0.2     历史低价
414544  0.0667  0.0667  种草记
71049   0.0167  0.0167  限华北
412221  0.0667  0.0667  反季特卖
397801  0.7     0.034999999999999996    一键海淘
35253   0.0667  0.0667  白菜党
418018  0.0833  0.0833  淘金v计划
403137  0.0333  0.0333  19块9包邮
404249  0.0333  0.0333  爱宠用品
64211   0.0833  0.0833  9块9包邮
415904  0.0167  0.0167  开春户外
12693   0.2333  0.2333  凑单品
421962  0.0333  0.0333  音频节目
392858  0.0167  0.0167  天猫618
391796  0.0167  0.0167  秒杀精选
149617  0.5167  0.5167  爱美丽
415900  0.0333  0.0     宠物囤货
418908  0.0167  0.0167  2017618数码3C
416513  0.1333  0.1333  京东PLUS会员
408332  0.0167  0.0167  2016天猫双11
415903  0.3167  0.3167  零食囤货
422458  0.1     0.1     中亚店庆3c家电
104979  0.0167  0.0167  绝对值
420876  0.1     0.1     全球PrimeDay
19855   0.0167  0.0167  神价格
405508  0.0167  0.0167  2016双11预售天猫预售
47323   0.0333  0.0333  新补货
59825   0.05    0.05    京东618
388447  0.0167  0.0167  英国馆
116853  0.35    0.35    七夕
388351  0.7833  0.7833  美国馆
420673  0.0167  0.0167  京东超市食品返场
399406  0.1667  0.1667  七夕礼物
421709  0.0667  0.0667  杉果夏促限时
397801  0.7     0.7     一键海淘
417286  0.0     0.0     目的地-肯尼亚



select b.brand_id,b.brand_level_standar,b.brand_level_standar_new,a.associate_title from sync_smzdm_brand a right join user_youhui_dislike_brand b  on a.id=b.brand_id where b.user_id='9443248028' and b.ds='20170813';
757     0.25    0.25    Onitsuka Tiger/鬼冢虎
247     0.25    0.25    Panasonic/松下
1847    0.25    0.25    EPSON/爱普生
33950   0.1667  0.1667  LENCIER/兰叙
41574   0.0833  0.0833  Violaine/菲欧兰
42304   0.0833  0.0833  甘顺
43624   0.0833  0.0833  citilink/协东盛
4213    0.0833  0.0833  BEKAHOS/百家好世
2253    0.0833  0.0833  HITACHI/日立
1673    0.0833  0.0833  HP/惠普
1983    0.0833  0.0833  kate spade NEW YORK
9113    0.0833  0.0833  KANSOON/凯速
1203    0.0833  0.0833  Calvin Klein/卡尔文·克莱
2203    0.0833  0.0833  PETER THOMAS ROTH/彼得罗夫
39343   0.0833  0.0833  The Wet Brush
2213    0.0833  0.0833  Aveeno
10063   0.0833  0.0833  BECBAS/贝克巴斯
31823   0.0833  0.0833  NITTAYA
32263   0.0833  0.0833  ANTIPODES
423     0.0833  0.0833  waterpik/洁碧
5903    0.0833  0.0833  超能
2763    0.0833  0.0833  GEOGEOCAFÉ/吉意欧
413     0.0833  0.0833  Donlim/东菱
803     0.0833  0.0833  THE NORTH FACE/北面
24573   0.0833  0.0833  DOREL
373     0.0833  0.0833  BLACK&DECKER/百得
5573    0.0833  0.0833  DEVON/大有
1729    0.1667  0.1667  SEAGATE/希捷
35809   0.1667  0.1667  VITALIFE/维纯
749     0.1667  0.1667  new balance
419     0.25    0.25    BRAUN/博朗
36916   0.0833  0.0833  DURAVIT
40456   0.0833  0.0833  苏鲜生
729     0.3333  0.3333  adidas/阿迪达斯
2131    0.1667  0.1667  Philosophy
451     0.1667  0.1667  ZWILLING/双立人
531     0.1667  0.1667  TIGER/虎牌
3211    0.1667  0.1667  China Mobile/中国移动
411     0.1667  0.1667  SUPOR/苏泊尔
365     0.0833  0.0833  FOTILE/方太
30295   0.0833  0.0833  ZUK
2105    0.0833  0.0833  OSPREY
2115    0.0833  0.0833  Rebecca Minkoff/瑞贝卡·明可弗
30555   0.0833  0.0833  DOSHISHA
925     0.0833  0.0833  LANCOME/兰蔻
3625    0.0833  0.0833  Loctek/乐歌
595     0.0833  0.0833  Johnson & Johnson/强生
2725    0.0833  0.0833  ThundeRobot/雷神
42735   0.0833  0.0833  奇晟铭源
1825    0.0833  0.0833  HUAWEI/华为
1395    0.0833  0.0833  Clarks
10255   0.0833  0.0833  WRIGLEY/箭牌
6485    0.0833  0.0833  finish/亮碟
10585   0.0833  0.0833  Monopoly
6625    0.0833  0.0833  NORTH BAYOU
25395   0.0833  0.0833  PDC
2545    0.0833  0.0833  ASD/爱仕达
2165    0.0833  0.0833  Brooks/布鲁克斯
2015    0.0833  0.0833  GNC/健安喜
19425   0.0833  0.0833  鲜动生活
823     0.1667  0.1667  Marmot/土拨鼠
11683   0.1667  0.1667  Laurier/乐而雅
273     0.1667  0.1667  PHILIPS/飞利浦
19943   0.1667  0.1667  Silit
32777   0.0833  0.0833  ANGLEPOISE
1047    0.0833  0.0833  Oral-B/欧乐-B
41117   0.0833  0.0833  aogula/奥古拉家具
2907    0.0833  0.0833  达利园
5797    0.0833  0.0833  Citylong/禧天龙
31847   0.0833  0.0833  DR.WU/达尔肤
28387   0.0833  0.0833  KAMILIANT
2087    0.0833  0.0833  Oakley/欧克利
1727    0.0833  0.0833  WD/西部数据
40277   0.0833  0.0833  汪陂途
7317    0.0833  0.0833  MIKI HOUSE
6027    0.0833  0.0833  MUH/甘蒂牧场
17237   0.0833  0.0833  Vitamix
4577    0.0833  0.0833  TSINGTAO/青岛啤酒
30837   0.0833  0.0833  So Natural
4117    0.0833  0.0833  DAPU/大朴
28867   0.0833  0.0833  KINTO
2197    0.0833  0.0833  Kiehl科颜氏
14347   0.0833  0.0833  Ravensburger/睿思
2017    0.0833  0.0833  Timberland/添柏岚
1567    0.0833  0.0833  AKG/爱科技
41157   0.0833  0.0833  AODMA/澳得迈
4807    0.0833  0.0833  Grelide/格来德
23837   0.0833  0.0833  THE BEAST/野兽派
1687    0.1667  0.1667  Apple/苹果
28067   0.1667  0.1667  HONDO BEEF/恒都
257     0.1667  0.1667  SAMSUNG/三星
16747   0.1667  0.1667  Hansgrohe/汉斯格雅
5407    0.1667  0.1667  JOMOO/九牧
1237    0.1667  0.1667  TUMI/途明
627     0.1667  0.1667  LEGO/乐高
4217    0.1667  0.1667  OCEAN FAMILY/大洋世家
2331    0.0833  0.0833  Wrangler
21961   0.0833  0.0833  白色恋人
7331    0.0833  0.0833  Rachael Ray
2441    0.0833  0.0833  Tender Plus/天谱乐食
2821    0.0833  0.0833  HAN
42201   0.0833  0.0833  简单滋味
4191    0.0833  0.0833  BABYBJORN
26581   0.0833  0.0833  MYPROTEIN
25151   0.0833  0.0833  OSK
371     0.0833  0.0833  ROBAM/老板
901     0.0833  0.0833  SHISEIDO/资生堂
1461    0.0833  0.0833  Microsoft/微软
9181    0.0833  0.0833  农夫山泉
15641   0.0833  0.0833  COFCO/中粮
42301   0.0833  0.0833  Frosch/菲洛施
27991   0.0833  0.0833  KUHN RIKON/瑞士力康
23271   0.0833  0.0833  PetMaster
14701   0.0833  0.0833  STAUB
5841    0.0833  0.0833  Zespri/佳沛
1681    0.0833  0.0833  ASUS/华硕
12851   0.0833  0.0833  GENMU/根沐
18221   0.0833  0.0833  HIGHCOOK/韩库
12191   0.3333  0.3333  Lodge
453     0.5     0.5     WOLL
5395    1.0     1.0     WMF/福腾宝
41748   0.1667  0.1667  PEARL METAL
41715   0.25    0.25    易果生鲜
4845    0.3333  0.3333  Tefal/特福
287     0.0833  0.0833  Midea/美的
5707    0.0     0.0     CHEERS/芝华仕
2553    0.3333  0.3333  Devondale/德运
287     0.0833  0.004165        Midea/美的
1933    0.25    0.25    MI/小米
42180   0.0833  0.0833  今锦上
NULL    0.0833  0.0833  NULL
40260   0.0833  0.0833  PEO
34430   0.0833  0.0833  THUASNE/途安
403     0.4167  0.4167  Joyoung/九阳
1367    0.8333  0.8333  SKECHERS/斯凯奇
319     0.0833  0.0833  BOSCH/博世
4069    0.0833  0.0833  COOKER KING/炊大皇
37109   0.0833  0.0833  MIJIA/米家
13379   0.0833  0.0833  PURSONIC
36039   0.0833  0.0833  千禾味业
4249    0.0833  0.0833  ViTa/維他
19799   0.0833  0.0833  安至选
32999   0.0833  0.0833  EGO
2549    0.0833  0.0833  LUOLAI/罗莱
39699   0.0833  0.0833  Pierre Lannier/连尼亚
43839   0.0833  0.0833  榕力
16939   0.0833  0.0833  KitchenAid/凯膳怡
249     0.0833  0.0833  SONY/索尼
4689    0.0833  0.0833  ZOJIRUSHI/象印
18359   0.0833  0.0833  sukin
1999    0.0833  0.0833  BEARPAW
14589   0.0833  0.0833  德青源
1529    0.0833  0.0833  Kenko/肯高
17389   0.0833  0.0833  WERNER/稳耐
309     0.0833  0.0833  SIEMENS/西门子
799     0.0833  0.0833  Wilson/威尔胜
19739   0.0833  0.0833  umbra
459     0.0833  0.0833  PEARL LIFE/珍珠生活
4309    0.0833  0.0833  rikang/日康
765     0.1667  0.1667  ASICS/亚瑟士
1045    0.1667  0.1667  LION/狮王
28155   0.1667  0.1667  w.p.c
399     0.0     0.0     iRobot
16125   0.0     0.0     SLEEMON/喜临门


select b.level_id,b.level_standar,b.level_standar_new,a.level from level_map a right join user_youhui_dislike_level b  on a.level_id=b.level_id where b.user_id='9443248028' and b.ds='20170813';
859     0.25    0.25    禽蛋肉类
1245    0.1875  0.1875  雨伞雨具
929     0.0625  0.0625  婴幼儿洗发沐浴
3825    0.2857  0.2857  龙头
857     0.375   0.375   海鲜水产
3339    0.1429  0.1429  iPad
235     0.0625  0.0625  户外背包
1889    0.1429  0.1429  儿童浴盆
4537    0.4286  0.4286  套锅
1481    0.3125  0.3125  洁牙用具
5375    0.0476  0.0476  文化娱乐
1331    0.0625  0.0625  电子秤
4905    0.1429  0.1429  咖啡豆/粉
1287    0.0625  0.0625  榨汁机
4301    0.1429  0.1429  拖把
677     0.0625  0.0625  防晒隔离
3821    0.4286  0.02143 马桶洁具
2903    0.1429  0.1429  茶类饮料
339     0.0625  0.003125        路由器
1311    0.0625  0.0625  其他厨房电器
2345    0.1429  0.1429  电动理发器
1087    0.0625  0.0625  童鞋
1627    0.0625  0.0625  酒类
87      0.0625  0.0     童车童床
1321    0.0625  0.0625  美容器
2085    0.1429  0.1429  婴儿摇椅
4103    0.4286  0.4286  电钻
3961    0.0625  0.0625  钟
207     0.0625  0.0625  速干衣裤
4319    0.1429  0.1429  洗碗布
2869    0.1429  0.1429  贝类
4525    1.0     1.0     压力锅
289     0.0625  0.0625  平板电脑
191     0.4286  0.4286  运动户外
4797    0.1429  0.1429  男性系列
4929    0.2857  0.014285        乐高
3813    0.1429  0.1429  浴室柜
3745    0.25    0.25    装修主材
3679    0.1429  0.1429  双肩背包
665     0.0625  0.0625  护肤精华
1589    0.5     0.5     休闲运动鞋
2867    0.1429  0.1429  虾类
579     0.1875  0.1875  男士钱包
1301    0.0625  0.0625  电烤箱
1261    0.0625  0.0     洗衣机
3851    0.0625  0.0625  客厅家具
95      0.5833  0.5833  食品保健
467     0.0625  0.0625  行车记录仪
1163    0.3125  0.3125  五金工具
283     0.0     0.0     笔记本电脑
313     0.0     0.0     散热器
1319    0.0     0.0     电吹风
1769    0.0     0.0     婴幼儿DHA
4619    0.0     0.0     咖啡具
527     0.0     0.0     安全座椅
4833    0.0     0.0     对开门冰箱
2299    0.0     0.0     半自动咖啡机
2439    0.0     0.0     绘画工具
1323    0.125   0.125   美发小家电
2073    0.0     0.0     早教启智
2873    0.1429  0.1429  蟹类
4143    0.1429  0.1429  卸妆乳
4125    0.2857  0.2857  梯子
1689    0.25    0.25    乳液面霜
743     0.0625  0.0625  情爱玩具
5287    0.1429  0.1429  男士短靴
3805    0.4375  0.0     卫浴用品
4517    0.5714  0.02857 炒锅
4521    0.1429  0.1429  煎锅
1297    0.0     0.0     咖啡机
3391    0.0     0.0     键鼠
989     0.1875  0.1875  积木拼插
1149    0.0625  0.0625  宠物主粮
437     0.0     0.0     移动硬盘
2297    0.0     0.0     全自动咖啡机
2407    0.0     0.0     心率表
3897    0.0     0.0     床垫
291     0.0625  0.0625  台式机
1831    0.0     0.0     婴儿尿裤
75      0.0476  0.0476  母婴用品
453     0.0     0.0     耳机
4929    0.2857  0.2857  乐高
927     0.0625  0.0625  洗浴用品
5173    0.0     0.0     电动玩具
4517    0.5714  0.5714  炒锅
5346    0.0     0.0     国外跟团
1153    0.0     0.0     宠物营养
1313    0.0     0.0     电动剃须刀
2453    0.0     0.0     卡通周边
2703    0.0     0.0     车用润滑油
1289    0.0     0.0     豆浆机
2425    0.0     0.0     室内健身玩具
1755    0.0     0.0     3段奶粉
1845    0.0     0.0     电动吸奶器
1291    0.5     0.5     电饭煲
2323    0.0     0.0     洗碗机
4835    0.0     0.0     多门冰箱
5145    0.0     0.0     洁身器
4529    0.1429  0.1429  蒸锅
2899    0.1429  0.1429  水
147     0.0476  0.0476  汽车消费
1737    0.1429  0.1429  唇彩唇蜜
2329    0.5714  0.5714  电动牙刷
107     0.125   0.125   饮料
1149    0.0625  0.0     宠物主粮
4015    0.1429  0.1429  台钟
853     0.1875  0.1875  新鲜水果
5311    0.1429  0.1429  徒步背包
1881    0.1429  0.1429  婴幼儿洗发水
1733    0.0625  0.0625  唇部彩妆
2887    0.4286  0.4286  牛羊肉
113     0.2857  0.2857  个护化妆
27      0.5595  0.5595  家用电器
2433    0.1429  0.1429  拼图
4953    0.1429  0.007145        安卓手机
1599    0.0625  0.0625  篮球
1049    0.0625  0.0625  男士休闲鞋
1179    1.0     1.0     烹饪锅具
5001    0.0625  0.0625  桌游
1515    1.0     1.0     日用百货
93      0.0476  0.0476  玩模乐器
3731    0.1429  0.1429  精华液
403     0.0     0.0     相机
1259    0.0     0.0     冰箱
4909    0.0     0.0     心率表
87      0.0625  0.0625  童车童床
1261    0.0625  0.0625  洗衣机
2109    0.0     0.0     立柜式空调
723     0.0625  0.0625  卸妆产品
1553    0.0     0.0     热水器
1219    0.0     0.0     吸顶灯
1429    0.0625  0.0625  吸尘器
2595    0.0     0.0     车载吸尘器
1085    0.0     0.0     亲子装
1275    0.0     0.0     净水设备
4953    0.1429  0.1429  安卓手机
3879    0.0     0.0     卧室家具
475     0.0     0.0     车用脚垫
2121    0.1429  0.1429  滚筒洗衣机
339     0.0625  0.0625  路由器
389     0.0625  0.0625  智能手机
2375    0.1429  0.1429  厨房秤
4673    0.4286  0.4286  雨伞
5491    0.1429  0.1429  食盐
4401    0.1429  0.1429  狗粮
927     0.0625  0.003125        洗浴用品
989     0.1875  0.0     积木拼插
131     0.0952  0.0952  礼品钟表
3639    0.0625  0.0625  太阳镜
2809    0.0625  0.0625  运动卫衣
1429    0.0625  0.003125        吸尘器
3945    0.0625  0.0625  男表
2911    0.1875  0.1875  冲调饮品
2191    0.1875  0.1875  车载空气净化器
1293    0.125   0.125   电压力锅
1323    0.125   0.0     美发小家电
1211    0.125   0.125   装饰台灯
1847    0.0     0.0     普通暖奶器
3805    0.4375  0.4375  卫浴用品
2107    0.0     0.0     壁挂式空调
2117    0.0     0.0     双门冰箱
3967    0.0     0.0     男款机械表
4837    0.0     0.0     固态硬盘
5067    0.0     0.0     泳衣
7       0.0119  0.0119  图书音像
1309    0.0625  0.0625  电水壶
5271    0.0625  0.0625  厨师机
291     0.0625  0.003125        台式机
1315    0.0625  0.003125        剃须除毛
177     0.0476  0.0476  办公设备
1143    0.125   0.125   清洁工具
1057    0.0625  0.0625  男靴
263     0.3125  0.3125  跑鞋
3451    0.1429  0.1429  无线路由
807     0.0625  0.0625  调味品
2121    0.1429  0.0     滚筒洗衣机
2171    0.1429  0.1429  手持式吸尘器
75      0.0476  0.0     母婴用品
163     0.5357  0.5357  电脑数码
723     0.0625  0.003125        卸妆产品
3921    0.0     0.0     门厅家具
601     0.0     0.0     妈咪包
1315    0.0625  0.0625  剃须除毛
1555    0.0     0.0     燃气热水器
301     0.0     0.0     硬盘
2131    0.0     0.0     吸油烟机
5421    0.0     0.0     洗烘一体机
489     0.0     0.0     机油
1255    0.0     0.0     电视
2041    0.0     0.0     防溢乳垫
4289    0.0625  0.0625  床上家纺
3963    0.1429  0.1429  男款石英表
389     0.0625  0.003125        智能手机
57      0.619   0.619   服饰鞋包
1325    0.0625  0.0625  按摩保健
1291    0.5     0.0     电饭煲
1675    0.1429  0.1429  休闲皮鞋
261     0.125   0.125   篮球鞋
37      0.3214  0.3214  家居家装
3677    0.0625  0.0625  休闲运动包
2331    0.1429  0.1429  冲牙器
2881    0.1429  0.1429  蛋类
3739    0.4286  0.4286  面霜
2067    0.0     0.0     婴儿玩具
4463    0.0     0.0     猫砂
4603    0.0     0.0     保温杯
1257    0.0     0.0     空调
3821    0.4286  0.4286  马桶洁具
1313    0.0     0.0     电动剃须刀
2123    0.0     0.0     波轮洗衣机
3823    0.0     0.0     花洒





9443248028      27:0.5595#0.7503850271073326,37:0.3214#0.4080686752394715,57:0.619#0.44474428639894603,75:0.0476#0.0,95:0.5833#0.45079913487463785,147:0.0476#0.41642906123845896,177:0.0476#0.27761937415897264,1515:1.0#1.0,5375:0.0476#0.27761937415897264,131:0.0952#0.6375330861944961,191:0.4286#0.8669494315345282,93:0.0476#0.41642906123845896,113:0.2857#0.5629003505765205,163:0.5357#0.5561838331623962|27:0.5595,37:0.3214,57:0.619,75:0.0476,95:0.5833,147:0.0476,177:0.0476,1515:1.0,5375:0.0476,131:0.0952,191:0.4286,93:0.0476,113:0.2857,163:0.5357|1149:0.0,1211:0.125,1257:0.0,1275:0.0,1293:0.125,1301:0.0625,2085:0.1429,283:0.0,2869:0.1429,2887:0.4286,3679:0.1429,3813:0.1429,3921:0.0,3967:0.0,5145:0.0,5271:0.0625,1143:0.125,1297:0.0,1323:0.0,1675:0.1429,2331:0.1429,2809:0.0625,2881:0.1429,313:0.0,3745:0.25,3961:0.0625,403:0.0,4401:0.1429,4537:0.4286,467:0.0625,5491:0.1429,601:0.0,665:0.0625,87:0.0,1245:0.1875,2073:0.0,2109:0.0,2299:0.0,235:0.0625,2433:0.1429,2703:0.0,3739:0.4286,389:0.003125,4125:0.2857,4143:0.1429,4521:0.1429,4837:0.0,4909:0.0,5287:0.1429,5421:0.0,677:0.0625,857:0.375,929:0.0625,1087:0.0625,1311:0.0625,1429:0.003125,1555:0.0,1627:0.0625,1889:0.1429,2121:0.0,2329:0.5714,301:0.0,3391:0.0,3805:0.0,3823:0.0,437:0.0,4525:1.0,5001:0.0625,5173:0.0,527:0.0,743:0.0625,1057:0.0625,1219:0.0,1255:0.0,1291:0.0,1309:0.0625,1589:0.5,1769:0.0,1831:0.0,2191:0.1875,2407:0.0,2425:0.0,263:0.3125,2867:0.1429,2911:0.1875,3451:0.1429,3677:0.0625,4289:0.0625,4603:0.0,489:0.0,579:0.1875,1179:1.0,1287:0.0625,1313:0.0,1331:0.0625,1737:0.1429,1755:0.0,1845:0.0,1881:0.1429,2123:0.0,2439:0.0,2899:0.1429,3339:0.1429,3825:0.2857,4103:0.4286,4301:0.1429,475:0.0,4833:0.0,4905:0.1429,5067:0.0,5346:0.0,853:0.1875,989:0.0,1259:0.0,1321:0.0625,2041:0.0,2131:0.0,2375:0.1429,339:0.003125,3851:0.0625,3879:0.0,3897:0.0,4319:0.1429,4463:0.0,4517:0.02857,4797:0.1429,807:0.0625,1049:0.0625,1085:0.0,1319:0.0,1481:0.3125,1553:0.0,1599:0.0625,1689:0.25,1733:0.0625,2345:0.1429,2453:0.0,2903:0.1429,291:0.003125,3731:0.1429,3821:0.02143,453:0.0,4929:0.014285,723:0.003125,859:0.25,1163:0.3125,1325:0.0625,207:0.0625,2117:0.0,2171:0.1429,2595:0.0,261:0.125,289:0.0625,3639:0.0625,3945:0.0625,3963:0.1429,4953:0.007145,107:0.125,1153:0.0,1261:0.0,1289:0.0,1315:0.003125,1847:0.0,2107:0.0,2297:0.0,2323:0.0,2873:0.1429,4015:0.1429,4529:0.1429,4619:0.0,4673:0.4286,4835:0.0,5311:0.1429,927:0.003125_1429:0.0,1555:0.0072,1573:0.0072,1915:0.0072,2121:0.0,301:0.0,3391:0.0,3805:0.0,3823:0.0,437:0.0,5173:0.0,527:0.0,75:0.0,1297:0.0,1323:0.0,241:1.0,313:0.0,403:0.0,4357:0.0072,4997:0.0072,5365:1.0,601:0.0,87:0.0,2073:0.0,2109:0.0,2299:0.0,2703:0.0,3801:0.0072,389:0.0,4521:0.0145,4837:0.0,4909:0.0,5421:0.0,1141:0.0072,1259:0.0,2041:0.0,2131:0.0,2285:0.0072,339:0.0,3879:0.0,3897:0.0,4463:0.0,4517:0.0,5273:0.0072,933:0.0072,1149:0.0,1257:0.0072,1275:0.0,2067:0.0,283:0.0,3787:0.0072,3921:0.0,3967:0.0,4669:0.0072,5145:0.0,5343:0.0072,913:0.0145,1009:0.0072,1153:0.0,1225:0.0072,1243:0.0072,125:0.0072,1261:0.0,1289:0.0,1315:0.0,1847:0.0,2107:0.0072,2297:0.0,2323:0.0,4349:0.0072,4619:0.0,4835:0.0,567:0.0072,927:0.0,1179:0.0145,1287:0.0072,1313:0.0,1755:0.0,1845:0.0145,2123:0.0,2439:0.0,475:0.0,4833:0.0,5067:0.0,5346:0.0,5355:1.0,5409:0.0072,989:0.0,1219:0.0,1255:0.0,1291:0.0145,1589:0.0217,1769:0.0,1831:0.0,2407:0.0,2425:0.0,263:0.0072,3163:0.0072,4261:0.0072,4603:0.0,489:0.0,687:0.0072,1145:0.0072,2117:0.0,2595:0.0,351:0.0072,4953:0.0,757:0.0072,1067:0.0072,1085:0.0,1319:0.0,1553:0.0072,2453:0.0,291:0.0,3821:0.0,453:0.0072,4929:0.0,723:0.0  12851:0.0833,14589:0.0833,15641:0.0833,247:0.25,2553:0.3333,31823:0.0833,319:0.0833,373:0.0833,4191:0.0833,43839:0.0833,6027:0.0833,823:0.1667,10063:0.0833,1395:0.0833,17237:0.0833,1999:0.0833,2015:0.0833,2105:0.0833,2213:0.0833,2763:0.0833,2907:0.0833,33950:0.1667,37109:0.0833,40277:0.0833,411:0.1667,42301:0.0833,5797:0.0833,5841:0.0833,7317:0.0833,925:0.0833,10255:0.0833,13379:0.0833,14701:0.0833,1983:0.0833,23837:0.0833,2441:0.0833,28067:0.1667,31847:0.0833,423:0.0833,42304:0.0833,4845:0.3333,531:0.1667,595:0.0833,757:0.25,18359:0.0833,19943:0.1667,2165:0.0833,273:0.1667,309:0.0833,32263:0.0833,32777:0.0833,39699:0.0833,4217:0.1667,453:0.5,7331:0.0833,9113:0.0833,10585:0.0833,11683:0.1667,1367:0.8333,14347:0.0833,1529:0.0833,1673:0.0833,1727:0.0833,19739:0.0833,2087:0.0833,2131:0.1667,21961:0.0833,2203:0.0833,249:0.0833,30295:0.0833,3211:0.1667,40456:0.0833,42201:0.0833,43624:0.0833,4689:0.0833,5903:0.0833,627:0.1667,799:0.0833,1045:0.1667,1847:0.25,2017:0.0833,2549:0.0833,26581:0.0833,32999:0.0833,39343:0.0833,40260:0.0833,413:0.0833,4213:0.0833,459:0.0833,5573:0.0833,729:0.3333,765:0.1667,1237:0.1667,16125:0.0,16747:0.1667,19799:0.0833,23271:0.0833,2821:0.0833,35809:0.1667,36916:0.0833,371:0.0833,399:0.0,4117:0.0833,42180:0.0833,4577:0.0833,6485:0.0833,803:0.0833,1203:0.0833,1681:0.0833,16939:0.0833,17389:0.0833,1825:0.0833,1933:0.25,2545:0.0833,257:0.1667,2725:0.0833,27991:0.0833,28387:0.0833,30555:0.0833,3625:0.0833,365:0.0833,41157:0.0833,41715:0.25,419:0.25,4309:0.0833,5407:0.1667,1567:0.0833,1729:0.1667,19425:0.0833,2115:0.0833,2197:0.0833,2331:0.0833,24573:0.0833,28155:0.1667,287:0.004165,28867:0.0833,30837:0.0833,36039:0.0833,403:0.4167,4069:0.0833,41574:0.0833,4249:0.0833,42735:0.0833,4807:0.0833,5707:0.0,6625:0.0833,9181:0.0833,1047:0.0833,12191:0.3333,1461:0.0833,1687:0.1667,18221:0.0833,2253:0.0833,25151:0.0833,25395:0.0833,34430:0.0833,41117:0.0833,41748:0.1667,451:0.1667,5395:1.0,749:0.1667,901:0.0833_1159:0.3333,11943:0.3333,2239:0.3333,2455:0.3333,2359:0.3333,2511:0.3333,403:0.3333,1687:0.3333,2965:0.3333,41711:0.3333,41784:0.3333,497:0.3333,599:0.3333,1367:1.0,2159:0.3333,36055:0.3333,4689:0.3333,4845:0.3333,1561:0.3333,17403:0.3333,583:0.3333,727:0.3333,17923:0.3333,1847:0.3333,567:0.3333,6545:0.3333,729:0.3333,765:0.3333,8471:0.3333,723:0.3333    261839:0.1667,337871:0.0333,404254:0.0167,405505:0.0167,415900:0.0,416099:0.0667,421138:0.0833,422281:0.0167,12693:0.2333,223871:0.7,322277:0.1833,404249:0.0,415904:0.0167,416516:0.1167,418910:0.0333,421709:0.0667,60753:0.05,6175:0.1333,71049:0.0167,15135:0.2,380411:0.0167,416763:0.0667,420876:0.1,421136:0.0833,54989:0.0333,64221:0.05,9149:0.0167,9239:0.0333,104979:0.0167,14995:0.2,19855:0.0167,304607:0.0333,322337:0.3,35253:0.0667,388605:0.0333,408332:0.0167,415884:0.2,416513:0.1333,418908:0.0167,39467:0.8333,399406:0.1667,416767:0.0333,418909:0.0333,47323:0.0333,59825:0.05,60751:0.2667,149611:0.0167,405508:0.0167,412221:0.0667,414544:0.0667,415903:0.3167,418018:0.0833,420673:0.0167,422284:0.05,116853:0.35,387:0.0333,388447:0.0167,405573:0.0167,418380:0.6667,421575:0.0167,421962:0.0333,422457:0.0167,2289:0.05,391796:0.0167,392858:0.0167,415898:0.1167,416734:0.0333,418264:0.9,422458:0.1,405421:0.0333,406493:0.8,422459:0.0833,64211:0.0833,149617:0.5167,381538:0.1,388351:0.7833,397801:0.034999999999999996,403137:0.0333,404253:1.0,406729:0.0167,416728:0.25,417286:0.0,421137:0.0167,421597:0.0333_370965:0.125,19787:1.0,302531:0.125,341:0.5,416030:0.25,302533:0.125,95109:0.25,389568:0.25,416026:0.375,146271:0.125,167917:1.0,422210:0.25,78121:0.25,362879:0.125,416031:0.25,67053:0.125,416025:0.5,31029:0.125
