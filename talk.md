ip测试

iptest
为一个go 语言项目，python 和 go 都可以。一般的ip时间为3-10分钟
user-zphdrj4g-region-us-sessid-2475130-sesstime-600:U9NR5Dr4@gate3.abcsocks.cloud:9102

一般的这种为ip 地址，2475130 这个为随机数，2475130 这个数可以定义，比如 给1000000  9000000
测试ip资源池。到底有多少，
测试选项，有半个小时，一个小时，时间可以选，
半小时可以停止，
重复度达到多少可以停止，
单ip测试时间。

报表显示出来
区域分布，主要区分国家就好了，
重复度。
连续性，比如同一个session获取的ip，能达到3分钟多少，达到10分钟有多少，
有多少换了ip，比如，一分钟循环一次，每次是不是同一个ip。

https://api.skyvora.cc/appapi/geo/cityinfo 这个能查询，到底多少重复的，因为返回ip。
curl -x 
socks5h://user-zphdrj4g-region-us-sessid-2475130-sesstime-600:U9NR5Dr4@gate3.abcsocks.cloud:9102 https://api.skyvora.cc/appapi/geo/cityinfo


有界面可以看，可以设置调价，执行要保存记录，点击查看，显示上次执行结果。

数据保存用sqllite，
性能有协程，高并发。   一般为8g 16g 看能支持多少并发。

先理解需求

系统背景不要搞成黑色的，主流美 。 重复率没有体现出来吗 。

最后搞个典型的任务测试，不要删除。 

任务可以复制，就是默认把原来的参数，带过去，点击保存，生成新的记录。 

界面不要这么黑，图标要搞主流的，搞那么丑的图标 。
SOCKS5 代理 配置 有点复杂，协议默认socks5 
提示字符串，
user-zphdrj4g-region-us-sessid-2475130-sesstime-600:U9NR5Dr4@gate3.abcsocks.cloud:9102 
 user-zphdrj4g-region-us-sessid-{num}-sesstime-600:U9NR5Dr4@gate3.abcsocks.cloud:9102 
这样他就会sessid 自动替换掉。
请求延迟也要记录。按秒吧，一位小数 。 

admin.dearka.cc/iptest/ 跳入到原来后台了，打不开


优化卡的原因，里面的重复度，和外面的重复度，对不上。 88.20% 列表重复度不一样吗，还是怎么的 。实时计算的 吗 。 停掉了又一样了。实在多的花，一个任务，要有静态数据，放静态数据。参数要显示上去。
user-zphdrj4g-region-us-sessid-2475130-sesstime-600:U9NR5Dr4@gate3.abcsocks.cloud:9102
至少第一行要把代理这些信息显示上去吧。不然怎么知道测试什么 

任务要能修改，没有停止，只有暂停，和开始。


跑了几个任务，整体还行。
检查数据各维度，有什么错误码，
性能上有什么需要优化的吗