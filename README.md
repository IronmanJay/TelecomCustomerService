# TelecomCustomerService
通信运营商每时每刻会产生大量的通信数据，例如通话记录，短信记录，彩信记录，第三方服务资费等等繁多信息。数据量如此巨大，除了要满足用户的实时查询和展示之外，还需要定时定期的对已有数据进行离线的分析处理。例如，当日话单，月度话单，季度话单，年度话单，通话详情，通话记录等等。我们以此为背景，寻找一个切入点，学习其中的方法论。当前我们的需求是：统计每天、每月以及每年的每个人的通话次数及时长。
## 项目环境
windows10、centos7(三集群，三台配置都为3G，4核)、jdk1.8、idea2019.3、maven3.3.9、hadoop2.7.2、zookeeper3.4.10、hbase1.3.1、flume1.7.0、kafka2.11-0.11.0.0
## 项目架构
Flume监听生成数据  
Kafka作为消息队列  
Hbase作为消息数据存储  
MapReduce作为业务指标分析  
Web前端展示使用echart  
