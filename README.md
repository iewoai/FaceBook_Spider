# FaceBook_Spider
[iewoai]facebook的商业信息爬虫（scrapy版）
### facebookSpider程序思路

##### facebook爬虫分为三部分
1. 爬取公司主页url丢进redis（利于多服务器分配任务）
2. 爬取每一个公司的字段信息丢进redis
3. 监听redis，将公司数据批量插入数据库
