# -*-coding:utf-8-*-
import pymysql
import redis
import time
import pickle
import random

REDIS_HOST = ''
REDIS_PORT = ''
REDIS_PASS = ''
item_key = ''
REDIS_DB = ''

MYSQL_HOST = ''
MYSQL_PORT = ''
MYSQL_PASS = ''
MYSQL_DB = ''
MYSQL_USER = ''
MYSQL_TABLE = ''


class Redis_Listen(object):
    def __init__(self):
        self.redis_server = redis.Redis(REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASS)
        self.isNone = False
        self.server, self.cur = self.get_server()

    def get_server(self):
        try:
            server = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASS, db=MYSQL_DB, charset='utf8')
            cur = server.cursor()
            return server, cur
        except Exception as e:
            print(e)

    def listen(self, timeout=0, count=10):
        item_data = []
        isNone = self.isNone

        if isNone:
            for i in range(0, count):
                data = self.redis_server.brpop(item_key, timeout)
                if data:
                    data = pickle.loads(data[1])
                    data = self.clear_data(data)
                    item_data.append(data)
        else:
            for i in range(0, count):
                data = self.redis_server.rpop(item_key)
                if data:
                    data = pickle.loads(data)
                    data = self.clear_data(data)
                    item_data.append(data)

        if len(item_data) > 0:
            self.isNone = False
        else:
            self.isNone = True

        return item_data

    def process_item(self, item_list):
        data_list = list()
        for item in item_list:
            if 'companyName' in item.keys():
                data = (item['originalUrl'],item['website'],item['updatetime'],item['phone'],item['description'],item['mainBusiness'],item['email'],item['detailUrl'],item['category'],item['address'],item['companyName'],item['sns'])
                data_list.append(data)
        sql = """insert ignore into {table} (originalUrl,website,updatetime,phone,description,mainBusiness,email,detailUrl,category,address,companyName,sns)  values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""".format(table=MYSQL_TABLE)
        while True:
            try:
                self.cur.executemany(sql, data_list)
                self.server.commit()
                print('####{} 插入成功 {} 表：{}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), len(data_list), MYSQL_TABLE))
                break
            except pymysql.err.InterfaceError as e:
                self.server = pymysql.connect(MYSQL_HOST, MYSQL_USER, MYSQL_PASS, MYSQL_DB)
                self.cur = self.server.cursor()
                time.sleep(2)
            except Exception as e:
                if 'Duplicate entry' in str(e):
                    break
                elif "doesn't exist" in str(e) and 'Table' in str(e):
                    self.create_table(MYSQL_TABLE)
                    continue
                else:
                    print('insert_error: ', e, item)
                    self.cur.execute('rollback;')

    def create_table(self, table, dbconn):
        cur = dbconn.cursor()
        sql = """
                CREATE TABLE `{}` (
                  `id` int(11) NOT NULL AUTO_INCREMENT,
                  `detailUrl` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '详情页地址',
                  `companyName` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '公司名称',
                  `category` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '类型',
                  `originalUrl` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '官网',
                  `website` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '网站',
                  `address` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '地址',
                  `countryEn` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '国家',
                  `city` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '城市',
                  `phone` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '电话',
                  `fax` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '传真',
                  `email` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '邮箱',
                  `description` longtext COLLATE utf8mb4_unicode_ci COMMENT '描述',
                  `mainBusiness` text COLLATE utf8mb4_unicode_ci COMMENT '主营',
                  `updateTime` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                  `contactName` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '联系人姓名',
                  `status` int(11) DEFAULT '0',
                  `sns` longtext COLLATE utf8mb4_unicode_ci COMMENT '社交账号',
                  `employees` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '员工',
                  `sales` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '销量',
                  `jobTitle` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '职位',
                  `language` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'en' COMMENT '网站语言 英文en 非英文other',
                  PRIMARY KEY (`id`),
                  UNIQUE KEY `key_unique` (`companyName`,`detailUrl`) USING BTREE
                ) ENGINE=InnoDB AUTO_INCREMENT=21625 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='facebook公司爬虫';
                """.format(table)
        try:
            cur.execute(sql)
            dbconn.commit()
            print('创建表' + table)
        except Exception as e:
            print(e)
            cur.execute('rollback;')

    def clear_data(self, data):
        data = str(data)
        if '\x00' in data:
            data = data.replace('\x00', '')
        data = eval(data)
        return data


def start():
    redis_listen_process = Redis_Listen()
    while True:
        num = redis_listen_process.redis_server.llen(item_key)
        print('#### redis共有%d条数据'%num)
        item_list = redis_listen_process.listen(timeout=1, count=1000)
        if item_list:
            redis_listen_process.process_item(item_list)
        else:
            r = random.uniform(30, 60)
            print('## redis无更多数据，暂停%ds'%r)
            time.sleep(r)


if __name__ == "__main__":
    start()

