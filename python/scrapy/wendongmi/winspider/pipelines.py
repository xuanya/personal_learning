# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from winspider.connect import MysqlConnect
from scrapy.exceptions import DropItem

class DuplicatesPipeline(object):
    """ Item去重 """
    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        if item.get('response'):
            if item['md5'] in self.ids_seen:
                raise DropItem("Duplicate item fount: %s" % item)
            else:
                self.ids_seen.add(item['md5'])
                return item
        return item

class FillResponsePipeline(object):
    """ 填充应答 """
    def process_item(self, item, spider):
        if not item.get('response'):
            item['response'] = ''
        return item


class WinspiderPipeline(object):
    """ 数据入库 """
    def __init__(self, host=None, port=None, user=None, passwd=None, db=None, charset='utf8'):
        """ 初始化数据 """
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.charset = charset

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            host=crawler.settings.get('MYSQL_HOST'),
            port=crawler.settings.get('MYSQL_PORT'),
            user=crawler.settings.get('MYSQL_USER'),
            passwd=crawler.settings.get('MYSQL_PASSWD'),
            db=crawler.settings.get('MYSQL_DB'),
            charset=crawler.settings.get('MYSQL_CHARSET')
        )

    def open_spider(self, spider):
        self.conn = MysqlConnect(self.host, self.port,
                                 self.db, self.user, self.passwd, self.charset)

    def process_item(self, item, spider):
        sql = """
            REPLACE INTO info_dongmi (md5, date, questioner, question, responder, response, securitycode, securityname) VALUES (%(md5)s, %(date)s, %(questioner)s, %(question)s, %(responder)s, %(response)s, %(securitycode)s, %(securityname)s)
            """
        sql_params = dict(item)
        self.conn.deal_sql(sql, sql_params)
        return item

    def close_spilder(self, spider):
        self.conn.close()
