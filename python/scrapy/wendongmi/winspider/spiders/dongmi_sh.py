# -*- coding: utf-8 -*-
# ================================================
# 版本: V1.0.0
# 作者: jlchen
# 时间: 20180211
# 描述: 问董秘_上交所数据爬取
# 内容：问董秘上交所数据爬取
# ================================================
import scrapy
from winspider.items import WinspiderItem
import hashlib
from datetime import datetime
import re
from winspider.connect import MysqlConnect
from functools import reduce


# http://sns.sseinfo.com/ajax/getCompany.do   根据股票名称查询股票的内部id号
# {"data":"华夏银行"}

# http://sns.sseinfo.com/getNewDataCount.do   根据id号获取股票的总数量
# {"sdate":"2017-11-01", "edate":"2018-02-11", "keyword":"", "type":"1", "comId":"92"}

# http://sns.sseinfo.com/getNewData.do        根据id获取实际数据，单页10条，根据上一个请求获取页数
# {"sdate":"2017-11-01", "edate":"2018-02-11", "keyword":"", "type":"1", "page":"1", "comId":"92"}

class DongmiShSpider(scrapy.Spider):
    name = 'dongmi_sh'
    allowed_domains = ['sns.sseinfo.com']

    def get_conn(self):
        host=self.settings.get('MYSQL_HOST')
        port=self.settings.get('MYSQL_PORT')
        user=self.settings.get('MYSQL_USER')
        passwd=self.settings.get('MYSQL_PASSWD')
        db=self.settings.get('MYSQL_DB')
        charset=self.settings.get('MYSQL_CHARSET')

        self.conn = MysqlConnect(host, port, db, user, passwd, charset)

    def create_md5(self, date, questioner, question):
        """ 对日期，提问者，问题进行加密，生成md5码 """
        m2 = hashlib.md5()
        src = date + questioner + question
        m2.update(src.encode())
        return m2.hexdigest()

    def start_requests(self):
        self.year = getattr(self, 'year', '2017')
        self.sdate = getattr(self, 'sdate', '2017-01-01')
        self.edate = getattr(self, 'edate', '2017-12-31')

        # 从数据库中获取数据
        self.get_conn()
        sql = """ 
            SELECT SECURITYCODE 
            FROM cdsy_secucode 
            where TRADEMARKETCODE like '069001001%' 
                and LISTSTATE in ('0', '3') 
                and SECURITYTYPECODE in ('058001001', '058001002')
                and EISDEL_ZQ = 0"""
        res = self.conn.deal_sql(sql)
        self.socketcodes = list(map(lambda x: x.get('SECURITYCODE'), res))


        self.formdata = {
            "data":self.socketcodes.pop()
        }
        self.conn.close()
        return [scrapy.FormRequest('http://sns.sseinfo.com/ajax/getCompany.do', formdata=self.formdata)]

    def parse(self, response):
        """ 获取股票的内部id """
        self.con_id =  response.text
        send_data = {"sdate": self.sdate, 
            "edate": self.edate,
            "keyword": "", 
            "type": "1", 
            "comId": self.con_id}

        yield scrapy.FormRequest('http://sns.sseinfo.com/getNewDataCount.do', formdata=send_data, callback=self.get_new_data_count)

    def get_new_data_count(self, response):
        """ 获取数据总数量 """
        count = response.text
        pg_count = int(int(count) / 10 + 1)
        for i in range(1, pg_count+1):
            send_data = {"sdate": self.sdate, 
            "edate": self.edate, 
            "keyword":"", 
            "type":"1", 
            "page":str(i), 
            "comId":self.con_id}
            yield scrapy.FormRequest("http://sns.sseinfo.com/getNewData.do", formdata=send_data, callback=self.get_new_data)

    def get_new_data(self, response):
        """ 获取单页数据 """
        # from scrapy.shell import inspect_response
        # inspect_response(response, self)

        # 问答最总列表
        feed_items = response.xpath('//div[@class="m_feed_item"]')

        for feed_item in feed_items:
            item = WinspiderItem()
            item_length = len(feed_item.xpath('./div'))

            # 提问者
            questioner = feed_item.xpath('./div[2]/div[1]/p/text()').extract_first()
            questioner = questioner.strip() if questioner else questioner
            item['questioner'] = questioner

            # 回答者
            responder = feed_item.xpath('./div[2]/div[2]/div[2]/a/text()').extract_first()
            responder = responder[1:] if responder.startswith(':') else responder
            item['responder'] = responder

            # 问题
            questions = feed_item.xpath('./div[2]/div[2]/div[2]/text()').extract()
            question = reduce(lambda x,y: x + y, questions)
            question = question.strip() if question else question
            item['question'] = question

            # 股票代码
            # 股票简称
            try:
                securityname = re.findall(r'[^()]+', responder)[0]
                securitycode = re.findall(r'[^()]+', responder)[1] 
            except:
                img = feed_item.xpath('./div[@class="m_feed_detail m_qa"]/div[@class="m_feed_face"]/a/img/@src').extract_first()
                if img:
                    securitycode = re.findall(r'\d+', img)[0] 
                securityname = feed_item.xpath('./div[@class="m_feed_detail m_qa"]/div[@class="m_feed_face"]/p/text()').extract_first()

            securityname = securityname.strip() if securityname else securityname
            item['securitycode'] = securitycode
            item['securityname'] = securityname

            #回答内容
            if item_length != 2:
                ans_response = feed_item.xpath('./div[@class="m_feed_detail m_qa"]/div[@class="m_feed_cnt"]/div[@class="m_feed_txt"]/text()').extract_first()
                ans_response = ans_response.strip() if ans_response else ans_response
                item['response'] = ans_response

            # 问答日期  01月22日 17:55
            date = None
            if item_length == 2:
                date = feed_item.xpath('./div[2]/div[2]/div[@class="m_feed_func top10"]/div[@class="m_feed_from"]/span/text()').extract_first()
            else:
                date = feed_item.xpath('./div[3]/div[@class="m_feed_func top10"]/div[@class="m_feed_from"]/span/text()').extract_first()
            date = date.strip() if date else date
            date = self.year + '年' + date

            # md5
            md5 = self.create_md5(date, questioner, question)
            item['md5'] = md5
            date = datetime.strptime(date, "%Y年%m月%d日 %H:%M")
            item['date'] = date

            yield item
        # ---------- 下一只股票 ----------
        try:
            if len(self.socketcodes):
                formdata = {"data": self.socketcodes.pop()}
                yield scrapy.FormRequest('http://sns.sseinfo.com/ajax/getCompany.do', formdata=formdata, callback=self.parse)
        except Exception as e:
            self.logger.error(e)

