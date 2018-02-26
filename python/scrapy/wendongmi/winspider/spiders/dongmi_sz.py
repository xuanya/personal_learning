# -*- coding: utf-8 -*-
# ================================================
# 版本: V1.0.0
# 作者: jlchen
# 时间: 20180205
# 描述: 问董秘_深交所数据爬取
# 内容：问董秘深交所数据爬取
# ================================================
import scrapy
from winspider.items import WinspiderItem
import hashlib
from datetime import datetime
import re
from winspider.connect import MysqlConnect


class DongmiSzSpider(scrapy.Spider):
    name = 'dongmi_sz'
    allowed_domains = ['irm.cninfo.com.cn']

    def get_conn(self):
        host=self.settings.get('MYSQL_HOST')
        port=self.settings.get('MYSQL_PORT')
        user=self.settings.get('MYSQL_USER')
        passwd=self.settings.get('MYSQL_PASSWD')
        db=self.settings.get('MYSQL_DB')
        charset=self.settings.get('MYSQL_CHARSET')

        self.conn = MysqlConnect(host, port, db, user, passwd, charset)

    def start_requests(self):
        # stockcodes = getattr(self, 'stockcodes', '')
        self.get_conn()
        sql = """ 
            SELECT SECURITYCODE 
            FROM cdsy_secucode 
            where TRADEMARKETCODE like '069001002%' 
                and LISTSTATE in ('0', '3') 
                and SECURITYTYPECODE in ('058001001', '058001002')
                and EISDEL_ZQ = 0"""
        res = self.conn.deal_sql(sql)
        self.socketcodes = list(map(lambda x: x.get('SECURITYCODE'), res))

        self.date_from = getattr(self, 'date_from', '2018-01-01')
        self.date_to = getattr(self, 'date_to', '2018-06-01')

        self.formdata = {
            "condition.dateFrom":self.date_from, 
            "condition.dateTo":self.date_to, 
            "condition.stockcode":self.socketcodes.pop(), 
            "condition.keyWord":"", 
            "condition.status":"3", 
            "condition.searchType":"code", 
            "condition.questionCla":"", 
            "condition.questionAtr":"", 
            "condition.marketType":"Z", 
            "condition.searchRange":"0", 
            "condition.questioner":"", 
            "condition.questionerType":"", 
            "condition.loginId":"", 
            "condition.provinceCode":"", 
            "condition.plate":"", 
            "pageNo":"1", 
            "categoryId":"", 
            "code":"", 
            "pageSize":"10", 
            "source":"2", 
            "requestUri":"/ircs/interaction/topSearchForSzse.do", 
            "requestMethod":"POST"
        }
        self.conn.close()
        return [scrapy.FormRequest('http://irm.cninfo.com.cn/ircs/interaction/topSearchForSzse.do',
            formdata=self.formdata
             )]

    def create_md5(self, date, questioner, question):
        """ 对日期，提问者，问题进行加密，生成md5码 """
        m2 = hashlib.md5()
        src = date + questioner + question
        m2.update(src.encode())
        return m2.hexdigest()

    def parse(self, response):
        lis = response.xpath('//div[@id="con_one_1"]/div/ul/li')
        for li in lis:
            # ------ 提问部分 ------
            name = li.xpath('./div[@class="ask_Box clear"]/div[@class="userPic"]/a/span/text()').extract_first()
            if name:
                name = name.strip()
            asktime = li.xpath('./div[@class="ask_Box clear"]/div[@class="msg_Box"]/div[@class="pubInfo"]/text()').extract_first()
            asktime = asktime.strip() if asktime else asktime
            ask_company = li.xpath('./div[@class="ask_Box clear"]/div[@class="msg_Box"]/div[@class="msgCnt gray666"]/div/a[@class="blue2"]/text()').extract_first()
            ask_company = ask_company.strip() if ask_company else ask_company
            if ask_company.endswith(':'):
                ask_company = ask_company[:-1]
            con = li.xpath('./div[@class="ask_Box clear"]/div[@class="msg_Box"]/div[@class="msgCnt gray666"]/div/a[@class="cntcolor"]/text()').extract_first()
            con = con.strip() if con else con

            # ------ 解答部分 ------
            ans = li.xpath('./div[@class="answer_Box clear"]/div[@class="content_Box"]/div/div[@class="msgCnt gray666"]/a[@class="cntcolor"]/text()').extract_first()
            ans = ans.strip() if ans else ans

            securitycode = li.xpath('./div[@class="answer_Box clear"]/div[@class="userPic"]/span[@class="comCode"]/a/text()').extract_first()
            securitycode = securitycode.strip() if securitycode else securitycode

            securityname = li.xpath('./div[@class="answer_Box clear"]/div[@class="userPic"]/span[@class="comName"]/a/text()').extract_first()
            securityname = securityname.strip() if securityname else securityname

            md5 = self.create_md5(asktime, name, con)

            item = WinspiderItem()
            item['md5'] = md5
            item['date'] = datetime.strptime(asktime, "%Y年%m月%d日 %H:%M")
            item['questioner'] = name
            item['question'] = con
            item['responder'] = ask_company
            item['response'] = ans
            item['securitycode'] = securitycode
            item['securityname'] = securityname
            
            yield item

        # 查找下一页的页码
        pg_list = response.xpath('//div[@id="box_center"]/div/div[2]/table/tbody/tr[2]/td/a')
        if pg_list:
            last_pg = pg_list[-1]
            self.logger.debug(last_pg)

            requestboy = response.request.body.decode()
            for i in requestboy.split('&'):
                if 'pageNo' in i:
                    up_pg = i.split('=')[1]
            pg_href = last_pg.xpath('@href').extract_first()
            page = re.findall(r'\d+', pg_href)
            try:
                page = page[0]
                if int(up_pg) < int(page):
                    self.formdata['pageNo'] = page
                    yield scrapy.FormRequest(url=response.url, formdata=self.formdata)
                else:
                    if len(self.socketcodes):
                        self.formdata['pageNo'] = '1'
                        self.formdata['condition.stockcode'] = self.socketcodes.pop()
                        yield scrapy.FormRequest(url=response.url, formdata=self.formdata)
            except Exception as e:
                self.log(e)
        else:
            if len(self.socketcodes):
                self.formdata['pageNo'] = '1'
                self.formdata['condition.stockcode'] = self.socketcodes.pop()
                yield scrapy.FormRequest(url=response.url, formdata=self.formdata)


