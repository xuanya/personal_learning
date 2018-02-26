# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class WinspiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    md5 = scrapy.Field()                # md5 加密结果，对日期，提问者，问题进行加密
    date = scrapy.Field()                # 提问日期
    questioner = scrapy.Field()            # 提问者
    question = scrapy.Field()            # 问题
    responder = scrapy.Field()            # 回答者
    response = scrapy.Field()            # 回答内容
    securitycode = scrapy.Field()        # 股票代码
    securityname = scrapy.Field()        # 股票简称
