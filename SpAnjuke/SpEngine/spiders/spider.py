# -*- coding: utf-8 -*-
import scrapy
import requests
import re
import time
import types
import json

from lxml import etree
from ..items import TheItem
from scrapy_redis.spiders import RedisSpider

import logging


class TheSpider(RedisSpider):
    name = 'wjSpider'
    redis_key = 'wjspiderKey:urls'
    #    start_urls = 'https://sh.lianjia.com/xiaoqu/'
    # start_urls = ['https://www.anjuke.com/fangjia/shanghai2018/',
    #               'https://www.anjuke.com/fangjia/shanghai2017/',
    #               'https://www.anjuke.com/fangjia/shanghai2016/',
    #               'https://www.anjuke.com/fangjia/shanghai2015/',
    #               'https://www.anjuke.com/fangjia/shanghai2014/',
    #               'https://www.anjuke.com/fangjia/shanghai2013/',
    #               'https://www.anjuke.com/fangjia/shanghai2012/',
    #               'https://www.anjuke.com/fangjia/shanghai2011/',
    #               'https://www.anjuke.com/fangjia/shanghai2010/',
    #               'https://www.anjuke.com/fangjia/shanghai2009/']
    start_urls = ['https://www.anjuke.com/fangjia/shanghai2018/']

    url_list = ('beicai',
                'biyun',
                'chuansha',
                'gaodong',
                'gaohang',
                'huamu',
                'jinqiao',
                'jinyang',
                'kangqiao',
                'lujiazui',
                'lianyang',
                'nanmatou',
                'shibo',
                'sanlin',
                'tangqiao',
                'tangzhen',
                'weifang',
                'yangdong',
                'yangjing',
                'yuqiao1',
                'yuanshen',
                'zhangjiang',
                'zhoupu')

    url_list_backup = ('beicai',
                       'biyun',
                       'chuansha',
                       'gaodong',
                       'gaohang',
                       'huamu',
                       'jinqiao',
                       'jinyang',
                       'kangqiao',
                       'lujiazui',
                       'lianyang',
                       'nanmatou',
                       'shibo',
                       'sanlin',
                       'tangqiao',
                       'tangzhen',
                       'weifang',
                       'yangdong',
                       'yangjing',
                       'yuqiao1',
                       'yuanshen',
                       'zhangjiang',
                       'zhoupu')

    handle_httpstatus_list = [301, 302]

    def start_requests(self):
        logging.debug("\033[1;32;47m WangJ := \033[0m"  'start_requests\n')
        user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.22 \
                         Safari/537.36 SE 2.X MetaSr 1.0'
        headers = {'User-Agent': user_agent}  # ,  'allow_redirects' : 'True' }

        # s = requests.Session()
        # p = s.get(self.start_urls, headers=headers, allow_redirects=True) #, allow_redirects=False
        # print( 'WangJ   pppp\n')
        # print p
        # print p.cookies
        # contents = etree.HTML(p.content.decode('utf-8'))
        # print contents
        # area_list = contents.xpath('/html/body/div[1]/div[2]/div[3]/div[1]/div[2]/a[@gahref="results_totalpage"]')
        #
        #
        # print area_list
        # print len(area_list)
        # for area in area_list:
        #     print area.text

        # area_list = contents.xpath('/html/body/div[1]/div[2]/div[3]/div[1]/div[2]/a[@gahref="results_totalpage"]')
        # print area_list

        logging.debug("\033[1;32;47m WangJ := \033[0m" "处理待爬首页面列表")

        for area in self.start_urls:
            logging.debug("\033[1;32;47m WangJ := \033[0m" "为parse-1 加入新的待爬页面 %s " % area)
            yield scrapy.Request(url=area, headers=headers, method='GET', callback=self.parse_1)

    # for URL in self.url_list :
    #     theURL = self.start_urls + URL + '/'
    #     yield scrapy.Request(url=theURL, headers=headers, method='GET', callback=self.parse)

    def getChildPageRequests(self, response, deep):
        user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.22 \
                                         Safari/537.36 SE 2.X MetaSr 1.0'
        headers = {'User-Agent': user_agent}

        # area_contents = requests.get(self.start_urls ,headers = headers ) #这是自己去读取网页
        # area_contents = etree.HTML(area_contents.content.decode('utf-8'))
        sText = ""
        theURL = ""

        lists = response.body.decode('utf-8')
        selector = etree.HTML(lists)

        if deep == 1:
            Cities = selector.xpath('/html/body/div[2]/div[3]/div[1]/span[2]/a')

        if deep == 11:
            Cities = selector.xpath('/html/body/div[2]/div[3]/div[1]/span[2]/div[1]/a')

        # print( Cities )
        for aaa in Cities:
            #     print aaa.attrib.get("href")
            #     print aaa.text

            if deep == 1:
                theURL = aaa.attrib.get("href")
                theURL = theURL.replace('http:', 'https:')
                sText = aaa.text
            if deep == 11:
                theURL = aaa.attrib.get("href")
                theURL = theURL.replace('http:', 'https:')
                sText = aaa.text

            logging.debug("\033[1;32;47m WangJ := \033[0m" "parse-%d-add-1 生成新的待爬页面 %s : %s" % (deep,sText, theURL))

            if deep == 1:
                yield scrapy.Request(url=theURL, headers=headers, method='GET', callback=self.parse_11)
            if deep == 11:
                yield scrapy.Request(url=theURL, headers=headers, method='GET', callback=self.parse_111)


    # def getIntInStr(self, theStr):
    #     theStr1 = theStr.encode('gbk')
    #     theStr1 = filter(str.isdigit, theStr1)
    #     theStr2 = ''.join(theStr1)
    #     #        print(type(theStr1))
    #     #        print(theStr2)
    #     if len(theStr2) > 0:
    #         return int(theStr2)
    #     else:
    #         return 0
    #
    # def getStrInXpath(self, theList):
    #     if isinstance(theList, list) and len(theList) > 0:
    #         return theList.pop()
    #     return ''

    def getYearPriceData(self, selector):

        item = TheItem()
        # item['title']
        item['dates'] = []
        item['prices'] = []

        item['name'] = ""
        item['year'] = 0

        # 读取html里面的script文本，其中包含图表数据
        readText = selector.xpath('/html/body/script[8]')[0].text

        # 找到drawChart的开始结尾，截取里面的数据
        p1 = readText.find('window.drawChart')
        if p1 < 0:
            return item

        p2 = readText.find(');', p1 + 2)
        readText = readText[p1 + 17:p2]
        # logging.debug("\033[1;32;47m WangJ := \033[0m" "%s" % readText)

        # 找到xyear的开始结尾，截取里面的数据
        p1 = readText.find('xyear')
        if p1 < 0:
            return item
        p1 = readText.find('{', p1)
        p2 = readText.find('}', p1)
        readData = readText[p1:p2 + 1]

        readData = json.JSONDecoder().decode(readData)
        # logging.debug("\033[1;32;47m WangJ := \033[0m" "%s" % readData)

        # dddd = json.loads(aaaa) # eval(aaaa)

        data_xyear = []
        for (d, x) in readData.items():
            data_xyear.append(x + d)
            # data_xyear.append( x.encode("utf-8")+d.encode("utf-8") )
        # data_xyear.sort()

        # 合成好时间列表，打印显示
        logging.debug("\033[1;32;47m WangJ := \033[0m" "%s" % data_xyear)

        item['dates'] = data_xyear # ",".join(data_xyear)

        # item['year'] = data_xyear[0] # 用第一个日期，作为当前数据的年份

        # 找到ydata的开始结尾，截取里面的数据
        p1 = readText.find('ydata')
        if p1 < 0:
            return item
        p1 = readText.find('{', p1)
        p2 = readText.find('}', p1)
        readData = readText[p1:p2 + 1]

        readData = json.JSONDecoder().decode(readData)

        # dddd = json.loads(aaaa) # eval(aaaa)


        data_city = readData['name']
        data_prices = readData['data']

        logging.debug("\033[1;32;47m WangJ := \033[0m" "%s" % data_city)

        item['name'] = data_city
        item['prices'] = data_prices

        return item

    def getParent(self, selector):
        # 获取顶部面包屑里面的可点击的父级城市名称
        readText = selector.xpath("/html/body/div[2]/div[1]/div[1]/div[1]/a/text()")

        # 生成父级城市的逗号字符串
        data_Parents = ","
        data_Parents = data_Parents.join(readText)

        # 生成父级城市的List
        data_Lists = "\',\'"
        data_Lists = "\'" + data_Lists.join(readText) + "\'"

        # for i in range(0, len(readText) - 1):
        #     readText1 = readText1 + readText[i].text
        # "/html/body/div[2]/div[1]/div/div/a[3]"

        # 获取顶部面包屑里面的最后一个当前城市名称
        readText = selector.xpath("/html/body/div[2]/div[1]/div[1]/div[1]/text()")
        if (len(readText) > 0):
            readText = readText[len(readText) - 1]

        # 生成父级城市的逗号字符串
        data_Parents = data_Parents + "," + readText
        data_Parents = data_Parents.replace('\r', '').replace('\n', '').replace('\t', '').replace('>', '').replace(' ', '')

        # 生成父级城市的List
        data_Lists = data_Lists + ",\'" + readText
        data_Lists = data_Lists.replace('\r', '').replace('\n', '').replace('\t', '').replace('>', '').replace(' ','')

        # 合成好父级城市结构，打印显示
        logging.debug("\033[1;32;47m WangJ := \033[0m" "prepare 父级: %s" % data_Parents)

        return data_Parents


    def getItemOnPage(self, response, deep):
        # 本例子中,顶部 crumb 导航中的 deep 1,11,111 的处理方式一样,否则就要按deep来不同处理

        lists = response.body.decode('utf-8')
        selector = etree.HTML(lists)

        # 生成当前页面的数据ITEM
        item = self.getYearPriceData(selector)
        data_Parents = self.getParent(selector)
        item['parents'] = data_Parents

        data_Lists = data_Parents.split(",")

        # 用顶部 crumb 导航中的最后一块,作为年付和地区名
        sText = data_Lists[len(data_Lists)-1]
        sText = sText.replace('房价', '')
        item['year'] = int(sText[0:4])
        item['name'] = sText[4:]

        # 去除掉年份,'房价'
        for i in range(2009, 2025):
            data_Parents = data_Parents.replace(str(i), '')
        data_Parents = data_Parents.replace('房价', '')
        data_Lists = data_Parents.split(",")
        item['p_list'] = data_Lists

        yield item

    def parse_1(self, response):
        # 第一层级 1，上海；第二层级11，浦东； 第三层级111，碧云
        deep = 1
        logging.debug("\033[1;32;47m WangJ := \033[0m" "parse-%d-Handle 处理安居客 %s" % (deep, response.url))  # 处理安居客shanghai2018

        # 生成当前页面的数据ITEM
        results = self.getItemOnPage(response, deep)
        for aItem in results:
            yield aItem

        # 搜索子页面
        results = self.getChildPageRequests(response, deep)
        for aReq in results:
            logging.debug("\033[1;32;47m WangJ := \033[0m" "parse-%d-add-2 加入新的待爬页面 %s " % (deep, aReq.url))
            yield aReq

    def parse_11(self, response):
        # 第一层级 1，上海；第二层级11，浦东； 第三层级111，碧云
        deep = 11
        logging.debug("\033[1;32;47m WangJ := \033[0m" "parse-%d-Handle 处理安居客 %s" % (deep, response.url))  # 处理安居客shanghai2018

        # 生成当前页面的数据ITEM
        results = self.getItemOnPage(response, deep)
        for aItem in results:
            yield aItem

        # 搜索子页面
        results = self.getChildPageRequests(response, deep)
        for aReq in results:
            logging.debug("\033[1;32;47m WangJ := \033[0m" "parse-%d-add-2 加入新的待爬页面 %s " % (deep, aReq.url))
            yield aReq


    def parse_111(self, response):
        # 第一层级 1，上海；第二层级11，浦东； 第三层级111，碧云
        deep = 111
        logging.debug("\033[1;32;47m WangJ := \033[0m" "parse-%d-Handle 处理安居客 %s" % (deep, response.url))  # 处理安居客shanghai2018

        # 生成当前页面的数据ITEM
        results = self.getItemOnPage(response, deep)
        for aItem in results:
            yield aItem
