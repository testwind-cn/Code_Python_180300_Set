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
    name = 'xiaoquSpider'
    redis_key = 'xqspiderKey:urls'
    start_urls = 'https://sh.lianjia.com/xiaoqu/'

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
        logging.debug( 'WangJ   start_requests\n')
        user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.22 \
                         Safari/537.36 SE 2.X MetaSr 1.0'
        headers = {'User-Agent': user_agent }

#        s = requests.Session()
#        p = s.get(self.start_urls, headers=headers, allow_redirects=True) #, allow_redirects=False
        print 'WangJ   pppp\n'
#        print p
#        print p.cookies		
#        contents = etree.HTML(p.content.decode('utf-8'))
#        print contents
#        area_list = contents.xpath('/html/body/div[1]/div[2]/div[3]/div[1]/div[2]/a[@gahref="results_totalpage"]')


#        print area_list
#        print len(area_list)
#        for area in area_list:
#            print area.text

		
#        area_list = contents.xpath('/html/body/div[1]/div[2]/div[3]/div[1]/div[2]/a[@gahref="results_totalpage"]')
#        print area_list

#        area_contents = requests.get(self.start_urls ,headers = headers )
#        area_contents = etree.HTML(area_contents.content.decode('utf-8'))

#        Cities = area_contents.xpath('/html/body/div[2]/div[3]/div[1]/span[2]/a')
#        print Cities
#        for aaa in Cities:
#            print aaa.attrib.get("href")
#            print aaa.text
#            theURL = aaa.attrib.get("href")
#            yield scrapy.Request(url=theURL, headers=headers, method='GET', callback=self.parse20)


        for URL in self.url_list :
            theURL = self.start_urls + URL + '/'
            yield scrapy.Request(url=theURL, headers=headers, method='GET', callback=self.parse2)

  
    def getIntInStr(self, theStr):
        theStr1 = theStr.encode('gbk')
        theStr1 = filter(str.isdigit, theStr1)
        theStr2 = ''.join(theStr1)
#        print(type(theStr1))
#        print(theStr2)
        if len(theStr2) > 0 :
            return int(theStr2)
        else :
            return 0


    def getStrInXpath(self, theList):
        if isinstance(theList, list) and len(theList) > 0:
            return theList.pop()
        return ''


    def parse3(self, response):
#        time.sleep(2)
        print 'WangJ   parse2 \n'
        try:
            lists = response.body.decode('utf-8')
            selector = etree.HTML(lists)
            houselist = selector.xpath('/html/body/div[1]/div[2]/div[3]/div[1]/ul[1]/li')
            print len(houselist)
           
            for house in houselist:
                print "get house list"
#               print house
#                print etree.tostring(house)
                try:
                    item = TheItem()






                    item['link'] ="http://sh.lianjia.com" + self.getStrInXpath(house.xpath('a[1]/@href'))
                    item['image'] = self.getStrInXpath(house.xpath('a[1]/img[1]/@data-img-real'))
                    item['title'] = self.getStrInXpath(house.xpath('div[1]/div[1]/a/text()'))

                    item['model'] = ''
                    item['area'] = ''
                    item['tall'] = ''
                    item['faceto'] = ''
                    detail = self.getStrInXpath(house.xpath('div[1]/div[2]/div[1]/span[1]/text()')).split('|')
                    if (len(detail) > 0) :
                        item['model'] = detail[0].replace('\t','').replace('\n','').replace(' ','')
                    if (len(detail) > 1) :
                        item['area'] = detail[1].replace('\t','').replace('\n','').replace(' ','')
                    if (len(detail) > 2) :
                        item['tall'] = detail[2].replace('\t','').replace('\n','').replace(' ','')
                    if (len(detail) > 3) :
                        item['faceto'] = detail[3].replace('\t','').replace('\n','').replace(' ','')


                    item['price'] = self.getStrInXpath(house.xpath('div[1]/div[2]/div[1]/div[1]/span[1]/text()'))
                    item['community'] = "http://sh.lianjia.com" + self.getStrInXpath(house.xpath('div[1]/div[2]/div[2]/span[1]/a[1]/@href'))
                    item['community_title'] = self.getStrInXpath(house.xpath('div[1]/div[2]/div[2]/span[1]/a[1]/span[1]/text()'))
                    item['city'] = self.getStrInXpath(house.xpath('div[1]/div[2]/div[2]/span[1]/a[2]/text()'))
                    item['town'] = self.getStrInXpath(house.xpath('div[1]/div[2]/div[2]/span[1]/a[3]/text()'))
                    item['age'] = self.getIntInStr( self.getStrInXpath(house.xpath('div[1]/div[2]/div[2]/span[1]/text()')) )
                    item['average_price'] = self.getIntInStr( self.getStrInXpath(house.xpath('div[1]/div[2]/div[2]/span[2]/text()')) )


                    item['tag0'] = ''
                    item['tag1'] = ''
                    item['tag2'] = ''
                    tags = house.xpath('div[1]/div[3]/span')
                    if (len(tags) > 0):
                        item['tag0'] = self.getStrInXpath(tags[0].xpath('text()'))
                    if (len(tags) > 1):
                        item['tag1'] = self.getStrInXpath(tags[1].xpath('text()'))
                    if (len(tags) > 2):
                        item['tag2'] = self.getStrInXpath(tags[2].xpath('text()'))

                except Exception:
                    pass
                yield item

        except Exception:
            pass


    def parse2(self, response):
        print 'WangJ   parse2 这是处理链家小区数据 \n'       # 这是处理链家小区数据
        user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.22 \
                                         Safari/537.36 SE 2.X MetaSr 1.0'
        headers = {'User-Agent': user_agent}

        #        print response.request.meta
        lists = response.body.decode('utf-8')
        selector = etree.HTML(lists)
#        area_list = selector.xpath('/html/body/div[4]/div[2]/div[2]/ul[1]/li')   # 处理小区数据 2018-2-20 之前的链家网址
        area_list = selector.xpath('/html/body/div[4]/div[1]/ul[1]/li')  # 处理小区数据 2018-2-20 之后的链家网址


        for area in area_list:
            try:
                item = TheItem()

                item['m_dist'] = -1
                item['d_dist1'] = -1
                item['c_dist1'] = -1
                item['w_dist1'] = -1
                item['d_dist2'] = -1
                item['c_dist2'] = -1
                item['w_dist2'] = -1
                item['d_dist3'] = -1
                item['c_dist3'] = -1
                item['w_dist3'] = -1
                item['d_dist4'] = -1
                item['c_dist4'] = -1
                item['w_dist4'] = -1


                area_key = area.xpath('div[1]/a[1]/@key').pop()
                area_url = 'http://sh.lianjia.com/ershoufang/q' + area_key
                area_gps = area.xpath('div[2]/div[1]/div[1]/a[1]/@xiaoqu').pop()    # 地点
                area_gps = area_gps.encode('gbk').replace('[', '').replace(']', '').replace(' ', '')
                area_gps_x = area_gps.split(',')[0]
                area_gps_y = area_gps.split(',')[1]
                print(area_url)
                print(area_gps_x + " " + area_gps_y)

                area_contents = requests.get(area_url)
                area_contents = etree.HTML(area_contents.content.decode('utf-8'))

                item['name'] = area_contents.xpath('/html/body/div[1]/div[2]/div[3]/div[2]/div[1]/a[1]/text()').pop()
                item['key'] = area_key
                tempText = area_contents.xpath('/html/body/div[1]/div[2]/div[3]/div[2]/div[1]/p[1]/text()').pop()
                if tempText.find('-') >= 0 :
                    item['city'] = tempText.split('-')[0].split(' ')[0]
                    item['town'] = tempText.split('-')[0].split(' ')[1]
                    item['age'] = tempText.split('-')[1].replace(' ', '')
                else :
                    item['city'] = tempText.split(' ')[0]
                    item['town'] = tempText.split(' ')[1]
                    item['age'] = ''
                item['gps_x'] = area_gps_x
                item['gps_y'] = area_gps_y
                item['average_price'] = self.getIntInStr( area_contents.xpath('/html/body/div[1]/div[2]/div[3]/div[2]/ul[1]/li[1]/div[2]/span[1]/text()').pop() )
                item['onsale_num'] = self.getIntInStr( area_contents.xpath('/html/body/div[1]/div[2]/div[3]/div[2]/ul[1]/li[2]/div[2]/span[1]/text()').pop() )
                item['deal_num'] = self.getIntInStr( area_contents.xpath('/html/body/div[1]/div[2]/div[3]/div[2]/ul[1]/a[1]/li[1]/div[2]/span[1]/text()').pop() )
                item['watch_num'] = self.getIntInStr( area_contents.xpath('/html/body/div[1]/div[2]/div[3]/div[2]/ul[1]/li[3]/div[2]/span[1]/text()').pop() )
                item['link'] = 'http://sh.lianjia.com/xiaoqu/{}.html'.format(area_key)



                yield item

# /html /body/div[1]/div[2]/div[3]/div[2]/div[1]/a[1]/text
# /html/body/div[1]/div[2]/div[3]/div[2]/div[1]/p[1]/text
# /html/body/div[1]/div[2]/div[3]/div[2]/ul[1]/li[1]/div[2]/span[1]/text
# /html/body/div[1]/div[2]/div[3]/div[2]/ul[1]/li[2]/div[2]/span[1]/text
# /html/body/div[1]/div[2]/div[3]/div[2]/ul[1]/a[1]/li[1]/div[2]/span[1]/text
# /html/body/div[1]/div[2]/div[3]/div[2]/ul[1]/li[3]/div[2]/span[1]/text
#                yield scrapy.Request(url=area_url, headers=headers, callback=self.detail_url, meta={"id1":area_gps,"id2":area_pin} )
            except Exception:
               pass

    
    def parse20(self, response):
        print 'WangJ   parse20 \n' #安居客shanghai2018

        
        user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.22 \
                                 Safari/537.36 SE 2.X MetaSr 1.0'
        headers = {'User-Agent': user_agent}
        
#        print response.request.meta
        
        
        lists = response.body.decode('utf-8')
        selector = etree.HTML(lists)
        aaaa = selector.xpath('/html/body/script[8]')[0].text

        p1 = aaaa.find('window.drawChart')
        p2 = aaaa.find(');', p1 + 2)
        aaaa = aaaa[p1 + 17:p2]
        print aaaa

        aaaa = json.JSONDecoder().decode(aaaa)

        dddd = json.loads(aaaa) # eval(aaaa)

        print dddd

#        area_list = selector.xpath('/html/body/div[1]/div[2]/div[3]/div[1]/div[2]/a[@gahref="results_totalpage"]')


#        area_list = selector.xpath('/html/body/div[4]/div[1]/div[3]')
#        area_list = selector.xpath('/html/body/div[4]/div[2]/div[3]/a')  # 在2018-2-1之前的小区页面模式
        area_list = selector.xpath('/html/body/div[4]/div[1]/div[3]/div[2]/div[1]/@page-data')
        print area_list   # {"totalPage":1,"curPage":1}
        print len(area_list)

        pages = 1    # 小于20个小区的页面，没有下一页，改变算法 默认为1个
        if len(area_list) > 0 :
             thePage = eval(area_list[0])
             if ( type(thePage) is types.DictType ) and thePage.has_key('totalPage') :
                 pages = thePage['totalPage']
                 print type(thePage)
                 print thePage.has_key('totalPage')
                 print pages
#            print area_list[len(area_list) - 2].text
#            pages = int( area_list[len(area_list) - 2].text )
        print pages

        for i in range( 0, pages ):
            print i+1
            area_url = '{}pg{}/'.format(response.request.url,i+1)
            print area_url
            yield scrapy.Request(url=area_url, headers=headers, callback=self.parse2 )

        def parse(self, response):
            print 'WangJ   parse1 \n'

            user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.22 \
                                     Safari/537.36 SE 2.X MetaSr 1.0'
            headers = {'User-Agent': user_agent}

            #        print response.request.meta

            lists = response.body.decode('utf-8')
            selector = etree.HTML(lists)
            #        area_list = selector.xpath('/html/body/div[1]/div[2]/div[3]/div[1]/div[2]/a[@gahref="results_totalpage"]')

            #        area_list = selector.xpath('/html/body/div[4]/div[1]/div[3]')
            #        area_list = selector.xpath('/html/body/div[4]/div[2]/div[3]/a')  # 在2018-2-1之前的小区页面模式
            area_list = selector.xpath('/html/body/div[4]/div[1]/div[3]/div[2]/div[1]/@page-data')
            print area_list  # {"totalPage":1,"curPage":1}
            print len(area_list)

            pages = 1  # 小于20个小区的页面，没有下一页，改变算法 默认为1个
            if len(area_list) > 0:
                thePage = eval(area_list[0])
                if (type(thePage) is types.DictType) and thePage.has_key('totalPage'):
                    pages = thePage['totalPage']
                    print type(thePage)
                    print thePage.has_key('totalPage')
                    print pages
            #            print area_list[len(area_list) - 2].text
            #            pages = int( area_list[len(area_list) - 2].text )
            print pages

            for i in range(0, pages):
                print i + 1
                area_url = '{}pg{}/'.format(response.request.url, i + 1)
                print area_url
                yield scrapy.Request(url=area_url, headers=headers, callback=self.parse2)
        
        
#        self.getItem(response)
        
#        for area in area_list:
#            try:
#                area_han = area.xpath('text()').pop()    # 地点
#                print(area_url)
#                print(area_han)
#                yield scrapy.Request(url=area_url, headers=headers, callback=self.detail_url, meta={"id1":area_han,"id2":area_pin} )
#            except Exception:
#                pass

    def get_latitude(self,url):  # 进入每个房源链接抓经纬度
        p = requests.get(url)
        contents = etree.HTML(p.content.decode('utf-8'))
        latitude = contents.xpath('/ html / body / script[19]/text()').pop()
        time.sleep(3)
        regex = '''resblockPosition(.+)'''
        items = re.search(regex, latitude)
        content = items.group()[:-1]  # 经纬度
        longitude_latitude = content.split(':')[1]
        return longitude_latitude[1:-1]

    def detail_url(self,response):
        'http://bj.lianjia.com/ershoufang/dongcheng/pg2/'
        for i in range(1,101):
            url = 'http://bj.lianjia.com/ershoufang/{}/pg{}/'.format(response.meta["id2"],str(1))
            time.sleep(2)
            try:
                contents = requests.get(url)
                contents = etree.HTML(contents.content.decode('utf-8'))
                houselist = contents.xpath('/html/body/div[4]/div[1]/ul/li')
                for house in houselist:
                    try:
                        item = TheItem()
                        item['title'] = house.xpath('div[1]/div[1]/a/text()').pop()
                        item['community'] = house.xpath('div[1]/div[2]/div/a/text()').pop()
                        item['model'] = house.xpath('div[1]/div[2]/div/text()').pop().split('|')[1]
                        item['area'] = house.xpath('div[1]/div[2]/div/text()').pop().split('|')[2]
                        item['focus_num'] = house.xpath('div[1]/div[4]/text()').pop().split('/')[0]
                        item['watch_num'] = house.xpath('div[1]/div[4]/text()').pop().split('/')[1]
                        item['time'] = house.xpath('div[1]/div[4]/text()').pop().split('/')[2]
                        item['price'] = house.xpath('div[1]/div[6]/div[1]/span/text()').pop()
                        item['average_price'] = house.xpath('div[1]/div[6]/div[2]/span/text()').pop()
                        item['link'] = house.xpath('div[1]/div[1]/a/@href').pop()
                        item['city'] = response.meta["id1"]
                        self.url_detail = house.xpath('div[1]/div[1]/a/@href').pop()
                        item['Latitude'] = self.get_latitude(self.url_detail)
                    except Exception:
                        pass
                    yield item
            except Exception:
                    pass