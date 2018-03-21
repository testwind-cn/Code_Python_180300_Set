# -*- coding: utf-8 -*-
import scrapy
import requests
import re
import time
from lxml import etree
from ..items import HouseItem
from scrapy_redis.spiders import RedisSpider


import logging


class HouseSpider(RedisSpider):
    name = 'housespider'
    redis_key = 'housespider:urls'
    start_urls = 'http://sh.lianjia.com/ershoufang/'

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
        headers = {'User-Agent': user_agent}
		
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
  #       print area_list

        for URL in self.url_list :
            theURL = self.start_urls + URL
            yield scrapy.Request(url=theURL, headers=headers, method='GET', callback=self.parse)

  
    def getIntInStr(self, theStr):
        theStr1 = theStr.encode('gbk')
        theStr1 = filter(str.isdigit, theStr1)
        theStr2 = ''.join(theStr1)
        #        print(type(theStr1))
        #        print(theStr2)
        if len(theStr2) > 0:
            return int(theStr2)
        else:
            return 0

    def getFloatInStr(self, theStr):
        theStr1 = theStr.encode('gbk')
#        theStr1 = filter(str.isdigit, theStr1)
#        theStr2 = ''.join(theStr1)
        #        print(type(theStr1))
        #        print(theStr2)
        if len(theStr1) > 0:
            return float(theStr1)
        else:
            return 0.0

    def getStrInXpath(self, theList):
        if isinstance(theList, list) and len(theList) > 0:
            return theList.pop()
        return ''


    def parse2(self, response):
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
                    item = HouseItem()

                    item['link'] ="http://sh.lianjia.com" + self.getStrInXpath(house.xpath('a[1]/@href'))
                    item['house_id'] = item['link'][33:42]
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
                        item['area'] = detail[1].replace('\t','').replace('\n','').replace(' ','').replace(u'平','')
                    if (len(detail) > 2) :
                        item['tall'] = detail[2].replace('\t','').replace('\n','').replace(' ','')
                    if (len(detail) > 3) :
                        item['faceto'] = detail[3].replace('\t','').replace('\n','').replace(' ','')

                    item['area'] = self.getFloatInStr( item['area'] )


                    item['price'] = self.getFloatInStr(self.getStrInXpath(house.xpath('div[1]/div[2]/div[1]/div[1]/span[1]/text()')))
                    item['community_id'] = self.getStrInXpath(house.xpath('div[1]/div[2]/div[2]/span[1]/a[1]/@href'))[8:21]
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
       

    
    def parse(self, response):
        print 'WangJ   parse1 \n'

        
        user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.22 \
                                 Safari/537.36 SE 2.X MetaSr 1.0'
        headers = {'User-Agent': user_agent}
        
#        print response.request.meta
        
        
        lists = response.body.decode('utf-8')
        selector = etree.HTML(lists)
#        area_list = selector.xpath('/html/body/div[1]/div[2]/div[3]/div[1]/div[2]/a[@gahref="results_totalpage"]')
        area_list = selector.xpath('/html/body/div[1]/div[2]/div[3]/div[1]/div[2]/a')
        print area_list
        print len(area_list)
        pages = 1
        if len(area_list) > 1 :
            print area_list[len(area_list) - 2].text
            pages = int( area_list[len(area_list) - 2].text )
        print pages

        for i in range( 0, pages ):
            print i+1
            area_url = '{}/d{}/'.format(response.request.url,i+1)
            print area_url
            yield scrapy.Request(url=area_url, headers=headers, callback=self.parse2 )
        
        
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
                        item = HouseItem()
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
                        yield item
                    except Exception:
                        pass

            except Exception:
                    pass