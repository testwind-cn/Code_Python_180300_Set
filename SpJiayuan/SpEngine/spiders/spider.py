# -*- coding: utf-8 -*-
import json
import logging
import re

import requests
import scrapy
from lxml import etree
from scrapy.http import FormRequest
from scrapy_redis.spiders import RedisSpider

from SpEngine.items_see import TheItem_See
# from SpEngine.spiders.TheWjCookie import TheWjCookie
from .TheWjCookie import TheWjCookie
from ..items import TheItem



class TheSpider(RedisSpider):
    name = 'wjSpider'
    redis_key = 'wjspiderKey:urls'

    start_urls = ['https://www.anjuke.com/fangjia/shanghai2018/']

    handle_httpstatus_list = [301, 302]

    # 为了模拟浏览器，我们定义httpheader
    post_headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.8,en;q=0.6",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.75 Safari/537.36",
        "Referer": "https://github.com/",
    }




    def start_requests(self):
        logging.debug("\033[1;32;47m WangJ := \033[0m"  'start_requests\n')
        user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.22 \
                         Safari/537.36 SE 2.X MetaSr 1.0'
        headers = {'User-Agent': user_agent}  # ,  'allow_redirects' : 'True' }

        logging.debug("\033[1;32;47m WangJ := \033[0m" "处理待爬首页面列表")

        # 访问 "http://login.jiayuan.com/"，
        # 取得  SESSION_HASH = b0921dd4168f225727af3abc4cb84f19321e634f
        # 取得  PHPSESSID = dd2f8b660b1b4f20a1befb767da85267
        res = requests.get('http://login.jiayuan.com/',  headers=headers)

        # 生成新的 CookieList,读取 PHPSESSID
        # dict( {'name':'PHPSESSID','value':'dd2f8b660b1b4f20a1befb767da85267'} )
        # 设置  '_s_x_id' ：dd2f8b660b1b4f20a1befb767da85267
        newCookie = TheWjCookie.createFromSimpelCookie(res.cookies)
        CookieList = newCookie.getCookieList()
        aPHPSESSID = newCookie.getCookieValue("PHPSESSID")



        # 用 cookie 和 Form 表单，生成新请求
        # url="https://passport.jiayuan.com/dologin.php?pre_url=http://www.jiayuan.com/usercp/"
        # url="https://passport.jiayuan.com/dologin.php?pre_url=http://usercp.jiayuan.com/?from=login"
        aaa = scrapy.FormRequest(url="https://passport.jiayuan.com/dologin.php?pre_url=http://usercp.jiayuan.com/",
                                 formdata={
                                     'name': 'wind_999@sina.cn',
                                     'password': '111111dd',
                                     '_s_x_id': aPHPSESSID,#  'dd2f8b660b1b4f20a1befb767da85267',
                                     'ljg_login': '1',
                                     'm_p_l': '1',
                                     'channel': '1',
                                     'position': '0'
                                 },
                                 headers=headers, method='POST', cookies=CookieList, callback=self.parse_1)


        yield aaa



    def getChildPageRequests(self, response, deep):
        user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.22 \
                                         Safari/537.36 SE 2.X MetaSr 1.0'
        headers = {'User-Agent': user_agent}

        sText = ""
        theURL = ""

        lists = response.body.decode('utf-8')
        selector = etree.HTML(lists)

        # 返回页面： 内容：cookie , script跳转：<script type='text/javascript'>top.location.href='http://login.jiayuan.com/jump/?cb=R4kyv2gwYkdZc4DqlL-qmf11d-xMV0GgMCNdFYIitYajes4CwQ2Wi9R9bXWkfRMYZytSHEUNtU8xC*U5fAMmMfVpHJUsbHfNLQATNc0H3Z7ONtyJQqZ15C5bP2SNgRXBAp880I4SA2VhMSwnU7pw-Gku5S51Bs67AiWpZb0jzL25qyxB1gY-rXx1dEg.';</script>
        if deep == 1:
            tStr = lists
            p1 = tStr.find('href=') + 6
            p2 = tStr.find('\'', p1)
            theURL = tStr[p1:p2]

        # 返回页面： 内容：有：页面超时跳转：location='http://www.jiayuan.com/usercp/?from=login'
        if deep == 11:
            dList = selector.xpath("/html/body/script[2]/text()")
            tStr = dList[0]
            p1 = tStr.find('location=') + 10
            p2 = tStr.find('\'', p1)
            theURL = tStr[p1:p2]

        # 返回页面： 子页：http://www.jiayuan.com/usercp/clicked_new.php
        if deep == 111:
            theURL ="http://www.jiayuan.com/usercp/clicked_new.php"


        # 读取响应的新cookie
        oriRespCookie = response.headers.getlist('Set-Cookie')
        newRespCookie  =  TheWjCookie.createFromRespCookie( oriRespCookie)
        # 读取请求的旧cookie
        reqReqsCookie = TheWjCookie.createFromReqsCookie(response.request.cookies)
        # 在旧的请求cookie 中，加入新的cookie
        reqReqsCookie.updateCookie( newRespCookie )
        print(reqReqsCookie)
        # 获取 TheWjCookie 类里面的真实cookie列表
        cookieList = reqReqsCookie.getCookieList()


        logging.debug("\033[1;32;47m WangJ := \033[0m" "parse-%d-add-1 生成新的待爬页面 %s : %s" % (deep, sText, theURL))

        if deep == 1:
            yield scrapy.Request(url=theURL, headers=headers, method='GET', cookies=cookieList,  callback=self.parse_11)

        if deep == 11:
            yield scrapy.Request(url=theURL, headers=headers, method='GET', cookies=cookieList,  callback=self.parse_111)

        if deep == 111: # 添加页面：谁看过我
            theURL = "http://www.jiayuan.com/usercp/clicked_new.php"
            yield scrapy.Request(url=theURL, headers=headers, method='GET', cookies=cookieList,  callback=self.parse_1111)
            theURL = "http://www.jiayuan.com/usercp/clicked_new.php?p=2"
            yield scrapy.Request(url=theURL, headers=headers, method='GET', cookies=cookieList,  callback=self.parse_1111)



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

    def getWhoSeeMe(self, selector):

        readText1 = selector.xpath(
            '/html/body/div[1]/div[4]/div[2]/div[1]/div[5]/ul[1]/li/div[@class=\'pic\']/a/img/@src')
        print(readText1)

        readText2 = selector.xpath(
            '/html/body/div[1]/div[4]/div[2]/div[1]/div[5]/ul[1]/li/div[@class=\'user_info\']/a/text()')
        print(readText2)

        readText3 = selector.xpath('/html/body/div[1]/div[4]/div[2]/div[1]/div[5]/ul[1]/li/div[@class=\'date\']/text()')
        print(readText3)


        readText4 = selector.xpath(
            '/html/body/div[1]/div[4]/div[2]/div[1]/div[5]/ul[1]/li/div[@class=\'check_zl\']/a/@onclick')
        print(readText4)



        # num = 四个list 中最大的一个
        num = len(readText1)
        b = len(readText2)
        if ( b > num ) : num = b
        b = len(readText3)
        if (b > num): num = b
        b = len(readText4)
        if (b > num): num = b



        for i in range(num):
            item = TheItem_See()
            if i < len(readText1):
                item['pic'] = readText1[i]
            if i < len(readText2):
                item['user_info'] = readText2[i]
            if i < len(readText3):
                item['date'] = readText3[i].replace('\n','').replace('\r','').replace('\t','')
            if i < len(readText4):
                text = readText4[i]
                item['check_zl'] = text[13:(len(text)-3)]
            yield item




    def getItemOnPage(self, response, deep):
        # 本例子中,顶部 crumb 导航中的 deep 1,11,111 的处理方式一样,否则就要按deep来不同处理

        lists = response.body.decode('utf-8')
        selector = etree.HTML(lists)

        # 安居客读取html里面的script文本，其中包含图表数据
        # readText = selector.xpath('/html/body/script[8]')[0].text

        # 生成当前页面的数据ITEM
        itemList = self.getWhoSeeMe(selector)
        for item in itemList:
            yield item



    def parse_1(self, response):
        # 第一层级 1，登录form；第二层级11，登录中转前； 第三层级111，登录后userg
        # 请求地址： https://passport.jiayuan.com/dologin.php?pre_url=http://www.jiayuan.com/usercp/
        # 请求地址： https://passport.jiayuan.com/dologin.php?pre_url=http://usercp.jiayuan.com/
        # 返回页面： 子页：有：http://login.jiayuan.com/jump/?cb=R4kyv2gwY
        # 返回页面： 内容：cookie , script跳转：<script type='text/javascript'>top.location.href='http://login.jiayuan.com/jump/?cb=R4kyv2gwYkdZc4DqlL-qmf11d-xMV0GgMCNdFYIitYajes4CwQ2Wi9R9bXWkfRMYZytSHEUNtU8xC*U5fAMmMfVpHJUsbHfNLQATNc0H3Z7ONtyJQqZ15C5bP2SNgRXBAp880I4SA2VhMSwnU7pw-Gku5S51Bs67AiWpZb0jzL25qyxB1gY-rXx1dEg.';</script>
        # 返回页面： 元素：无
        deep = 1
        logging.debug("\033[1;32;47m WangJ := \033[0m" "parse-%d-Handle 登录首页 %s" % (deep, response.url))  # 处理安居客shanghai2018

        # # 生成当前页面的数据ITEM
        # results = self.getItemOnPage(response, deep)
        # for aItem in results:
        #     yield aItem

        # 搜索子页面
        results = self.getChildPageRequests(response, deep)
        for aReq in results:
            logging.debug("\033[1;32;47m WangJ := \033[0m" "parse-%d-add-2 加入新的待爬页面 %s " % (deep, aReq.url))
            yield aReq

    def parse_11(self, response):
        # 第一层级 1，登录form；第二层级11，登录中转前； 第三层级111，登录后userg
        # 请求地址： http://login.jiayuan.com/jump/?cb=tzDo9jpk2Vh6-3Uj-zRoHYzmb3rON6Nz00bcARuxVcB3GF2OcIgDVQyeyUf3V1gHA370z5PHdzmnfe43Km*kuxXo14zJIgQpMDdd2kIOLathxGI4D0sXsBkvG8WcQ33JTsttB5Uk3aCW3YqbOXwA0OC*U27e6zDCMOJZcypJoDTZGweraQP3KsTFXUMavUM.
        # 返回页面： 子页：有：http://www.jiayuan.com/usercp/?from=login
        # 返回页面： 内容：有：页面超时跳转：location='http://www.jiayuan.com/usercp/?from=login'
        # 返回页面： 元素：无

        deep = 11
        logging.debug("\033[1;32;47m WangJ := \033[0m" "parse-%d-Handle 处理登录中转跳转前页面 %s" % (deep, response.url))  # 处理安居客shanghai2018

        # # 生成当前页面的数据ITEM
        # results = self.getItemOnPage(response, deep)
        # for aItem in results:
        #     yield aItem

        # 搜索子页面
        results = self.getChildPageRequests(response, deep)
        for aReq in results:
            logging.debug("\033[1;32;47m WangJ := \033[0m" "parse-%d-add-2 加入新的待爬页面 %s " % (deep, aReq.url))
            yield aReq


    def parse_111(self, response):
        # 第一层级 1，上海；第二层级11，浦东； 第三层级111，碧云
        # 请求地址： http://www.jiayuan.com/usercp/?from=login
        # 请求地址： http://usercp.jiayuan.com/?from=login
        # 返回页面： 子页：http://www.jiayuan.com/usercp/clicked_new.php
        # 返回页面： 子页：http://www.jiayuan.com/usercp/clicked_new.php?p=2
        # 返回页面： 内容：无，特殊添加的子页面
        # 返回页面： 元素：无
        deep = 111
        logging.debug("\033[1;32;47m WangJ := \033[0m" "parse-%d-Handle 处理登录后userg页面 %s" % (deep, response.url))  # 处理安居客shanghai2018

        # # 生成当前页面的数据ITEM
        # results = self.getItemOnPage(response, deep)
        # for aItem in results:
        #     yield aItem

        # 搜索子页面
        results = self.getChildPageRequests(response, deep)
        for aReq in results:
            logging.debug("\033[1;32;47m WangJ := \033[0m" "parse-%d-add-2 加入新的待爬页面 %s " % (deep, aReq.url))
            yield aReq

    def parse_1111(self, response):
        # 第一层级 1，上海；第二层级11，浦东； 第三层级111，碧云
        # 请求地址： http://www.jiayuan.com/usercp/clicked_new.php
        # 请求地址： http://www.jiayuan.com/usercp/clicked_new.php?p=2
        # 返回页面： 子页：无
        # 返回页面： 内容：谁看过我
        # 返回页面： 元素：有 Item WhoSeeMe

        deep = 1111
        logging.debug("\033[1;32;47m WangJ := \033[0m" "parse-%d-Handle 处理登录后userg页面 %s" % (deep, response.url))  # 处理安居客shanghai2018

        # 生成当前页面的数据ITEM
        results = self.getItemOnPage(response, deep)
        for aItem in results:
            yield aItem
