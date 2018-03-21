# -*- coding: utf-8 -*-
import requests
import random
from lxml import etree

def detail_url():
    'http://bj.lianjia.com/ershoufang/dongcheng/pg2/'

    user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.22 \
                                    Safari/537.36 SE 2.X MetaSr 1.0'
    headers = {'User-Agent': user_agent}



    baseURL = 'https://ipcrs.pbccrc.org.cn'
    url = baseURL + '/page/login/loginreg.jsp'
    resp = requests.get(url, headers = headers , verify=False)
    cookies = resp.cookies
    print cookies
#    contents = etree.HTML(resp.content.decode('utf-8'))
    contents = etree.HTML(resp.content)

    #    mtd = contents.xpath('/html/body/div[2]/div[1]/div[1]/form[1]/input[1]/@value').pop()
    mtd = contents.xpath('''/html/body/div[2]/div[1]/div[1]/form[1]/input[@name='method']/@value''').pop()
    print mtd
    #    tdate = contents.xpath('/html/body/div[2]/div[1]/div[1]/form[1]/input[2]/@value').pop()
    tdate = contents.xpath('''/html/body/div[2]/div[1]/div[1]/form[1]/input[@name='date']/@value''').pop()
    print tdate


    imgsrc = contents.xpath('/html/body/div[2]/div[1]/div[1]/form[1]/div[4]/div[2]/img/@src').pop()
    print imgsrc
    headers = {'User-Agent': user_agent,'Referer': 'https://ipcrs.pbccrc.org.cn/page/login/loginreg.jsp','Host': 'ipcrs.pbccrc.org.cn'}
    resp = requests.get( baseURL+imgsrc, headers = headers , cookies = cookies, verify=False)
    print resp
    imgFile = open('D:\img_'+tdate +'__' +cookies['TSf75e5b'] + '.jpg', 'wb')
    imgFile.write(resp.content)
    imgFile.close()



    d = {'method': 'login', 'date': tdate ,'loginname':'acewind', 'password':'123321aC','_@IMGRC@_' : '' }
    resp = requests.post( baseURL+'/login.do', data = d, headers = headers , cookies = cookies, verify=False)
#    cookies = resp.cookies
#  cookies u'少了'
    print resp.content.decode('gbk')
    print random.random()
    d = {'method': 'send', 'verifyCode': '234567'}
    headers = {'User-Agent': user_agent, 'Referer': 'https://ipcrs.pbccrc.org.cn/reportAction.do?method=applicationReport',
               'Host': 'ipcrs.pbccrc.org.cn','Origin':'https://ipcrs.pbccrc.org.cn'}
    sendMsg ='/reportAction.do?num=0.6786053179799577'
    resp = requests.post(baseURL + sendMsg, data = d, headers=headers, cookies=cookies, verify=False)
    print resp.content

detail_url()