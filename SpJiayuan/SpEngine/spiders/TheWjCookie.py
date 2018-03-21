# -*- coding: utf-8 -*-

import requests
import re

# http://blog.chinaunix.net/uid-26602509-id-3087296.html
# python --类方法、对象方法、静态方法


# class Person:
#     grade = 1
#
#     def __init__(self, name):
#         self.name = name
#
#     def sayHi(self):  # 加self区别于普通函数
#         print
#         'Hello, your name is?', self.name
#
#     @staticmethod  # 声明静态，去掉则编译报错;还有静态方法不能访问类变量和实例变量
#     def sayName():  # 使用了静态方法，则不能再使用self
#         print
#         "my name is king"  # ,grade,#self.name
#
#     @classmethod  # 类方法
#     def classMethod(cls):
#         print("class method")
#
#
# p = Person("king")
# p.sayHi()
# p.sayName()
# p.classMethod()
# Person.classMethod()
#
# 输出：
# Hello, your
# name is? king
# my
# name is king
#
#
# class method
# class method

# cookie = CK.SimpleCookie(newCookie)

class TheWjCookie:
    __CookieList = [] ## 空列表

    def __init__(self):
        self.__CookieList = []

    def __init__(self, alist ):
        self.__CookieList = []
        if isinstance(alist, list):
            self.__CookieList = alist.copy()

    def __str__(self):
        return str(self.__CookieList)

    def getCookieList(self):
        return self.__CookieList.copy()

    def updateCookie(self , newRespCookie):

        newCookie = newRespCookie
        newList = newCookie.getCookieList()

        for aCookie in newList:
            tName = aCookie['name']
            # 删除掉已经存在 bCookie
            for bCookie in self.__CookieList:
                if ( bCookie['name'] == tName ):
                    self.__CookieList.remove(bCookie )
            # 添加新的 aCookie
            self.__CookieList.append(aCookie)

        # return newCookie

    def getCookieValue(self, keyName):

        # aLen = len(self.__CookieList)
        # aDict.has_key(keyName)
        result = ""

        for bCookie in self.__CookieList:
            theName = bCookie.get("name", "")
            if ( theName == keyName):
                result = bCookie.get("value", "")

        return result

    @staticmethod
    def createFromReqsCookie(oriCookie):
        # oriCookie = response.request.cookies
        # 输入: 从请求数据里获得的cookie
        # 输出: List ，每个元素是 Dict

        if isinstance(oriCookie, dict): # 暂时不能解析 dict ，后续补充
            newCookie = TheWjCookie()

        if isinstance(oriCookie, list):
            newCookie = TheWjCookie(alist = oriCookie)

        return newCookie

    @staticmethod
    def createFromSimpelCookie(oriCookie):
        # res = requests.get('http://login.jiayuan.com/',  headers=headers)
        # oriCookie = res.cookies  #类型是 RequestsCookieJar
        # oriCookie.keys()        # oriCookie.values()
        # 输入: 从请求数据里获得的cookie
        # 输出: List ，每个元素是 Dict
        # 读取 SESSID , 生成新的 CookieList
        # dict( {'name':'PHPSESSID','value':'c3f80da0d257a088acb87f14b49bee4f'} )

        keyList = oriCookie.keys() # !!!!!! 注意修改
        valueList = oriCookie.values() # !!!!!! 注意修改
        aLen = len(keyList)
        bLen = len(valueList)

        aCookieList = []
        for i in range(aLen):
            if i < bLen:
                dictStr = "dict({\'name\':\'" + keyList[i] + "\',\'value\':\'" + valueList[i] + "\'})"
                newDict = eval(dictStr)
                aCookieList.append(newDict)

        newCookie = TheWjCookie(alist=aCookieList)
        return newCookie


    @staticmethod  # 声明静态，去掉则编译报错;还有静态方法不能访问类变量和实例变量
    def createFromRespCookie(oriCookie):  # 使用了静态方法，则不能再使用self

        # oriCookie = response.headers.getlist('Set-Cookie')
        # 输入: 从响应数据里获得的cookie ，是list ,里面是 bytes
        # 输出: List ，每个元素是 Dict

        aCookieList = []

        # for sByte in oriCookie:
        #     sText = sByte.decode(encoding="utf-8")
        #     print(sText)
        #     # 去掉;后面的空格
        #     sText = re.sub(r';[ ]{1,}', ";", sText)
        #     print(sText)
        #     # 用分号把分成List
        #     sList = sText.split(';')
        #     # 把List中每组数据用=分开，得到的ckPair前面是name，后面是value
        #     for ckData in sList:
        #         ckPair = ckData.split('=')

        for sByte in oriCookie:
            sText = sByte.decode(encoding="utf-8")
            # print(sText)

            # 去掉;后面的空格
            sText = re.sub(r';[ ]{1,}', ";", sText)
            # 去掉;前面的空格
            sText = re.sub(r'[ ]{1,};', ";", sText)
            # print(sText)
            # 去掉头部的空格
            sText = re.sub(r'^[ ]{1,}', "", sText)
            # 去掉尾部的空格
            sText = re.sub(r'[ ]{1,}$', "", sText)

            # 按第二个目标
            # dict({a: 1, b: 2, c: 3})
            # dict = {'Name': 'Zara', 'Age': 7, 'Class': 'First'};

            # PHPSESSID=c3f80da0d257a088acb87f14b49bee4f;path=/
            # 把第一等号，变成  =  ->  ;value
            sText = sText.replace("=", ";value=", 1)
            # PHPSESSID;value=c3f80da0d257a088acb87f14b49bee4f;path=/

            # 最前面加上 name=
            sText = "name=" + sText
            # name=PHPSESSID;value=c3f80da0d257a088acb87f14b49bee4f;path=/

            # 等号变成  =  -> ':'
            sText = sText.replace("=", "\':\'")
            # name':'PHPSESSID;value':'c3f80da0d257a088acb87f14b49bee4f;path':'/

            # 把分号变成逗号和单引号   ; -> ','
            sText = sText.replace(";", "\',\'")
            # name':'PHPSESSID','value':'c3f80da0d257a088acb87f14b49bee4f','path':'/

            # 前后加引号括号  {'   '}
            dictStr = "dict( { \'" + sText + "\' } )"
            # dict( {'name':'PHPSESSID','value':'c3f80da0d257a088acb87f14b49bee4f','path':'/'} )

            newDict = eval(dictStr)
            aCookieList.append(newDict)

        newCookie = TheWjCookie( alist = aCookieList)

        return newCookie
