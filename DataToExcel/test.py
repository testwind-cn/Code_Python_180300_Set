# -*- coding: utf-8 -*- 
# 第一行必须有，否则报中文字符非ascii码错误
import urllib
import hashlib

# 以get请求为例http://api.map.baidu.com/geocoder/v2/?address=百度大厦&output=json&ak=yourak
#  queryStr = '/geocoder/v2/?address=百度大厦&output=json&ak=yourak'
queryStr = '/api?v=2.0&ak=cmKDgPoQtEZEXhViAUCz12VK'

# 对queryStr进行转码，safe内的保留字符不转换
encodedStr = urllib.quote(queryStr, safe="/:=&?#+!$,;'@()*[]")

# 在最后直接追加上yoursk
rawStr = encodedStr + '6Hn7lnVlzMos9rZFyhGRlP2FuZ2KMpv3'

# md5计算出的sn值7de5a22212ffaa9e326444c75a58f9a0
# 最终合法请求url是http://api.map.baidu.com/geocoder/v2/?address=百度大厦&output=json&ak=yourak&sn=7de5a22212ffaa9e326444c75a58f9a0
print hashlib.md5(urllib.quote_plus(rawStr)).hexdigest()