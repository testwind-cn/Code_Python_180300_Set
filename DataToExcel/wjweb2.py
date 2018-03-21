# -*- coding: utf-8 -*-
# 第一行必须有，否则报中文字符非ascii码错误
import os
import cgi
import requests
import random
from lxml import etree


from pymongo import MongoClient
import time
#
import os  # Python的标准库中的os模块包含普遍的操作系统功能
import re  # 引入正则表达式对象
import urllib  # 用于对URL进行编解码
from BaseHTTPServer import HTTPServer
from SimpleHTTPServer  import SimpleHTTPRequestHandler  # 导入HTTP处理相关的模块


# 自定义处理程序，用于处理HTTP请求
class TestHTTPHandler(SimpleHTTPRequestHandler):
    baseURL = 'https://ipcrs.pbccrc.org.cn'
    # 处理GET请求
    def do_GET1(self):
        # 页面输出模板字符串
        templateStr = '''   
<html>   
<head>   
<title>ren hang</title>   
</head>   
<body>   
%s 
<br>   
<br>   
<form action="/qr" name=f method="GET"><input maxLength=1024 size=70   
name=s value="" title="Text to QR Encode"><input type=submit   
value="Show QR" name=qr>   
</form> 
</body>   
</html> '''



        self.protocal_version = 'HTTP/1.1'  # 设置协议版本
        self.send_response(200)  # 设置响应状态码
        self.send_header("Welcome", "Contect")  # 设置响应头
        self.end_headers()
        self.wfile.write(templateStr % qrImg)  # 输出响应内容

    def my_send_head(self, content, code=200, type="text/html"):
        self.send_response(code)
        self.send_header("Content-Type", type)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()

    def do_return_img(self):
        SimpleHTTPRequestHandler.do_GET(self)

    def reload_img(self):

        print self.path

    def do_reload_img(self):
        aaa = self.path.split('?')[-1]
        for key_value in aaa.split('&'):
            key_name = key_value.split('=')[0]
            if  key_name == 'TSf75e5b':
                TSf75e5b = urllib.unquote(key_value.split('=')[1])
            if key_name == 'a':
                tdate = key_value.split('=')[1]
            if key_name == 'JSESSIONID':
                jsID = urllib.unquote(key_value.split('=')[1])
            if key_name == 'BIGipServerpool_ipcrs_app':
                ipcrs_app = urllib.unquote(key_value.split('=')[1])
            if key_name == 'BIGipServerpool_ipcrs_web':
                ipcrs_web = urllib.unquote(key_value.split('=')[1])
            print key_value.split('=')

        imgsrc = '/imgrc.do?a='+tdate + ';'
        self.do_download_img(imgsrc ,tdate,jsID, TSf75e5b,ipcrs_app,ipcrs_web)
        self.path = '/img_'+tdate+'_'+TSf75e5b+'.jpg'
        SimpleHTTPRequestHandler.do_GET(self)

    def do_download_img(self,imgsrc,tdate, jsID, TSf75e5b,ipcrs_app, ipcrs_web):
        user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.22 \
                                                Safari/537.36 SE 2.X MetaSr 1.0'
        headers = {'User-Agent': user_agent, 'Referer': 'https://ipcrs.pbccrc.org.cn/page/login/loginreg.jsp',
                   'Host': 'ipcrs.pbccrc.org.cn'}
        cookies = requests.cookies.RequestsCookieJar()
        cookies.set('JSESSIONID', jsID, path='/', domain='ipcrs.pbccrc.org.cn')
        cookies.set('TSf75e5b', TSf75e5b, path='/', domain='ipcrs.pbccrc.org.cn')
        cookies.set('BIGipServerpool_ipcrs_app', ipcrs_app, path='/', domain='ipcrs.pbccrc.org.cn')
        cookies.set('BIGipServerpool_ipcrs_web', ipcrs_web, path='/', domain='ipcrs.pbccrc.org.cn')

        resp = requests.get(self.baseURL + imgsrc, headers=headers, cookies=cookies, verify=False)
        print resp
        imgFile = open('D:\img_' + tdate + '_' + TSf75e5b + '.jpg', 'wb')
        imgFile.write(resp.content)
        imgFile.close()

    def do_GET(self):
        ssss = self.path[0:5]
        if ssss == '/img_' :
            self.do_return_img()
        else:
            self.process(2)
#path = '/login?dd=123&dds=232'
    def do_POST(self):
        self.process(1)

    def real_login(self,date,jsID,TSf75e5b,ipcrs_app,ipcrs_web, loginname,password,IMGRC ):
        user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.22 \
                                                                        Safari/537.36 SE 2.X MetaSr 1.0'
        headers = {'User-Agent': user_agent, 'Referer': 'https://ipcrs.pbccrc.org.cn/page/login/loginreg.jsp',
                   'Host': 'ipcrs.pbccrc.org.cn'}
        cookies = requests.cookies.RequestsCookieJar()

        cookies.set('JSESSIONID', jsID, path='/', domain='ipcrs.pbccrc.org.cn')
        cookies.set('TSf75e5b', TSf75e5b, path='/', domain='ipcrs.pbccrc.org.cn')
        cookies.set('BIGipServerpool_ipcrs_app', ipcrs_app, path='/', domain='ipcrs.pbccrc.org.cn')
        cookies.set('BIGipServerpool_ipcrs_web', ipcrs_web, path='/', domain='ipcrs.pbccrc.org.cn')

        d = {'method': 'login', 'date': date, 'loginname':loginname , 'password':password ,'_@IMGRC@_':IMGRC }
        resp = requests.post(self.baseURL + '/login.do', data=d, headers=headers, cookies=cookies, verify=False)
        #    cookies = resp.cookies
        #  cookies u'少了'
        print resp.content.decode('gbk')

    def process_myLogin(self):
        print self.path
        form = cgi.FieldStorage(
            fp=self.rfile,
            headers=self.headers,
            environ={'REQUEST_METHOD': 'POST',
                     'CONTENT_TYPE': self.headers['Content-Type'],
                     })

        IMGRC = form['_@IMGRC@_'].value

        password = form['password'].value
        loginname = form['loginname'].value
        date = form['date'].value
        jsID = form['JSESSIONID'].value
        TSf75e5b = form['TSf75e5b'].value
        ipcrs_app = form['BIGipServerpool_ipcrs_app'].value
        ipcrs_web = form['BIGipServerpool_ipcrs_web'].value

        return self.real_login(date,jsID, TSf75e5b, ipcrs_app,ipcrs_web, loginname, password, IMGRC)

    def process(self, type):
        if type == 2 :
            if self.path[0:8] == '/myimgrc':
                self.do_reload_img()
                return
            if self.path[0:6] == '/Tools':
                SimpleHTTPRequestHandler.do_GET(self)
                return
            if self.path[0:6] == '/':
                self.firstPage()
                return
        else:
            if self.path[0:8] == '/mylogin':
                self.process_myLogin()
                return


    def get_cookie_value(self,cookies,name):
        for i in cookies:
            print i.name.upper() + i.value
            if i.name.upper() == name.upper():
                return i.value
        return ''


    def MyloginPage(self):
        'http://bj.lianjia.com/ershoufang/dongcheng/pg2/'

        user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.22 \
                                        Safari/537.36 SE 2.X MetaSr 1.0'
        headers = {'User-Agent': user_agent}


        url = self.baseURL + '/page/login/loginreg.jsp'
        resp = requests.get(url, headers=headers, verify=False)
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

        jsID = self.get_cookie_value(cookies, 'JSESSIONID')
        print jsID

        TSf75e5b = self.get_cookie_value(cookies, 'TSf75e5b')
        print TSf75e5b

        ipcrs_app = self.get_cookie_value(cookies, 'BIGipServerpool_ipcrs_app')
        print ipcrs_app

        ipcrs_web = self.get_cookie_value(cookies, 'BIGipServerpool_ipcrs_web')
        print ipcrs_web

        imgsrc = contents.xpath('/html/body/div[2]/div[1]/div[1]/form[1]/div[4]/div[2]/img/@src').pop()
        print imgsrc
        self.do_download_img(imgsrc,tdate, jsID, TSf75e5b,ipcrs_app, ipcrs_web)
#        cookies.set('TSf75e5b', '1', path='/', domain='ipcrs.pbccrc.org.cn')
#        cookies.clear( domain='ipcrs.pbccrc.org.cn',path='/', 'TSf75e5b' )





        return tdate +','+ jsID+ ','+ TSf75e5b+','+ ipcrs_app+','+ipcrs_web

    



    def firstPage(self):
        tdate_tkey = self.MyloginPage()
        tdate = tdate_tkey.split(',')[0]
        jsID = tdate_tkey.split(',')[1]
        TSf75e5b = tdate_tkey.split(',')[2]
        ipcrs_app = tdate_tkey.split(',')[3]
        ipcrs_web = tdate_tkey.split(',')[4]

        sss = ' '
        # 页面输出模板字符串
        templateStr = u'''   
    <html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
		<title>个人信用信息服务平台  </title>
		
<script type="text/javascript" src="https://ipcrs.pbccrc.org.cn/js/input_notice.js"></script>
<script type="text/javascript" src="https://ipcrs.pbccrc.org.cn/page/login/js/loginreg.js"></script>  
</head>

<body>
    <!--页面主体-->

    <div id="content">
        <div id="main">
            
            <div class="leftBar left">
                <div style=" height:36px; line-height:36px; color:#202020; border-bottom:1px #c11000 solid;">
                  <span style="font-size:16px; font-weight:bold; float:left">用户登录</span>
                </div>
                
                
                <form name="loginForm" method="post" action="/mylogin" onsubmit="return check(this)"> 
                <input type="hidden" name="method" value="login"/>
                <input type="hidden" name="date" value="'''+  tdate +  u'''"/>
                <input type="hidden" name="JSESSIONID" value="'''+  jsID +  u'''"/>
                <input type="hidden" name="TSf75e5b" value="'''+  TSf75e5b +  u'''"/>
                <input type="hidden" name="BIGipServerpool_ipcrs_app" value="'''+  ipcrs_app +  u'''"/>
                <input type="hidden" name="BIGipServerpool_ipcrs_web" value="'''+  ipcrs_web +  u'''"/>
                	<div class="erro_div3">
		      			<span>
			      			
		                   	
	                   	</span>
	    			</div>
                   <div class="form-group">
                       <div class="lable lable1">登录名：</div>
                       <div class="inputBox">
                           <input type="text" maxlength="16" name="loginname" id="loginname" class="inputText text1 div_left" onfocus="displayYes(this);" onblur="displayNo(this);checkLoginName();"/>
                           <span class="notice1 span-grey">请输入登录名</span>
                           <span class="span-grey" id="loginNameInfo"></span>
                       </div>	                        
                       
                   </div>
                   <div class="form-group">
                       <div class="lable lable1">密&nbsp; 码：</div>
                       <div class="inputBox position_re">
                           <input type="password" maxlength="20" name="password" id="password" class="inputText text1 div_left" onfocus="displayYes(this);"  onblur="displayNo(this);checkPassword();" />
                           
                           <span class="notice1 span-grey">请输入登录密码</span>
                           <span class="span-grey" id="passwordInfo"></span>
                       </div>
                       <div class="login_forget"><a href="https://ipcrs.pbccrc.org.cn/resetPassword.do?method=init" target="_self">忘记密码？</a></div>
                   </div>
                   <div class="form-group">
                       <div class="lable lable1">验证码：</div>
                       <div>
                       		<input class = "inputText text3 left" maxLength="6" type="text" id="_@IMGRC@_" name="_@IMGRC@_" onfocus="if(document.getElementById('_@MSG@_')!=null)document.getElementById('_@MSG@_').innerHTML='';">
<img src="/img_'''+ tdate+ '_' + TSf75e5b +u'''.jpg" id="imgrc" class = "yzm_img"/>
<a class = "yzm_a" href="###" onclick="document.getElementById('imgrc').src='/myimgrc?a='+new Date().getTime()+'&JSESSIONID='''+ urllib.quote(jsID) + '&TSf75e5b=' + urllib.quote(TSf75e5b)+ '&BIGipServerpool_ipcrs_app=' +urllib.quote(ipcrs_app)+ '&BIGipServerpool_ipcrs_web=' +urllib.quote(ipcrs_web) + u''''">看不清，换一个</a>
<script>document.getElementById('imgrc').style.display='';</script>

                       		<span class="span-grey" id="imageCodeInfo" style="width:110px;"></span>
                       	</div>
                       <div style="clear:both"></div>
                   </div>
                   <div class="form-group mar_top_30">
                       <div class="lable lable1"></div>
                       <div class="inputBox">
                          	<input type="submit" class="inputBtn btn2" value="登录"/>
                       </div>
                       
                   </div>
                   </form>
            </div>


</body>
</html>
'''

        #        sss = templateStr.encode('utf-8') + sss
        sss = templateStr + sss
#              + str(int( time.time() * 1000))


        self.protocal_version = 'HTTP/1.1'  # 设置协议版本
        self.send_response(200)  # 设置响应状态码
        self.send_header("Welcome", "Contect")  # 设置响应头
        self.end_headers()
        oweb = sss.encode('utf-8')
        self.wfile.write(oweb)  # 输出响应内容


# 启动服务函数
def start_server(port):
#    os.setcwd()
    os.chdir("D:/" )
    http_server = HTTPServer(('', int(port)), TestHTTPHandler)
    http_server.serve_forever()  # 设置一直监听并接收请求


# os.chdir('static')  # 改变工作目录到 static 目录
start_server(8080)  # 启动服务，监听8000端口