# -*- coding: utf-8 -*-
# 第一行必须有，否则报中文字符非ascii码错误
from pymongo import MongoClient
import time
#
import os  # Python的标准库中的os模块包含普遍的操作系统功能
import re  # 引入正则表达式对象
import urllib  # 用于对URL进行编解码
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler  # 导入HTTP处理相关的模块


# 自定义处理程序，用于处理HTTP请求
class TestHTTPHandler(BaseHTTPRequestHandler):
    # 处理GET请求
    def do_GET1(self):
        # 页面输出模板字符串
        templateStr = '''   
<html>   
<head>   
<title>QR Link Generator</title>   
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


        # 将正则表达式编译成Pattern对象

        pattern = re.compile(r'/qr\?s=([^\&]+)\&qr=Show\+QR')
        # 使用Pattern匹配文本，获得匹配结果，无法匹配时将返回None
        match = pattern.match(self.path)
        qrImg = ''

        if match:
        # 使用Match获得分组信息
            qrImg = '<img src="http://chart.apis.google.com/chart?chs=300x300&cht=qr&choe=UTF-8&chl=' + match.group(1) + '" /><br />' + urllib.unquote(match.group(1))

        self.protocal_version = 'HTTP/1.1'  # 设置协议版本
        self.send_response(200)  # 设置响应状态码
        self.send_header("Welcome", "Contect")  # 设置响应头
        self.end_headers()
        self.wfile.write(templateStr % qrImg)  # 输出响应内容

    def do_GET(self):
        client = MongoClient('localhost', 27017)
        db = client['lianjia']

        #    sDate = settings['MONGODB_DOCNAME'] + time.strftime('%Y%m%d', time.localtime(time.time()))
        #    coll = db['house_20170712']
        sDate = 'xiaoqu' + time.strftime('%Y%m%d', time.localtime(time.time()))
        sDate = 'xiaoqu_20170713_M1'
        coll = db[sDate]

        sss = u'''
        <script type="text/javascript">  
            var xiaoqu = new Array(''' + str( coll.count() ) + ''');
            var xiaoquNum = ''' + str( coll.count() ) + ''';
        '''

        i=0
        for obj in coll.find():
#            p1.lat = float(obj['m_dist'])
#            p1.lng = float(obj['gps_x'])
            sss = sss + 'xiaoqu[' + str( i) + '''] = new Array("'''+ obj['name']+ '''",'''+ obj['gps_x'] +','+obj['gps_y']+','+ str( obj['m_dist']) +''');
            '''
            i = i + 1
        sss = sss + '''
        </script>
        '''

        # 页面输出模板字符串
        templateStr = u'''   
    <html>
<head>
	<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
	<meta name="viewport" content="initial-scale=1.0, user-scalable=no" />
	<style type="text/css">
		#allmap {width: 1100;height: 800;float:left;margin:10;font-family:"微软雅黑";}
		#thetext {width: 150;height: 800;float:left;margin:10;font-family:"微软雅黑";}
	</style>
	<script type="text/javascript" src="http://api.map.baidu.com/api?v=2.0&ak=vS1UU40ebhE1dkncZXsM9zY1hqFGqGjA"></script>
	<title>111</title>
</head>
<body>
	<div id="allmap"></div>
<div id="thetext">
	<span id="theGPS"></span>
</div>
</body>
</html>
'''

#        sss = templateStr.encode('utf-8') + sss
        sss = templateStr + sss
        sss = sss + u'''

<script type="text/javascript">
	var num = 1;
	function addMarker(point){
	  var marker = new BMap.Marker(point);
	  map.addOverlay(marker);
	}
	
	function deletePoint(){
		var allOverlay = map.getOverlays();
		for (var i = 0; i < allOverlay.length -1; i++){
			if(allOverlay[i].getLabel().content == "我是id=1"){
				map.removeOverlay(allOverlay[i]);
				return false;
			}
		}
	}
	
	function deleteAllPoint(){
		var allOverlay = map.getOverlays();
		for (var i = allOverlay.length -1; i >=0 ; i--){
		    alert(i);
		    map.removeOverlay(allOverlay[i]);
		    
		}
//		return false;
	}
	
	function addAllPoint(){
	    for (var i = 0; i < '''+str( coll.count() ) + u'''; i++){
	        var x = xiaoqu[i][1];
		    var y = xiaoqu[i][2];
		    
		    var ggPoint = new BMap.Point(x,y);
		    alert(xiaoqu[i][1]);
		    alert(xiaoqu[i][2]);
		    addMarker(ggPoint)
		}
	}

	// 百度地图API功能
	var map = new BMap.Map("allmap");
	var sstr 
	map.addControl(new BMap.NavigationControl());    
	map.addControl(new BMap.ScaleControl());    
	map.addControl(new BMap.OverviewMapControl());    
	map.addControl(new BMap.MapTypeControl()); 
	map.enableScrollWheelZoom();   //启用滚轮放大缩小，默认禁用
	map.enableContinuousZoom();    //启用地图惯性拖拽，默认禁用
        
	map.centerAndZoom("浦东",12);           
	//单击获取点击的经纬度
	map.addEventListener("rightclick",function(e){
		//	alert(e.point.lng + "," + e.point.lat);
		addMarker(e.point);
		sstr = document.getElementById("theGPS").innerHTML;
		sstr = sstr  + "<br>" + num +":,"+ e.point.lng + "," + e.point.lat;
		document.getElementById("theGPS").innerHTML = sstr;
		num  = num  + 1;
	});
	map.addEventListener("rightdblclick",function(e){
//	    deleteAllPoint();
  //      addAllPoint();
 alert( new Date().getTime() );
	});
</script> '''


        self.protocal_version = 'HTTP/1.1'  # 设置协议版本
        self.send_response(200)  # 设置响应状态码
        self.send_header("Welcome", "Contect")  # 设置响应头
        self.end_headers()
        oweb = sss.encode('utf-8')
        self.wfile.write(oweb)  # 输出响应内容


# 启动服务函数
def start_server(port):
    http_server = HTTPServer(('', int(port)), TestHTTPHandler)
    http_server.serve_forever()  # 设置一直监听并接收请求


# os.chdir('static')  # 改变工作目录到 static 目录
start_server(8000)  # 启动服务，监听8000端口