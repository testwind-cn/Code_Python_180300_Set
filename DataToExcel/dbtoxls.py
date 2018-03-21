# -*- coding: utf-8 -*-
from pymongo import MongoClient
import math
from test2 import getDistance
from test2 import Point

import pyExcelerator
from pyExcelerator import Formatting
import time

def delDB(sss) :
    client = MongoClient('localhost', 27017)
    db = client['lianjia']

def XiaoquToMetroSaveToDB() :
    client = MongoClient('localhost', 27017)
    db = client['lianjia']

    #    sDate = settings['MONGODB_DOCNAME'] + time.strftime('%Y%m%d', time.localtime(time.time()))
    #    coll = db['house_20170712']
    sDate = 'xiaoqu' + time.strftime('%Y%m%d', time.localtime(time.time()))
    sDate = 'xiaoqu_20170713'
    coll = db[sDate]

    p1 = Point()
    p1.lat = 37.480563
    p1.lng = 121.467113
    p2 = Point()
    p2.lat = 37.480591
    p2.lng = 121.467926

    print getDistance(p1, p2)

    sheets = pyExcelerator.parse_xls(u'E:/地铁坐标.xls')
    print sheets
#    for i in len(sheets[0][1]) / 2
    for i in range(0, len(sheets[0][1]) / 2) :
#        print  " {} : {} : {} ".format(i, sheets[0][1][(i,0)],sheets[0][1][(i,1)])
        pass

    for obj in coll.find():
        p1.lat = float(obj['gps_y'])
        p1.lng = float(obj['gps_x'])
        min_dist = 200000.0
        for i in range(0, len(sheets[0][1]) / 2):
            p2.lat = sheets[0][1][(i,1)]
            p2.lng = sheets[0][1][(i,0)]
            tmp_dist = getDistance(p1, p2)
            print " {} : {} : {} ".format(p1, p2, tmp_dist)
            if ( tmp_dist < min_dist ) :
                min_dist = tmp_dist
        obj['m_dist'] = min_dist
        print obj['m_dist']
        coll.update({'_id': obj['_id']}, {'$set': {'m_dist': obj['m_dist']}})




def saveHouseToFile() :
    wb = pyExcelerator.Workbook()
    ws = wb.add_sheet('pages1')
    myfont = pyExcelerator.Font()
    myfont.name = u'宋体'
    myfont.height = 0x00DC
    mystyle = pyExcelerator.XFStyle()
    mystyle.font = myfont

    badBG = Formatting.Pattern()
    badBG.pattern = badBG.NO_PATTERN
    badBG.pattern_back_colour  = 'red'
    mystyle.pattern = badBG

#    mystyle.num_format_str = '0'




    client = MongoClient('localhost', 27017)
    db = client['lianjia']

#    sDate = settings['MONGODB_DOCNAME'] + time.strftime('%Y%m%d', time.localtime(time.time()))
#    coll = db['house_20170712']
    sDate = 'house_' + time.strftime('%Y%m%d', time.localtime(time.time()))
    coll = db[sDate]

#  城区 板块 小区名称 户型  面积 价格 单价 楼层 朝向 房龄 小区ID 小区链接 标题 详情链接 图片  标签0 标签1 标签2

    i = 0
    ws.write(i, 0, u'城区', mystyle)
    ws.write(i, 1, u'板块', mystyle)
    ws.write(i, 2, u'小区名称', mystyle)
    ws.write(i, 3, u'户型', mystyle)
    ws.write(i, 4, u'面积', mystyle)
    ws.write(i, 5, u'价格', mystyle)
    ws.write(i, 6, u'单价', mystyle)
    ws.write(i, 7, u'楼层', mystyle)
    ws.write(i, 8, u'朝向', mystyle)
    ws.write(i, 9, u'房龄', mystyle)
    ws.write(i, 10, u'小区ID', mystyle)
    ws.write(i, 11, u'小区链接', mystyle)
    ws.write(i, 12, u'标题', mystyle)
    ws.write(i, 13, u'房源编号', mystyle)
    ws.write(i, 14, u'详情链接', mystyle)
    ws.write(i, 15, u'图片', mystyle)
    ws.write(i, 16, u'标签0', mystyle)
    ws.write(i, 17, u'标签1', mystyle)
    ws.write(i, 18, u'标签2', mystyle)

    badBG.pattern_back_colour = 'blue'
    mystyle.pattern = badBG

    for obj in coll.find():
        i = i + 1
        mystyle.num_format_str = 'general'
        ws.write(i, 0, obj['city'], mystyle)
        ws.write(i, 1, obj['town'], mystyle)
        ws.write(i, 2, obj['community_title'], mystyle)
        ws.write(i, 3, obj['model'], mystyle)
        ws.write(i, 4, obj['area'], mystyle)
        mystyle.num_format_str = '0.00'
        ws.write(i, 5, int( obj['price'].encode('gbk') ), mystyle)
        ws.write(i, 6, int( obj['average_price'].encode('gbk') ), mystyle)
        mystyle.num_format_str = 'general'
        ws.write(i, 7, obj['tall'], mystyle)
        ws.write(i, 8, obj['faceto'], mystyle)
        mystyle.num_format_str = '0'
        if len( obj['age'] ) <= 0 :
            theage = 0
        else :
            theage = int(obj['age'].encode('gbk'))
        ws.write(i, 9, theage, mystyle)
        mystyle.num_format_str = 'general'
        ws.write(i, 10, obj['community_id'], mystyle)
        ws.write(i, 11, obj['community'], mystyle)
        ws.write(i, 12, obj['title'], mystyle)
        ws.write(i, 13, obj['house_id'], mystyle)
        ws.write(i,14,obj['link'],mystyle)
        ws.write(i,15,obj['image'],mystyle)
        ws.write(i,16,obj['tag0'],mystyle)
        ws.write(i,17,obj['tag1'],mystyle)
        ws.write(i,18,obj['tag2'],mystyle)

#        print obj['_id']
#    ws.write(i,17,ss,mystyle)
    sDate = 'E:\\' + sDate + '.xls'
    wb.save(sDate)

def saveXiaoquToFile() :
    wb = pyExcelerator.Workbook()
    ws = wb.add_sheet('pages1')
    myfont = pyExcelerator.Font()
    myfont.name = u'宋体'
    myfont.height = 0x00DC
    mystyle = pyExcelerator.XFStyle()
    mystyle.font = myfont

    badBG = Formatting.Pattern()
    badBG.pattern = badBG.NO_PATTERN
    badBG.pattern_back_colour  = 'red'
    mystyle.pattern = badBG

#    mystyle.num_format_str = '0'




    client = MongoClient('localhost', 27017)
    db = client['lianjia']

#    sDate = settings['MONGODB_DOCNAME'] + time.strftime('%Y%m%d', time.localtime(time.time()))
#    coll = db['house_20170712']
    sDate = 'xiaoqu_' + time.strftime('%Y%m%d', time.localtime(time.time()))
    coll = db[sDate]

#  城区 板块 小区名称 年代 编号  均价 坐标1 坐标2 在售 成交 看房 地铁距离 链接

    i = 0
    ws.write(i, 0, u'城区', mystyle)
    ws.write(i, 1, u'板块', mystyle)
    ws.write(i, 2, u'小区名称', mystyle)
    ws.write(i, 3, u'年代', mystyle)
    ws.write(i, 4, u'编号', mystyle)
    ws.write(i, 5, u'均价', mystyle)
    ws.write(i, 6, u'坐标1', mystyle)
    ws.write(i, 7, u'坐标2', mystyle)
    ws.write(i, 8, u'在售', mystyle)
    ws.write(i, 9, u'成交', mystyle)
    ws.write(i, 10, u'看房', mystyle)
    ws.write(i, 11, u'地铁距离', mystyle)
    ws.write(i, 12, u'链接', mystyle)

    badBG.pattern_back_colour = 'blue'
    mystyle.pattern = badBG

    for obj in coll.find():
        i = i + 1
        mystyle.num_format_str = 'general'
        ws.write(i, 0, obj['city'], mystyle)
        ws.write(i, 1, obj['town'], mystyle)
        ws.write(i, 2, obj['name'], mystyle)
        ws.write(i, 3, obj['age'], mystyle)
        ws.write(i, 4, obj['key'], mystyle)
        mystyle.num_format_str = '0.00'
        ws.write(i, 5,  obj['average_price'] , mystyle)
        mystyle.num_format_str = 'general'
        ws.write(i, 6, obj['gps_y'], mystyle)
        ws.write(i, 7, obj['gps_x'], mystyle)
        mystyle.num_format_str = '0'
        ws.write(i, 8, obj['onsale_num'], mystyle)
        ws.write(i, 9, obj['deal_num'], mystyle)
        ws.write(i, 10, obj['watch_num'], mystyle)
        mystyle.num_format_str = '0.00'
        ws.write(i, 11, obj['m_dist'], mystyle)
        mystyle.num_format_str = 'general'
        ws.write(i, 12, obj['link'], mystyle)

#        print obj['_id']
#    ws.write(i,17,ss,mystyle)
    sDate = 'E:\\' + sDate + '.xls'
    wb.save(sDate)

def covertHouse2() :

    client = MongoClient('localhost', 27017)
    db = client['lianjia']
    coll1 = db['zz_house_20170713']
    coll2 = db['house_20170713']
#    obj = coll.find_one()

    i = 0

    for obj in coll1.find():
 #       obj['community_id'] = obj['community'][29:42].encode('gbk')
        if len(obj['age']) > 0 :
            obj['age'] = int ( obj['age'] )
        else :
            obj['age'] = 0
        obj['price'] = float(obj['price'])
        obj['average_price'] = float(obj['average_price'])
        obj['area'] = float( obj['area'].replace(u'平','') )

#        obj['community_id'] = obj['community'][29:42].encode('gbk')
        try:
#            info = dict(obj)
#            if coll2.insert(info):
            if coll2.insert(obj):
                print('House bingo')
        except Exception:
            pass

def covertHouse() :

    client = MongoClient('localhost', 27017)
    db = client['lianjia']
    coll1 = db['zz_house_20170711']
    coll2 = db['house_2017071']
#    obj = coll.find_one()

    i = 0

    for obj in coll1.find():
 #       obj['community_id'] = obj['community'][29:42].encode('gbk')
        obj['house_id'] = obj['link'][33:42]
        obj['community_id'] = obj['community'][29:42].encode('gbk')
        try:
#            info = dict(obj)
#            if coll2.insert(info):
            if coll2.insert(obj):
                print('House bingo')
        except Exception:
            pass


def saveNearMetroXiaoquA() :

    client = MongoClient('localhost', 27017)
    db = client['lianjia']
    sDate = 'xiaoqu_' + time.strftime('%Y%m%d', time.localtime(time.time()))
    coll1 = db[sDate]
    coll2 = db[sDate+'_M2']
#    obj = coll.find_one()

    i = 0

    for obj in coll1.find():
        if obj['m_dist'] < 650.00 :
            try:
#            info = dict(obj)
#            if coll2.insert(info):
                coll2.insert(obj)
            except Exception:
                pass

def getIntInStr(theStr):
    theStr1 = theStr.encode('gbk', 'ignore')
    theStr1 = filter(str.isdigit, theStr1)
    theStr2 = ''.join(theStr1)
    #        print(type(theStr1))
    #        print(theStr2)
    if len(theStr2) > 0:
        return int(theStr2)
    else:
        return 0


def covertXiaoqu() :

    client = MongoClient('localhost', 27017)
    db = client['lianjia']
    coll1 = db['zz_xiaoqu_20170712']
    coll2 = db['xiaoqu_20170712']
#    obj = coll.find_one()

    i = 0

    for obj in coll1.find():
        obj['m_dist'] = -1
        obj['d_dist1'] = -1
        obj['c_dist1'] = -1
        obj['w_dist1'] = -1
        obj['d_dist2'] = -1
        obj['c_dist2'] = -1
        obj['w_dist2'] = -1
        obj['d_dist3'] = -1
        obj['c_dist3'] = -1
        obj['w_dist3'] = -1
        obj['d_dist4'] = -1
        obj['c_dist4'] = -1
        obj['w_dist4'] = -1
 #       obj['community_id'] = obj['community'][29:42].encode('gbk')

        try:
            obj['deal_num'] = int(obj['deal_num'])
            obj['watch_num'] = int(obj['watch_num'])
            obj['onsale_num'] = int(obj['onsale_num'])
            obj['average_price'] = getIntInStr(obj['average_price'])
        except Exception:
            print "error Wang J"

#        obj['community_id'] = obj['community'][29:42].encode('gbk')
        try:
#            info = dict(obj)
#            if coll2.insert(info):
            if coll2.insert(obj):
                print('House bingo')
        except Exception:
            pass

#covertXiaoqu()
#XiaoquToMetroSaveToDB()
saveNearMetroXiaoquA()
#saveHouseToFile()
#saveXiaoquToFile()

"""
        # twip = 1/20 of a point = 1/1440 of a inch
        # usually resolution == 96 pixels per 1 inch 
        # (rarely 120 pixels per 1 inch or another one)
        
        self.height = 0x00C8 # 200: this is font with height 10 points
        
    def __init__(self):
        self.num_format_str  = _default_num_format
        self.font            = _default_font.copy()
        self.alignment       = _default_alignment.copy()
        self.borders         = _default_borders.copy()
        self.pattern         = _default_pattern.copy()
        self.protection      = _default_protection.copy()

    _default_num_format = 'general'
    _default_font = Formatting.Font()
    _default_alignment = Formatting.Alignment()
    _default_borders = Formatting.Borders()
    _default_pattern = Formatting.Pattern()
    _default_protection = Formatting.Protection()
    _std_num_fmt_list = [
        'general',
        '0',
        '0.00',
        '#,##0',
        '#,##0.00',
        '"$"#,##0_);("$"#,##',
        '"$"#,##0_);[Red]("$"#,##',
        '"$"#,##0.00_);("$"#,##',
        '"$"#,##0.00_);[Red]("$"#,##',
        '0%',
        '0.00%',
        '0.00E+00',
        '# ?/?',
        '# ??/??',
        'M/D/YY',
        'D-MMM-YY',
        'D-MMM',
        'MMM-YY',
        'h:mm AM/PM',
        'h:mm:ss AM/PM',
        'h:mm',
        'h:mm:ss',
        'M/D/YY h:mm',
        '_(#,##0_);(#,##0)',
        '_(#,##0_);[Red](#,##0)',
        '_(#,##0.00_);(#,##0.00)',
        '_(#,##0.00_);[Red](#,##0.00)',
        '_("$"* #,##0_);_("$"* (#,##0);_("$"* "-"_);_(@_)',
        '_(* #,##0_);_(* (#,##0);_(* "-"_);_(@_)',
        '_("$"* #,##0.00_);_("$"* (#,##0.00);_("$"* "-"??_);_(@_)',
        '_(* #,##0.00_);_(* (#,##0.00);_(* "-"??_);_(@_)',
        'mm:ss',
        '[h]:mm:ss',
        'mm:ss.0',
        '##0.0E+0',
        '@'
"""