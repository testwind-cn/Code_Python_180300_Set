# -*- coding: utf-8 -*-
from pymongo import MongoClient

# import pyExcelerator
import xlwt
import time

# 设置单元格样式
def set_style(name, height, bold=False):
    style = xlwt.XFStyle()  # 初始化样式

    font = xlwt.Font()  # 为样式创建字体
    font.name = name  # 'Times New Roman'
    font.bold = bold
    font.color_index = 4
    font.height = height

    # borders= xlwt.Borders()
    # borders.left= 6
    # borders.right= 6
    # borders.top= 6
    # borders.bottom= 6

    style.font = font
    # style.borders = borders

    return style

def saveToFile() :
    wBook = xlwt.Workbook() #创建工作簿
    wSheet = wBook.add_sheet('pages')

    client = MongoClient('localhost', 27017)
    db = client['wind_Ajk']

    timeName = time.strftime('%Y%m%d', time.localtime(time.time()))
    collName = "spider_" + timeName
    coll = db[collName] # 'spider_20180319'
    obj = coll.find_one()


    mystyle = set_style('Times New Roman', 220, False)

    i = 0
    for obj in coll.find():
        wSheet.write(i,0,obj['name'],mystyle)
        wSheet.write(i,1,obj['year'],mystyle)
        wSheet.write(i,2,obj['parents'],mystyle)

        n = len( obj['p_list'] )
        for x in range( n ) :
            wSheet.write(i, 3+x, obj['p_list'][x], mystyle)

        n = len(obj['dates'])
        for x in range(n):
            wSheet.write(i, 9 + 2*x, obj['dates'][x], mystyle)

        n = len(obj['prices'])
        for x in range(n):
            wSheet.write(i, 10 + 2 * x, obj['prices'][x], mystyle)

        i = i + 1
    fileName = "E:\\Anjuke_%s.xls" % timeName
    wBook.save(fileName)  #'E:\\ddd1.xls'


saveToFile()
