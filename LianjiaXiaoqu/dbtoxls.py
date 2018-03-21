from pymongo import MongoClient

import pyExcelerator

def saveToFile() :
    wb = pyExcelerator.Workbook()
    ws = wb.add_sheet('pages')
    myfont = pyExcelerator.Font()
    myfont.name = 'Times New Roman'
    mystyle = pyExcelerator.XFStyle()
    mystyle.font = myfont

    client = MongoClient('localhost', 27017)
    db = client['lianjia']
    coll = db['saveinfo_20170709']
    obj = coll.find_one()

    i = 0

    for obj in coll.find():
        ws.write(i,0,obj['link'],mystyle)
        ws.write(i,1,obj['title'],mystyle)
        ws.write(i,2,obj['image'],mystyle)
        ws.write(i,3,obj['community'],mystyle)
        ws.write(i,4,obj['community_title'],mystyle)
        ws.write(i,5,obj['model'],mystyle)
        ws.write(i,6,obj['area'],mystyle)
        ws.write(i,7,obj['tall'],mystyle)
        ws.write(i,8,obj['faceto'],mystyle)
        ws.write(i,9,obj['price'],mystyle)
        ws.write(i,10,obj['average_price'],mystyle)
        ws.write(i,11,obj['city'],mystyle)
        ws.write(i,12,obj['town'],mystyle)
        ws.write(i,13,obj['age'],mystyle)
        ws.write(i,14,obj['tag0'],mystyle)
        ws.write(i,15,obj['tag1'],mystyle)
        ws.write(i,16,obj['tag2'],mystyle)
        print obj['_id']
#    ws.write(i,17,ss,mystyle)
        i = i + 1
    wb.save('E:\\ddd1.xls')