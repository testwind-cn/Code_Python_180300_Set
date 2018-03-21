# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
from scrapy.conf import settings
from .items import HouseItem
import time

class HousePipeline(object):
    def __init__(self):
        host = settings['MONGODB_HOST']
        port = settings['MONGODB_PORT']
        db_name = settings['MONGODB_DBNAME']
        client = pymongo.MongoClient(host=host,port=port)
        tdb = client[db_name]
        sDate = settings['MONGODB_DOCNAME'] + time.strftime('%Y%m%d',time.localtime(time.time()))
        self.post = tdb[sDate]

    def process_item(self, item, spider):
        print "\n\n a Item =============================="
        print item['link']
        print item['title']
        print item['model']
        print item['area']
        print item['tall']
        print item['faceto']
        print item['price']
        print item['community']
        print item['community_title']
        print item['city']
        print item['town']
        print item['age']
        print item['average_price']
        print item['tag0']
        print item['tag1']
        print item['tag2']

        if isinstance(item,HouseItem):
            try:
                info = dict(item)
                if self.post.insert(info):
                    print('House bingo')
            except Exception:
                pass
        return item