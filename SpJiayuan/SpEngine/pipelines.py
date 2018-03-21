﻿# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
from scrapy.conf import settings

from SpEngine.items_see import TheItem_See
from .items import TheItem
import time
import logging

class ThePipeline(object):
    def __init__(self):
        host = settings['MONGODB_HOST']
        port = settings['MONGODB_PORT']
        db_name = settings['MONGODB_DBNAME']
        client = pymongo.MongoClient(host=host,port=port)
        tdb = client[db_name]
        sDate = settings['MONGODB_DOCNAME'] + time.strftime('%Y%m%d',time.localtime(time.time()))
        self.post = tdb[sDate]
#        self.post = tdb['xiaoqu_20170712']

    def process_item(self, item, spider):
        # print("\n\n a Item ==============================")
        # print(item['name'])
        # print(item['key'])
        # print(item['city'])
        # print(item['town'])
        # print(item['age'])
        # print(item['gps_x'])
        # print(item['gps_y'])
        # print(item['average_price'])
        # print(item['onsale_num'])
        # print(item['deal_num'])
        # print(item['watch_num'])
        # print(item['link'])

        # try:
        #     print(item['name'])
        #     print(item['parents'])
        #     print(item['dates'])
        #     print(item['prices'])
        # except Exception:
        #     print("Error: 没有找到文件或读取文件失败")
        # else:
        #     print( "OK")

        logging.debug("\033[1;32;47m WangJ := \033[0m" "====== Find a new Item ======" )

        if isinstance(item,  TheItem):
            try:
                info = dict(item)
                if self.post.insert(info):
                    logging.debug( "\033[1;32;47m WangJ := \033[0m" "插入数据库: %s %s %s \n" %(item['name'] , item['year'] , item['parents']) )  # 处理安居客shanghai2018
            except Exception:
                pass
        if isinstance(item,  TheItem_See):
            try:
                info = dict(item)
                if self.post.insert(info):
                    logging.debug( "\033[1;32;47m WangJ := \033[0m" "插入数据库: %s %s %s \n" %(item['name'] , item['year'] , item['parents']) )  # 处理安居客shanghai2018
            except Exception:
                pass
        return item