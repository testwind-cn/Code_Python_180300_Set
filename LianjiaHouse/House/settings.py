# -*- coding: utf-8 -*-

# Scrapy settings for House project


BOT_NAME = 'House'

SPIDER_MODULES = ['House.spiders']
NEWSPIDER_MODULE = 'House.spiders'


ROBOTSTXT_OBEY = False

DOWNLOAD_DELAY = 3

SCHEDULER = "scrapy_redis.scheduler.Scheduler"    #调度
DUPEFILTER_CLASS = ""  #去重 scrapy_redis.dupefilter.RFPDupeFilter
SCHEDULER_PERSIST = True       #不清理Redis队列True
SCHEDULER_QUEUE_CLASS = "scrapy_redis.queue.SpiderQueue"    #队列

DOWNLOADER_MIDDLEWARES = {
    'House.middlewares2.CustomRetryMiddleware': 650,
#    'scrapy.downloadermiddlewares.redirect.RedirectMiddleware': None,
}

#    'House.middlewares1.FilterURLs': 1,
#    'House.middlewares2.CustomRetryMiddleware': 120,
#    'scrapy.downloadermiddlewares.redirect.RedirectMiddleware': None,
#    'scrapy.downloadermiddlewares.redirect.MetaRefreshMiddleware': None,

ITEM_PIPELINES = {
   'House.pipelines.HousePipeline': 300,
}

MONGODB_HOST = '127.0.0.1'
MONGODB_PORT = 27017
MONGODB_DBNAME = "lianjia"
MONGODB_DOCNAME = "house_"