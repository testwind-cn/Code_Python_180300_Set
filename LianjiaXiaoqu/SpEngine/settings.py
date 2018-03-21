# -*- coding: utf-8 -*-

# Scrapy settings for Xiaoqu project


BOT_NAME = 'SpEngine'

SPIDER_MODULES = ['SpEngine.spiders']
NEWSPIDER_MODULE = 'SpEngine.spiders'


ROBOTSTXT_OBEY = False

DOWNLOAD_DELAY = 3

SCHEDULER = "scrapy_redis.scheduler.Scheduler"    #调度
DUPEFILTER_CLASS = ""  #去重 scrapy_redis.dupefilter.RFPDupeFilter
SCHEDULER_PERSIST = True       #不清理Redis队列True
SCHEDULER_QUEUE_CLASS = "scrapy_redis.queue.SpiderQueue"    #队列

DOWNLOADER_MIDDLEWARES = {
    'SpEngine.middlewares2.CustomRetryMiddleware': 650,
#    'scrapy.downloadermiddlewares.redirect.RedirectMiddleware': None,
}

#    'Xiaoqu.middlewares1.FilterURLs': 1,
#    'Xiaoqu.middlewares2.CustomRetryMiddleware': 120,
#    'scrapy.downloadermiddlewares.redirect.RedirectMiddleware': None,
#    'scrapy.downloadermiddlewares.redirect.MetaRefreshMiddleware': None,

ITEM_PIPELINES = {
   'SpEngine.pipelines.ThePipeline': 300,
}

MONGODB_HOST = '127.0.0.1'
MONGODB_PORT = 27017
MONGODB_DBNAME = "lianjia"
MONGODB_DOCNAME = "xiaoqu_"