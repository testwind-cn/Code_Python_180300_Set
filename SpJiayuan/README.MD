
# 标准爬虫模板-修改内容

## spider.py
1. 修改初始页面列表  start_urls
2. 修改  parse10 ， parse20， parse30 不同层级的页面处理程序
3. 修改 getItemOnPage(self, response, deep) 抓取页面元素数据的程序
4. 修改 getChildPageRequests(self, response, deep) 抓取子页面请求数据的程序

## Item.py
修改里面的变量定义

## pipelines.py
修改 process_item 里面的日志输出


## settings.py
### 修改
MONGODB_DBNAME = "wind_Ajk"
MONGODB_DOCNAME = "spider_"







## settings.py
### 不变
ITEM_PIPELINES = {
   'SpEngine.pipelines.ThePipeline': 300,

DOWNLOADER_MIDDLEWARES = {
    'SpEngine.middlewares2.CustomRetryMiddleware': 650,





http://cache.baiducontent.com/c?m=9f65cb4a8c8507ed19fa950d100b813b484380147d8690463d9f8448e4391b145a31bfa679715653928361215ced120dabed7625744477f6dddf883c9decd36a72d46663671cf11b548c47bb8e1b65972fd10ba9fc5bacadf043d3f99194881515dd53742bddadd20157&p=8b2a9706a49419ea08e2977e0600bb&newp=8e39df16d9c152bc08e2977e065fc4231610db2151d1db01298ffe0cc4241a1a1a3aecbf20231104d4c17e6502a84c5de0f53d78350834f1f689df08d2ecce7e3c9b&user=baidu&fm=sc&query=scrapy+request+cookie&qid=f71d8fbd00020943&p1=3

Using a dict:

    request_with_cookies = Request(url="http://www.example.com",
                                   cookies={'currency': 'USD', 'country': 'UY'})
Using a list of dicts:

    request_with_cookies = Request(url="http://www.example.com",
                                   cookies=[{'name': 'currency',
                                            'value': 'USD',
                                            'domain': 'example.com',
                                            'path': '/currency'}])


Example of request without merging cookies:

    request_with_cookies = Request(url="http://www.example.com",
                               cookies={'currency': 'USD', 'country': 'UY'},
                               meta={'dont_merge_cookies': True})


----------
END