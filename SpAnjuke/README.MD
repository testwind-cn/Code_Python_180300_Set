
# 标准爬虫模板-修改内容

## spider.py
1. 修改初始页面列表  start_urls
2. 修改  parse1 ， parse11， parse111 不同层级的页面处理程序
3. 修改 getItemOnPage(self, response, deep) 抓取页面元素数据的程序
4. 修改 getChildPageRequests(self, response, deep) 抓取子页面请求数据的程序

## parse level
- 一级<br>
1 ||| 2<br>
- 二级<br>
11,21,31 || 12,22,32<br>
- 三级<br>
111,211,311 | 121,221,321 | 131,231,331 ||<br>
112,212,312 | 122,222,322 | 132,232,332<br>


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



----------
END