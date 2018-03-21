# -*- coding: utf-8 -*-

import scrapy


class HouseItem(scrapy.Item):
    # old 详情链接 标签 图片 小区 小区名称 户型   面积  楼层 朝向 关注人数  观看人数  发布时间  价格   均价    经纬度 城区 市政 年代 标签1 标签2 标签3


#  城区 板块 小区名称 户型  面积 价格 单价 楼层 朝向 房龄 小区ID 小区链接 标题 房源编号 详情链接 图片  标签0 标签1 标签2
#    Latitude = scrapy.Field()
    city = scrapy.Field()
    town = scrapy.Field()
    community_title = scrapy.Field()
    model = scrapy.Field()
    area = scrapy.Field()
    price = scrapy.Field()
    average_price = scrapy.Field()
    tall = scrapy.Field()
    faceto = scrapy.Field()

    age = scrapy.Field()
    community_id = scrapy.Field()
    community = scrapy.Field()
    title = scrapy.Field()
    house_id = scrapy.Field()
    link = scrapy.Field()
    image = scrapy.Field()



    tag0 = scrapy.Field()
    tag1 = scrapy.Field()
    tag2 = scrapy.Field()

    #
    focus_num = scrapy.Field()
    watch_num = scrapy.Field()
    time = scrapy.Field()