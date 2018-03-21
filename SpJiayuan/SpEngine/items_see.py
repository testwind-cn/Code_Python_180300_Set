# -*- coding: utf-8 -*-

import scrapy


class TheItem_See( scrapy.Item):
    # 详情链接 标签 图片 小区 小区名称 户型   面积  楼层 朝向 关注人数  观看人数  发布时间  价格   均价    经纬度 城区 市政 年代 标签1 标签2 标签3 地铁距离 直线距离1 开车距离1 步行距离1 直线距离2 开车距离2 步行距离2 直线距离3 开车距离3 步行距离3 直线距离4 开车距离4 步行距离4
    # name = scrapy.Field()
    # key = scrapy.Field()
    # city = scrapy.Field()
    # town = scrapy.Field()
    # age = scrapy.Field()
    # gps_x = scrapy.Field()
    # gps_y = scrapy.Field()
    #
    # average_price = scrapy.Field()
    # onsale_num = scrapy.Field()
    # deal_num = scrapy.Field()
    # watch_num = scrapy.Field()
    # link = scrapy.Field()
    # m_dist = scrapy.Field()
    # d_dist1 = scrapy.Field()
    # c_dist1 = scrapy.Field()
    # w_dist1 = scrapy.Field()
    # d_dist2 = scrapy.Field()
    # c_dist2 = scrapy.Field()
    # w_dist2 = scrapy.Field()
    # d_dist3 = scrapy.Field()
    # c_dist3 = scrapy.Field()
    # w_dist3 = scrapy.Field()
    # d_dist4 = scrapy.Field()
    # c_dist4 = scrapy.Field()
    # w_dist4 = scrapy.Field()

    pic = scrapy.Field()
    user_info = scrapy.Field()
    date = scrapy.Field()
    check_zl = scrapy.Field()
