# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class TennisDataCoUkItem(scrapy.Item):
    file_urls = scrapy.Field()


class ATPWorldTourItem(scrapy.Item):
    winner_name = scrapy.Field()
    loser_name = scrapy.Field()
    winner_stats = scrapy.Field()
    loser_stats = scrapy.Field()
    tournament = scrapy.Field()
    stage_name = scrapy.Field()
    year = scrapy.Field()