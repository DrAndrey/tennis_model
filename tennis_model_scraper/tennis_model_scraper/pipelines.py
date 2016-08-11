# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import os

from scrapy.pipelines.files import FilesPipeline


class TennisModelScraperPipeline(FilesPipeline):
    def file_path(self, request, response=None, info=None):
        file_name = request.url.split('/')[-2]
        file_path = os.path.join("tennis_data_co_uk", "{0}.zip".format(file_name))
        return file_path
