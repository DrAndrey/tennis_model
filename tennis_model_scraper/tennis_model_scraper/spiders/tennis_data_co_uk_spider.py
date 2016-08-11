# -*- coding: utf-8 -*-

"""

"""

import scrapy

from tennis_model.tennis_model_scraper.tennis_model_scraper import items


class TennisDataCoUkSpider(scrapy.Spider):
    name = "tennis_data_co_uk"
    allowed_domains = ["http://www.tennis-data.co.uk/alldata.php"]
    start_urls = ["http://www.tennis-data.co.uk/alldata.php"]

    def parse(self, response):
        archive_links = response.xpath("/html/body/table[5]/tr[2]/td[3]/a/@href")
        for link in archive_links:
            file_url = response.urljoin(link.extract())
            item = items.TennisModelScraperItem()
            item["file_urls"] = [file_url]
            yield item

if __name__ == '__main__':
    pass
