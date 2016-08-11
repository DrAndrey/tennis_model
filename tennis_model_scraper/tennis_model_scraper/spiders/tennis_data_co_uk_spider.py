# -*- coding: utf-8 -*-

"""

"""

import scrapy

from tennis_model.tennis_model_scraper.tennis_model_scraper import items


class TennisDataCoUkSpider(scrapy.Spider):
    name = "tennis_data_co_uk"
    allowed_domains = ["http://www.tennis-data.co.uk/alldata.php"]
    start_urls = ["http://www.tennis-data.co.uk/alldata.php"]

    def _correct_ext(self, link):
        if ".zip" in link:
            return link
        elif "zip" in link:
            return ".zip".join(link.split("zip"))
        else:
            raise Exception("Unknown file extension from url - {0}. 'zip' is expected".format(link))

    def parse(self, response):
        archive_links = response.xpath("/html/body/table[5]/tr[2]/td[3]/a/@href")
        for link in archive_links:
            short_file_url = self._correct_ext(link.extract())
            full_file_url = response.urljoin(short_file_url)
            item = items.TennisModelScraperItem()
            item["file_urls"] = [full_file_url]
            yield item

if __name__ == '__main__':
    pass
