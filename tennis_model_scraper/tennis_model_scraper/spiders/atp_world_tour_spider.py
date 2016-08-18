# -*- coding: utf-8 -*-

"""

"""

import json
import datetime

import scrapy

from tennis_model.tennis_model_scraper.tennis_model_scraper import items

first_year = datetime.datetime.now().year
last_year = first_year

player_stats_keys = ['playerStats', 'opponentStats']
necessary_stats = ['TotalServicePointsWonDivisor', 'TotalServicePointsWonDividend', 'TotalReturnPointsWonDivisor',
                   'TotalReturnPointsWonDividend']


class ATPWorldTourSpider(scrapy.Spider):
    name = "atp_world_tour"
    allowed_domains = ["atpworldtour.com"]
    start_urls = ["http://www.atpworldtour.com/en/scores/results-archive?year={0}".format(year)
                  for year in range(first_year, last_year+1)]
    custom_settings = {'ITEM_PIPELINES': {'tennis_model_scraper.pipelines.ATPWorldTourPipeline': 1}}

    def _get_player_stats(self, response):
        necessary_palyer_stats = []
        stats = json.loads(response.xpath("//*[@id='matchStatsData']/text()").extract()[0])
        for player_stats_key in player_stats_keys:
            payer_stats = stats[0][player_stats_key]
            necessary_palyer_stats.append({})
            for stat_key in necessary_stats:
                necessary_palyer_stats[-1][stat_key] = payer_stats[stat_key]
        return necessary_palyer_stats

    def parse(self, response):
        tournament_links = response.xpath("//*[@id='scoresResultsArchive']/table/tbody/tr/td[8]/a/@href")
        for link in tournament_links:
            url = response.urljoin(link.extract())
            yield scrapy.Request(url, callback=self.parse_tournament)

    def parse_tournament(self, response):
        stage_names = []
        winner_names = []
        loser_names = []
        match_links = []

        tbodies = response.xpath("//*[@id='scoresResultsContent']/div/table/tbody")
        theads = response.xpath("//*[@id='scoresResultsContent']/div/table/thead")
        for tbody, thead in zip(tbodies, theads):
            stage_name = thead.xpath("tr/th/text()")[0]
            for row in tbody.xpath("tr"):
                href = row.xpath("td[8]/a/@href")
                if len(href) == 1:
                    winner_name = row.xpath("td[3]/a/text()")[0]
                    loser_name = row.xpath("td[7]/a/text()")[0]

                    winner_names.append(winner_name)
                    loser_names.append(loser_name)
                    match_links.append(href[0])
                    stage_names.append(stage_name)
        winner_names.reverse()
        loser_names.reverse()
        match_links.reverse()
        stage_names.reverse()
        for winner_name, loser_name, stage_name, link in zip(winner_names, loser_names, stage_names, match_links):
            url = response.urljoin(link.extract())
            request = scrapy.Request(url, callback=self.parse_match)
            request.meta['winner_name'] = winner_name.extract()
            request.meta['loser_name'] = loser_name.extract()
            request.meta['stage_name'] = stage_name.extract()
            yield request

    def parse_match(self, response):
        splitted_url = response.url.split('/')
        tournament = splitted_url[5]
        year = int(splitted_url[7])
        winner_stats, loser_stats = self._get_player_stats(response)

        item = items.ATPWorldTourItem()
        item["winner_name"] = response.meta['winner_name']
        item["loser_name"] = response.meta['loser_name']
        item["winner_stats"] = winner_stats
        item["loser_stats"] = loser_stats
        item["tournament"] = tournament
        item["stage_name"] = response.meta['stage_name']
        item["year"] = year
        return item


if __name__ == '__main__':
    pass
