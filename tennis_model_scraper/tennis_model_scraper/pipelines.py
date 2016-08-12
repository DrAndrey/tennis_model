# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import os
import csv

from scrapy.pipelines.files import FilesPipeline

import tennis_model.tennis_model_scraper.tennis_model_scraper.settings as settings
import tennis_model.tennis_model_scraper.tennis_model_scraper.spiders.atp_world_tour_spider as atp_world_tour_spider

tennis_data_co_uk_path = "tennis_data_co_uk"
atp_world_tour_path = "atp_world_tour"

csv_field_names = ["tournament", "stage_name", "winner_first_name", "winner_second_name", "loser_first_name",
                   "loser_second_name", "winner_service_share", "winner_return_share", "loser_service_share",
                   "loser_return_share"]


class TennisDataCoUkPipeline(FilesPipeline):
    def file_path(self, request, response=None, info=None):
        file_name = request.url.split('/')[-2]
        file_path = os.path.join(tennis_data_co_uk_path, "{0}.zip".format(file_name))
        return file_path


class ATPWorldTourPipeline(object):
    def __init__(self):
        self.output_path = os.path.join(settings.FILES_STORE, atp_world_tour_path)
        for year in range(atp_world_tour_spider.first_year, atp_world_tour_spider.last_year+1):
            os.remove(self._get_file_path(year))

        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)

    def _get_file_path(self, year):
        return os.path.join(self.output_path, "{0}.csv".format(year))

    def _split_player_name(self, name):
        splitted_name = name.split()
        first_name = " ".join(splitted_name[: -1])
        second_name = splitted_name[-1]
        return first_name, second_name

    def process_item(self, item, spider):
        file_path = self._get_file_path(item["year"])
        is_file_already_exists = os.path.exists(file_path)

        with open(file_path, "a") as f:
            writer = csv.DictWriter(f, fieldnames=csv_field_names)

            if not is_file_already_exists:
                writer.writeheader()

            row = {}
            row["tournament"] = item["tournament"]
            row["stage_name"] = item["stage_name"]
            print(row["stage_name"])
            row["winner_first_name"], row["winner_second_name"] = self._split_player_name(item["winner_name"])
            row["loser_first_name"], row["loser_second_name"] = self._split_player_name(item["loser_name"])

            row["winner_service_share"] = float(item["winner_stats"]["TotalServicePointsWonDividend"]) / \
                                          item["winner_stats"]["TotalServicePointsWonDivisor"]
            row["winner_return_share"] = float(item["winner_stats"]["TotalReturnPointsWonDividend"]) / \
                                         item["winner_stats"]["TotalReturnPointsWonDivisor"]
            row["loser_service_share"] = float(item["loser_stats"]["TotalServicePointsWonDividend"]) / \
                                         item["loser_stats"]["TotalServicePointsWonDivisor"]
            row["loser_return_share"] = float(item["loser_stats"]["TotalReturnPointsWonDividend"]) / \
                                        item["loser_stats"]["TotalReturnPointsWonDivisor"]
            writer.writerow(row)
