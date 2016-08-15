# -*- coding: utf-8 -*-

"""

"""

import os
import zipfile

import pandas as pd
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Date, Table, Float, UniqueConstraint
from sqlalchemy.orm import sessionmaker, relationship

import tennis_model.tennis_model_scraper.tennis_model_scraper.settings as settings

Base = declarative_base()

match_game_table = Table('match_game', Base.metadata, Column('match_id', Integer, ForeignKey('matches.match_id')),
                         Column('game_id', Integer, ForeignKey('games.game_id')))
player_match_table = Table('player_match', Base.metadata, Column('player_id', Integer, ForeignKey('players.player_id')),
                           Column('match_id', Integer, ForeignKey('matches.match_id')))


class Game(Base):
    __tablename__ = 'games'
    game_id = Column(Integer, primary_key=True)
    winner_points = Column(Integer)
    loser_points = Column(Integer)
    __table_args__ = (UniqueConstraint('winner_points', 'loser_points', name='winner_loser_points'),)


class Player(Base):
    __tablename__ = 'players'
    player_id = Column(Integer, primary_key=True)
    first_name = Column(String)
    second_name = Column(String)
    matches = relationship("Match", secondary=player_match_table)
    __table_args__ = (UniqueConstraint('first_name', 'second_name', name='first_second_name'),)


class Match(Base):
    __tablename__ = 'matches'
    match_id = Column(Integer, primary_key=True)
    date = Column(Date)
    tournament = Column(String)
    stage = Column(String)
    surface = Column(String)
    best_of = Column(Integer)
    games = relationship("Game", secondary=match_game_table)
    winner_rank = Column(Integer)
    loser_rank = Column(Integer)
    winner_odd = Column(Float)
    loser_odd = Column(Float)
    winner_service = Column(Float)
    loser_service = Column(Float)
    winner_return = Column(Float)
    loser_return = Column(Float)


def init_games(session):
    games = [[6, 0], [6, 1], [6, 2], [6, 3], [6, 4], [7, 5], [7, 6]]
    for winner_points, loser_points in games:
        if not session.query(Game).filter(winner_points == winner_points and loser_points == loser_points).count():
            game = Game(winner_points=winner_points, loser_points=loser_points)
            session.add(game)
    session.commit()


def unzip_tennis_data_co_uk_data(converted_year):
    tennis_data_co_uk_zip_file = os.path.join(settings.FILES_STORE, settings.tennis_data_co_uk_path,
                                          "{0}.zip".format(converted_year))
    with zipfile.ZipFile(tennis_data_co_uk_zip_file, "r") as f_zip:
        f_zip.extractall()


def load_data(converted_year):
    unzip_tennis_data_co_uk_data(converted_year)
    atp_world_tour_file = os.path.join(settings.FILES_STORE, settings.atp_world_tour_path,
                                       "{0}.csv".format(converted_year))
    tennis_data_co_uk_file = "{0}.xls".format(converted_year)

    atp_df = pd.read_csv(atp_world_tour_file)
    co_uk_df = pd.read_excel(tennis_data_co_uk_file)

    os.remove(tennis_data_co_uk_file)
    return atp_df, co_uk_df


def compare_tournament_names(atp_df, co_uk_df):
    atp_df["tournament"] = atp_df["tournament"].apply(lambda x: x.replace("-", " "))
    co_uk_df["Tournament"] = co_uk_df["Tournament"].apply(lambda x: x.replace(".", "").lower())

    atp_names = atp_df["tournament"].unique()
    co_uk_names = co_uk_df["Tournament"].unique()

    compared_names = {}
    search_names = set(atp_names)
    for atp_name in atp_names:
        if atp_name in co_uk_names:
            compared_names[atp_name] = atp_name
            search_names.remove(atp_name)

    handle_comparison = {"synsam swedish open": "skistar swedish open", "rogers masters": "rogers cup",
                         "open de tenis comunidad valenciana": "valencia open", "madrid masters": "mutua madrid open",
                         "qatar exxon mobil open": "qatar exxonmobil open", "estoril open": "portugal open",
                         "us men's clay court championships": "fayez sarofim  co us mens clay court championship",
                         "campionati internazional d'italia": "internazionali bnl ditalia",
                         "open romania": "brd nastase tiriac trophy", "red letter days open": "aegon open nottingham",
                         "western & southern financial group masters": "western  southern open",
                         "french open": "roland garros", "davidoff swiss indoors": "swiss indoors basel",
                         "heineken open": "asb classic", "hamburg tms": "german tennis championships 2016",
                         "ba-ca tennis trophy": "erste bank open 500", "copa telmex": "argentina open",
                         "kingfisher airlines tennis open": "mumbai open", "idea prokom open": "orange warsaw open",
                         "nasdaq-100 open": "miami open presented by itau", "movistar open": "royal guard open chile",
                         "countrywide classic": "farmers classic", "legg mason classic": "citi open",
                         "allianz suisse open": "j safra sarasin swiss open gstaad", "ordina open": "ricoh open",
                         "international championships": "delray beach open", "dutch open": "dutch open tennis",
                         "regions morgan keegan championships": "memphis open", "channel open": "tennis channel open",
                         "open seat godo": "barcelona open bancsabadell", "stella artois": "aegon championships",
                         "rca championships": "indianapolis tennis championships", "open 13": "open 13 provence",
                         "monte carlo masters": "monte carlo rolex masters", "bmw open": "bmw open by fwu ag",
                         "hall of fame championships": "hall of fame tennis championships",
                         "abierto mexicano": "abierto mexicano telcel", "croatia open": "konzum croatia open umag",
                         "japan open": "rakuten japan open tennis championships 2016",
                         "grand prix de lyon": "grand prix de tennis de lyon", "open de moselle": "moselle open",
                         "sydney international": "apia international sydney", "stockholm open": "if stockholm open",
                         "dubai tennis championships": "dubai duty free tennis championships",
                         "kremlin cup": "vtb kremlin cup", "chennai open": "aircel chennai open",
                         "bnp paribas": "bnp paribas masters", "pacific life open": "bnp paribas open"}
    for co_uk_name, atp_name in handle_comparison.items():
        compared_names[co_uk_name] = atp_name
        search_names.remove(atp_name)

    if len(search_names):
        print(len(search_names))
        print(search_names)
        print(set(co_uk_names) - compared_names.keys())
        raise Exception("There is more tournament names to search")

    co_uk_df["Tournament"] = co_uk_df["Tournament"].map(compared_names)


def create_players(session, atp_df):

    def determine_long_name(splitted_name):
        articles = ["de", "el", "di", "del", "van", "der"]
        for border_num, name_part in enumerate(splitted_name[1:]):
            if name_part in articles:
                break
        border_num += 1
        first_name = " ".join(splitted_name[: border_num])
        second_name = " ".join(splitted_name[border_num:])

        if first_name == "martin vassallo":
            first_name = "martin"
            second_name = "vassallo-arguello"
        return first_name, second_name

    atp_df["winner_first_name"] = atp_df["winner_first_name"].apply(lambda x: x.lower())
    atp_df["winner_second_name"] = atp_df["winner_second_name"].apply(lambda x: x.lower())
    atp_df["loser_first_name"] = atp_df["loser_first_name"].apply(lambda x: x.lower())
    atp_df["loser_second_name"] = atp_df["loser_second_name"].apply(lambda x: x.lower())

    winner_full_name = atp_df.apply(lambda x: "{0} {1}".format(x["winner_first_name"], x["winner_second_name"]), axis=1)
    loser_full_name = atp_df.apply(lambda x: "{0} {1}".format(x["loser_first_name"], x["loser_second_name"]), axis=1)

    unique_winner_names = set(winner_full_name.unique())
    unique_loser_names = set(loser_full_name.unique())
    unique_names = unique_winner_names.union(unique_loser_names)

    for name in unique_names:
        clean_name = name.replace("jr.", "")
        splitted_name = clean_name.split()
        if len(splitted_name) == 2:
            first_name = splitted_name[0]
            second_name = splitted_name[1]
        else:
            first_name, second_name = determine_long_name(splitted_name)
        if not session.query(Player).filter(first_name == first_name and second_name == second_name).count():
            player = Player(first_name=first_name, second_name=second_name)
            session.add(player)
    session.commit()


def create_matches(session, atp_df, co_uk_df):

    def get_atp_row(atp_df, co_uk_row_data):
        splitted_winner_name = co_uk_row_data["Winner"].split()
        splitted_loser_name = co_uk_row_data["Loser"].split()
        winner_second_name = " ".join(splitted_winner_name[: -1]).lower()
        loser_second_name = " ".join(splitted_loser_name[: -1]).lower()
        atp_row = atp_df[(atp_df["tournament"] == co_uk_row_data["Tournament"]) &
                         (atp_df["winner_second_name"] == winner_second_name) &
                         (atp_df["loser_second_name"] == loser_second_name)]
        assert len(atp_row) == 1
        return atp_row

    for co_uk_row in co_uk_df[co_uk_df["Comment"] == "Completed"].iterrows():
        co_uk_row_data = co_uk_row[1]
        print(co_uk_row_data)

        atp_row = get_atp_row(atp_df, co_uk_row_data)
        print(atp_row)

        date = co_uk_row_data["Date"].date()
        tournament = atp_row["tournament"]
        stage = atp_row["stage_name"]
        surface = co_uk_row_data["Surface"].lower()
        best_of = co_uk_row_data["Best of"]
        # games =
        winner_rank = int(co_uk_row_data["WRank"])
        loser_rank = int(co_uk_row_data["LRank"])
        # winner_odd =
        # loser_odd =
        winner_service = atp_row["winner_service_share"].values[0]
        loser_service = atp_row["loser_service_share"].values[0]
        winner_return = atp_row["winner_return_share"].values[0]
        loser_return = atp_row["loser_return_share"].values[0]
        break


if __name__ == '__main__':
    converted_year = 2006
    engine = create_engine('sqlite:///tennis_model.db')

    # Base.metadata.drop_all(engine)
    # Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()
    init_games(session)

    atp_df, co_uk_df = load_data(converted_year)

    # print(atp_df.columns.values)
    # print(co_uk_df.columns.values)

    compare_tournament_names(atp_df, co_uk_df)
    create_players(session, atp_df)
    create_matches(session, atp_df, co_uk_df)
