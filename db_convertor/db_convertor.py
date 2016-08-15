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
    last_name = Column(String)
    matches = relationship("Match", secondary=player_match_table)
    __table_args__ = (UniqueConstraint('first_name', 'last_name', name='first_last_names'),)


class Match(Base):
    __tablename__ = 'matches'
    match_id = Column(Integer, primary_key=True)
    date = Column(Date)
    tournament = Column(String)
    stage = Column(String)
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


if __name__ == '__main__':
    converted_year = 2006
    engine = create_engine('sqlite:///tennis_model.db')

    # Base.metadata.drop_all(engine)
    # Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()
    init_games(session)

    atp_df, co_uk_df = load_data(converted_year)
