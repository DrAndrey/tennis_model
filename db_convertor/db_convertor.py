# -*- coding: utf-8 -*-

"""

"""

import os
import zipfile
import datetime

import xlrd
import pandas as pd
from sqlalchemy import desc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Date, Table, Float, UniqueConstraint
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.orm.exc import NoResultFound

import tennis_model.tennis_model_scraper.tennis_model_scraper.settings as settings

Base = declarative_base()

match_game_table = Table('match_game', Base.metadata, Column('match_id', Integer, ForeignKey('matches.match_id')),
                         Column('game_id', Integer, ForeignKey('games.game_id')))
player_match_table = Table('player_match', Base.metadata, Column('player_id', Integer, ForeignKey('players.player_id')),
                           Column('match_id', Integer, ForeignKey('matches.match_id')))


class Game(Base):
    __tablename__ = 'games'
    game_id = Column(Integer, primary_key=True)
    winner_points = Column(Integer, nullable=False)
    loser_points = Column(Integer, nullable=False)
    __table_args__ = (UniqueConstraint('winner_points', 'loser_points', name='winner_loser_points'),)


class Player(Base):
    __tablename__ = 'players'
    player_id = Column(Integer, primary_key=True)
    first_name = Column(String, nullable=False)
    second_name = Column(String, nullable=False)
    matches = relationship("Match", secondary=player_match_table, lazy='dynamic')
    __table_args__ = (UniqueConstraint('first_name', 'second_name', name='first_second_name'),)

    def get_mean_probability(self, n, surface, date, probability_type):
        last_matches = self.matches.filter(Match.best_of == 3, Match.surface == surface, Match.date < date)\
            .order_by(desc(Match.date)).limit(n)

        if last_matches.count() < n:
            return None

        probability = 0.
        for match in last_matches:
            if match.winner is self:
                probability += match.__getattribute__("winner_{0}".format(probability_type))
            else:
                probability += match.__getattribute__("loser_{0}".format(probability_type))
        return probability / n

    def __repr__(self):
        return "{0} {1}".format(self.first_name, self.second_name)


class Match(Base):
    __tablename__ = 'matches'
    match_id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    tournament = Column(String, nullable=False)
    stage = Column(String, nullable=False)
    surface = Column(String, nullable=False)
    best_of = Column(Integer, nullable=False)
    games = relationship("Game", secondary=match_game_table)
    winner_rank = Column(Integer, nullable=False)
    loser_rank = Column(Integer, nullable=False)
    winner_odd = Column(Float, nullable=False)
    loser_odd = Column(Float, nullable=False)
    winner_service = Column(Float, nullable=False)
    loser_service = Column(Float, nullable=False)
    winner_return = Column(Float, nullable=False)
    loser_return = Column(Float, nullable=False)

    winner_id = Column(Integer, ForeignKey("players.player_id"), nullable=False)
    loser_id = Column(Integer, ForeignKey("players.player_id"), nullable=False)
    winner = relationship("Player", foreign_keys=[winner_id])
    loser = relationship("Player", foreign_keys=[loser_id])

    def __repr__(self):
        return "{0} vs {1}".format(self.winner, self.loser)


def init_games(session):
    games = [[6, 0], [6, 1], [6, 2], [6, 3], [6, 4], [7, 5], [7, 6], [10, 8], [8, 6], [11, 9], [9, 7], [16, 14],
             [17, 15], [13, 11], [12, 10], [70, 68], [18, 16], [14, 12]]
    for winner_points, loser_points in games:
        game_w = Game(winner_points=winner_points, loser_points=loser_points)
        game_l = Game(winner_points=loser_points, loser_points=winner_points)
        session.add_all([game_w, game_l])

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
    try:
        co_uk_df = pd.read_excel(tennis_data_co_uk_file, sheetname=str(converted_year))
        os.remove(tennis_data_co_uk_file)
    except FileNotFoundError:
        co_uk_df = pd.read_excel(tennis_data_co_uk_file + "x", sheetname=str(converted_year))
        os.remove(tennis_data_co_uk_file + "x")
    except xlrd.biffh.XLRDError:
        co_uk_df = pd.read_excel(tennis_data_co_uk_file)
        os.remove(tennis_data_co_uk_file)

    return atp_df, co_uk_df


def compare_tournament_names(atp_df, co_uk_df, converted_year):
    atp_df["tournament"] = atp_df["tournament"].apply(lambda x: x.replace("-", " "))
    co_uk_df["Tournament"] = co_uk_df["Tournament"].dropna().apply(lambda x: x.replace(".", "").lower())

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
                         "bnp paribas": "bnp paribas masters", "pacific life open": "bnp paribas open",
                         "austrian open": "generali open", "shanghai masters": "shanghai rolex masters",
                         "brisbane international": "brisbane international presented by suncorp",
                         "proton malaysian open": "malaysian open kuala lumpur",
                         "atlanta tennis championships": "bbt atlanta open", "power horse cup": "dusseldorf open",
                         "open de nice côte d’azur": "open de nice cote dazur",
                         "winston-salem open at wake forest university": "winston salem open",
                         "rio open": "rio open presented by claro", "millenium estoril open": "millennium estoril open",
                         "ecuador open": "ecuador open quito", "geneva open": "banque eric sturdza geneva open",
                         "istanbul open": "teb bnp paribas istanbul open"}
    if converted_year >= 2015:
        handle_comparison["masters cup"] = "barclays atp world tour finals"

    for co_uk_name, atp_name in handle_comparison.items():
        if atp_name in search_names:
            compared_names[co_uk_name] = atp_name
            search_names.remove(atp_name)
    search_names = [name for name in search_names if "olympic" not in name]

    if len(search_names):
        print(len(search_names))
        print(search_names)
        print(set(co_uk_names) - compared_names.keys())
        raise Exception("There is more tournament names to search")

    co_uk_df["Tournament"] = co_uk_df["Tournament"].map(compared_names)


def correct_player_names(co_uk_df):
    winner_names = co_uk_df["Winner"].unique()
    winner_dict = {name: name for name in winner_names}

    loser_names = co_uk_df["Loser"].unique()
    loser_dict = {name: name for name in loser_names}

    handle_correction = {"Navarro-Pastor I.": "Navarro I.", "Sultan-Khalfan A.": "Khalfan S.",
                         "Ramirez-Hidalgo R.": "Hidalgo R.", "Querry S.": "Querrey S.",
                         "Van der Dium A.": "Van der Duim A.", "Gallardo Valles M.": "Gallardo-Valles M.",
                         "Al Ghareeb M.": "Ghareeb M.", "Bahrouzyan O.": "Alawadhi O.",
                         "Granollers-Pujol M.": "Granollers M.", "Salva B.": "Salva-Vidal B.",
                         "Luque D.": "Luque-Velasco D.", "Haider-Mauer A.": "Haider-Maurer A.",
                         "Dev Varman S.": "Devvarman S.", "Schuttler P.": "Schuettler P.",
                         "Dutra Silva R.": "Silva R.", "Huta Galung J.": "Galung J.",
                         "Fornell M.": "Fornell-Mestres M.", "Ruevski P.": "Rusevski P.",
                         "Matsukevitch D.": "Matsukevich D.", "Chekov P.": "Chekhov P.", "Haji A.": "Hajji A.",
                         "Podlipnik H.": "Podlipnik-Castillo H.", "Munoz de la Nava D.": "de la Nava D.",
                         "Al-Ghareeb M.": "Ghareeb M.", "Lopez-Jaen M.A.": "Jaen M.",
                         "Sanchez De Luna J.": "Luna J.", "Estrella V.": "Burgos V.", "De Heart R.": "Deheart R.",
                         "Munoz de La Nava D.": "de la Nava D.", "Munoz-De La Nava D.": "de la Nava D.",
                         "Del Bonis F.": "Delbonis F.", "Saavedra Corvalan C.": "Saavedra-Corvalan C.",
                         "Riba-Madrid P.": "Riba P.", "Dasnieres de Veigy J.": "de Veigy J.",
                         "Awadhy O.": "Alawadhi O.", "Granollers Pujol G.": "Granollers G.",
                         "Ramos A.": "Ramos-Vinolas A.", "Dutra Da Silva R.": "Silva R.",
                         "Al Mutawa J.": "Mutawa J.", "Bautista R.": "Agut R.",
                         "Van D. Merwe I.": "Van der Merwe I.", "Ali Mutawa J.M.": "Mutawa J.",
                         "Zayed M. S.": "Zayed S.", "Carreno-Busta P.": "Busta P.", "Estrella Burgos V.": "Burgos V.",
                         "Artunedo Martinavarro A.": "Martinavarro A.", "Carballes Baena R.": "Baena R.",
                         "Vega Hernandez D.": "Hernandez D.", "Zayid M. S.": "Zayid M.",
                         "Munoz De La Nava D.": "de la Nava D.", "Carreno Busta P.": "Busta P.",
                         "Bautista Agut R.": "Agut R."}
    for old_name, new_name in handle_correction.items():
        if old_name in winner_names:
            winner_dict[old_name] = new_name
        if old_name in loser_names:
            loser_dict[old_name] = new_name

    co_uk_df["Winner"] = co_uk_df["Winner"].map(winner_dict)
    co_uk_df["Loser"] = co_uk_df["Loser"].map(loser_dict)


def determine_name_parts(name):
    articles = ["de", "el", "di", "del", "van", "der"]
    clean_name = name.replace("jr.", "")
    splitted_name = clean_name.split()
    if len(splitted_name) == 2:
        first_name = splitted_name[0]
        second_name = splitted_name[1]
    else:
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


def create_players(session, atp_df):
    atp_df["winner_first_name"] = atp_df["winner_first_name"].apply(lambda x: x.lower())
    atp_df["winner_second_name"] = atp_df["winner_second_name"].apply(lambda x: x.lower())
    atp_df["loser_first_name"] = atp_df["loser_first_name"].apply(lambda x: x.lower())
    atp_df["loser_second_name"] = atp_df["loser_second_name"].apply(lambda x: x.lower())

    atp_df["winner_full_name"] = atp_df.apply(lambda x: "{0} {1}".format(x["winner_first_name"],
                                                                         x["winner_second_name"]), axis=1)
    atp_df["loser_full_name"] = atp_df.apply(lambda x: "{0} {1}".format(x["loser_first_name"], x["loser_second_name"]),
                                             axis=1)
    unique_winner_names = set(atp_df["winner_full_name"].unique())
    unique_loser_names = set(atp_df["loser_full_name"].unique())
    unique_names = unique_winner_names.union(unique_loser_names)

    for name in unique_names:
        first_name, second_name = determine_name_parts(name)
        atp_df.loc[atp_df["winner_full_name"] == name, "winner_second_name"] = second_name
        atp_df.loc[atp_df["winner_full_name"] == name, "winner_first_name"] = first_name
        atp_df.loc[atp_df["loser_full_name"] == name, "loser_second_name"] = second_name
        atp_df.loc[atp_df["loser_full_name"] == name, "loser_first_name"] = first_name
        if not session.query(Player).filter(Player.first_name == first_name, Player.second_name == second_name).count():
            player = Player(first_name=first_name, second_name=second_name)
            session.add(player)
    session.commit()


def create_matches(session, atp_df, co_uk_df, converted_year):

    def get_atp_row(atp_df, co_uk_row_data):
        def clean_name(name):
            splitted_name = name.split()[: -1]
            return " ".join(splitted_name).lower()

        winner_second_name = clean_name(co_uk_row_data["Winner"])
        loser_second_name = clean_name(co_uk_row_data["Loser"])
        atp_row = atp_df[(atp_df["tournament"] == co_uk_row_data["Tournament"]) &
                         (atp_df["winner_second_name"] == winner_second_name) &
                         (atp_df["loser_second_name"] == loser_second_name)]
        if len(atp_row) != 1:
            print(atp_row)
            print(co_uk_row_data)
            print(winner_second_name)
            print(loser_second_name)
            # print(atp_df["loser_second_name"].unique())

        assert len(atp_row) == 1
        return atp_row

    def get_games(co_uk_row):
        games = []
        co_uk_games = co_uk_row["W1": "L5"]
        for game_num in range(1, 6):
            winner_col_name = "W{0}".format(game_num)
            loser_col_name = "L{0}".format(game_num)
            winner_points = co_uk_games[winner_col_name]
            loser_points = co_uk_games[loser_col_name]
            if pd.isnull(winner_points) or pd.isnull(loser_points):
                if (pd.isnull(winner_points) and not pd.isnull(loser_points)) or \
                   (not pd.isnull(winner_points) and pd.isnull(loser_points)):
                    raise Exception("Strange games points for row - {0}".format(co_uk_row))
                break
            winner_points = int(winner_points)
            loser_points = int(loser_points)
            if (winner_points == 5 and loser_points == 6) or winner_points == loser_points == 0:
                raise ValueError()
            try:
                game = session.query(Game).filter(Game.winner_points == winner_points,
                                                  Game.loser_points == loser_points).one()
            except NoResultFound:
                print(co_uk_row)
                raise Exception("Need to add new game. Winner points - {0}, loser points - {1}".format(winner_points,
                                                                                                       loser_points))
            games.append(game)
        return games

    def get_players(atp_row):
        winner = session.query(Player).filter(Player.first_name == atp_row["winner_first_name"].values[0],
                                              Player.second_name == atp_row["winner_second_name"].values[0])
        loser = session.query(Player).filter(Player.first_name == atp_row["loser_first_name"].values[0],
                                             Player.second_name == atp_row["loser_second_name"].values[0])
        if winner.count() == 1 and loser.count() == 1:
            winner = winner[0]
            loser = loser[0]
            return winner, loser
        else:
            raise Exception("Strange player queries for atp_row - {0}, winner count - {1}, loser count - {2}"
                            .format(atp_row, winner.count(), loser.count()))

    def get_odds(co_uk_row):
        winner_odd = None
        loser_odd = None
        if "AvgW" in co_uk_row.index.values:
            if not pd.isnull(winner_odd):
                winner_odd = co_uk_row["AvgW"]
            if not pd.isnull(winner_odd):
                loser_odd = co_uk_row["AvgL"]

        if "AvgW" not in co_uk_row.index.values or (not winner_odd or not loser_odd):
            col_names = co_uk_row["Comment":].index.values

            winner_odd = 0
            loser_odd = 0
            delim_w = 0
            delim_l = 0
            for name in col_names[1:]:
                if not pd.isnull(co_uk_row[name]) and "Max" not in name:
                    if name[-1] == "W":
                        winner_odd += co_uk_row[name]
                        delim_w += 1
                    elif name[-1] == "L":
                        loser_odd += co_uk_row[name]
                        delim_l += 1
                    else:
                        raise Exception("Strange odd name - {0}".format(name))

            if not delim_w or not delim_l:
                raise ValueError("No odds or odd number of odds for row - {0}".format(co_uk_row))

            winner_odd /= delim_w
            loser_odd /= delim_l
        return winner_odd, loser_odd

    co_uk_df = co_uk_df[(co_uk_df["Comment"] == "Completed") & (~pd.isnull(co_uk_df["Tournament"]) &
                        (co_uk_df["Round"] != "Round Robin") & (co_uk_df["Winner"] != "fnisk"))]
    dropped_matches = {2006: [159, 1486, 1511, 1919, 2147], 2007: [240, 387, 558, 581, 1763, 2552],
                       2008: [43, 602, 621, 714, 1060, 1061, 1064, 1073, 1076, 2691], 2009: [61, 195, 218, 1666],
                       2010: [1241, 1299], 2011: [1286], 2012: [529, 547], 2013: [1040, 2162, 2176], 2014: [971, 1914],
                       2015: [2287, 2293], 2016: list(range(1382, 1409))}
    for co_uk_row in co_uk_df.iterrows():
        co_uk_row_data = co_uk_row[1]
        if co_uk_row_data.name not in dropped_matches[converted_year]:
            need_to_save = True
            atp_row = get_atp_row(atp_df, co_uk_row_data)

            date = co_uk_row_data["Date"].date()
            tournament = atp_row["tournament"].values[0]
            stage = atp_row["stage_name"].values[0]
            surface = co_uk_row_data["Surface"].lower()
            best_of = co_uk_row_data["Best of"]
            try:
                games = get_games(co_uk_row_data)
                winner_rank = int(co_uk_row_data["WRank"])
                loser_rank = int(co_uk_row_data["LRank"])
                winner_odd, loser_odd = get_odds(co_uk_row_data)
            except ValueError:
                need_to_save = False

            winner_service = atp_row["winner_service_share"].values[0]
            loser_service = atp_row["loser_service_share"].values[0]
            winner_return = atp_row["winner_return_share"].values[0]
            loser_return = atp_row["loser_return_share"].values[0]

            winner, loser = get_players(atp_row)
            if need_to_save:
                try:
                    match = Match(date=date, tournament=tournament, stage=stage, surface=surface, best_of=best_of,
                                  games=games, winner_rank=winner_rank, loser_rank=loser_rank, winner_odd=winner_odd,
                                  loser_odd=loser_odd, winner_service=winner_service, loser_service=loser_service,
                                  winner_return=winner_return, loser_return=loser_return, winner_id=winner.player_id,
                                  loser_id=loser.player_id)
                except:
                    print(co_uk_row_data)
                    print(winner_odd)
                    raise Exception()
                session.add(match)

                winner = session.query(Player).filter(Player.second_name == atp_row["winner_second_name"].values[0],
                                                      Player.first_name == atp_row["winner_first_name"].values[0]).one()
                loser = session.query(Player).filter(Player.second_name == atp_row["loser_second_name"].values[0],
                                                     Player.first_name == atp_row["loser_first_name"].values[0]).one()
                winner.matches.append(match)
                loser.matches.append(match)
    session.commit()


if __name__ == '__main__':
    converted_years = list(range(2006, datetime.datetime.now().year))
    engine = create_engine('sqlite:///tennis_model.db')

    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()
    init_games(session)

    for converted_year in converted_years:
        print(converted_year)
        atp_df, co_uk_df = load_data(converted_year)

        compare_tournament_names(atp_df, co_uk_df, converted_year)
        correct_player_names(co_uk_df)
        create_players(session, atp_df)
        create_matches(session, atp_df, co_uk_df, converted_year)
