# -*- coding: utf-8 -*-

"""

"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from tennis_model.db_convertor.db_convertor import Match, Player

NUMBER_LAST_MATCHES = 10


def calculate_probability_of_winning_game(p):
    return (15 - 4 * p - 10 * p ** 2 /(1 - 2 * p * (1 - p))) * p ** 4

if __name__ == '__main__':
    engine = create_engine('sqlite:///../db_convertor/tennis_model.db')
    Session = sessionmaker(bind=engine)
    session = Session()

    for match in session.query(Match)[: 1000]:
        winner_service = match.winner.get_mean_probability(NUMBER_LAST_MATCHES, match.surface, match.date, "service")
        loser_service = match.loser.get_mean_probability(NUMBER_LAST_MATCHES, match.surface, match.date, "service")
        winner_return = match.winner.get_mean_probability(NUMBER_LAST_MATCHES, match.surface, match.date, "return")
        loser_return = match.loser.get_mean_probability(NUMBER_LAST_MATCHES, match.surface, match.date, "return")

        if winner_service is not None and loser_service is not None and winner_return is not None and \
            loser_return is not None:
            print(winner_service, loser_service, winner_return, loser_return)

            g_w = calculate_probability_of_winning_game(winner_service)
            g_l = calculate_probability_of_winning_game(loser_service)

            print(g_w, g_l)