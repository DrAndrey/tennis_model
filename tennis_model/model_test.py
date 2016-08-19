# -*- coding: utf-8 -*-

"""

"""

import numpy as np

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from tennis_model.db_convertor.db_convertor import Match

NUMBER_LAST_MATCHES = 3

table_a = np.array([[1, 3, 0, 4, 0, 0],
                    [3, 3, 1, 4, 0, 0],
                    [4, 4, 0, 3, 1, 0],
                    [6, 3, 2, 4, 0, 0],
                    [16, 4, 1, 3, 1, 0],
                    [6, 5, 0, 2, 2, 0],
                    [10, 2, 3, 5, 0, 0],
                    [40, 3, 2, 4, 1, 0],
                    [30, 4, 1, 3, 2, 0],
                    [4, 5, 0, 2, 3, 0],
                    [5, 1, 4, 6, 0, 0],
                    [50, 2, 3, 5, 1, 0],
                    [100, 3, 2, 4, 2, 0],
                    [50, 4, 1, 3, 3, 0],
                    [5, 5, 0, 2, 4, 0],
                    [1, 1, 5, 6, 0, 0],
                    [30, 2, 4, 5, 1, 0],
                    [150, 3, 3, 4, 2, 0],
                    [200, 4, 2, 3, 3, 0],
                    [75, 5, 1, 2, 4, 0],
                    [6, 6, 0, 1, 5, 0],
                    [1, 0, 6, 6, 0, 1],
                    [36, 1, 5, 5, 1, 1],
                    [225, 2, 4, 4, 2, 1],
                    [400, 3, 3, 3, 3, 1],
                    [225, 4, 2, 2, 2, 1],
                    [36, 5, 1, 1, 5, 1],
                    [1, 6, 0, 0, 6, 1]])

table_b = np.array([[1, 3, 0, 3, 0, 0],
                    [3, 3, 1, 3, 0, 0],
                    [3, 4, 0, 2, 1, 0],
                    [6, 2, 2, 4, 0, 0],
                    [12, 3, 1, 3, 1, 0],
                    [3, 4, 0, 2, 2, 0],
                    [4, 2, 3, 4, 0, 0],
                    [24, 3, 2, 3, 1, 0],
                    [24, 4, 1, 2, 2, 0],
                    [4, 5, 0, 1, 3, 0],
                    [5, 1, 4, 5, 0, 0],
                    [40, 2, 3, 4, 1, 0],
                    [60, 3, 2, 3, 2, 0],
                    [20, 4, 1, 2, 3, 0],
                    [1, 5, 0, 1, 4, 0],
                    [1, 0, 5, 5, 0, 1],
                    [25, 1, 4, 4, 1, 1],
                    [100, 2, 3, 3, 2, 1],
                    [100, 3, 2, 2, 3, 1],
                    [25, 4, 1, 1, 4, 1],
                    [1, 5, 0, 0, 5, 1]])


def calculate_probability_of_winning_game(p):
    return (15 - 4 * p - 10 * p ** 2 /(1 - 2 * p * (1 - p))) * p ** 4


def calculate_d(p, q):
    return p * q / (1 - (p *(1 - q) + (1 - p) * q))


def calculate_probability_of_winning_tie_breaker(p, q):
    d = calculate_d(p, q)
    sum_value = table_a[:, 0] * p ** table_a[:, 1] * (1 - p) ** table_a[:, 2] * q ** table_a[:, 3] * \
                  (1 - q) ** table_a[:, 4] * d ** table_a[:, 5]
    return sum_value.sum()


def calculate_probability_of_winning_tiebreaker_set(p, q):
    g_p = calculate_probability_of_winning_game(p)
    g_q = calculate_probability_of_winning_game(q)
    t = calculate_probability_of_winning_tie_breaker(p, q)
    sum_value = table_b[:, 0] * g_p ** table_b[:, 1] * (1 - g_p) ** table_b[:, 2] * g_q ** table_b[:, 3] * \
                (1 - g_q) ** table_b[:, 4] * (g_p * g_q + (g_p * (1 - g_q) + (1 - g_p) * g_q) * t) ** table_b[:, 5]
    return sum_value.sum()


def calculate_probability_of_winning_3_set_match(p, q):
    s = calculate_probability_of_winning_tiebreaker_set(p, q)
    return s ** 2 * (1 + 2 * (1 - s))


def calculate_delta_ab(w_s, l_s, w_r, l_r):
    return (w_s - (1 - w_r)) - (l_s - (1 - l_r))


def calculate_mean_pr(w_s, l_s, w_r, l_r):
    pr = 0
    delta = calculate_delta_ab(w_s, l_s, w_r, l_r)
    for cur_delta in delta:
        m1 = calculate_probability_of_winning_3_set_match(0.6+cur_delta, 0.4)
        m2 = calculate_probability_of_winning_3_set_match(0.6, 0.4+cur_delta)
        pr += (m1 + m2) / 2
    return pr / len(delta)


def calculate_stake(match, w, l):
    winner_odd = 1 / w
    loser_odd = 1 / l

    if match.winner_odd > winner_odd and winner_odd < 2:
        return match.winner_odd - 1
    else:
        if match.loser_odd > loser_odd and loser_odd < 2:
            return -1
    # elif match.loser_odd > loser_odd and match.winner_odd > winner_odd:
    #     winner_ratio = (match.winner_odd - 1) / (winner_odd - 1)
    #     loser_ratio = (match.loser_odd - 1) / (loser_odd - 1)
    #     if winner_ratio > loser_ratio and winner_odd < 2:
    #         return match.winner_odd - 1
    #     elif winner_ratio < loser_ratio and loser_odd < 2:
    #         return -1

    return 0


if __name__ == '__main__':
    engine = create_engine('sqlite:///../db_convertor/tennis_model.db')
    Session = sessionmaker(bind=engine)
    session = Session()

    gain = 0
    n_siutable_matches = 0
    n_gains = 0
    for match in session.query(Match).all()[-3000:]:
        winner_service, winner_return = match.winner.get_mean_probability_for_common_opponents(NUMBER_LAST_MATCHES,
                                                                                               match.surface,
                                                                                               match.date, match.loser)
        loser_service, loser_return = match.loser.get_mean_probability_for_common_opponents(NUMBER_LAST_MATCHES,
                                                                                            match.surface, match.date,
                                                                                            match.winner)

        if winner_service is not None and loser_service is not None and winner_return is not None and \
            loser_return is not None:
                w_p = calculate_mean_pr(winner_service, loser_service, winner_return, loser_return)
                l_p = calculate_mean_pr(loser_service, winner_service, loser_return, winner_return)

                cur_gain = calculate_stake(match, w_p, l_p)
                gain += cur_gain
                n_siutable_matches += 1
                if cur_gain:
                    n_gains += 1

                print(gain, n_siutable_matches, n_gains)
