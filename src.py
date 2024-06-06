import pandas as pd
import numpy as np
from config import PERIOD
from tqdm.auto import tqdm
from multiprocessing import Queue


def left_end(tim):
    return tim // PERIOD * PERIOD


def right_end(tim):
    return tim // PERIOD * PERIOD + PERIOD


def find_start_end(df):
    start = left_end(df.iloc[0]["timestamp"])
    end = left_end(df.iloc[-1]["timestamp"])
    return start, end


def timestamp_pos(timestamp, start_user):
    return (timestamp - start_user) // PERIOD

def pos_timestamp(pos, start_user):
    return start_user + pos * PERIOD


def main(q: Queue, df_market: pd.DataFrame, df_user: pd.DataFrame):
    start_user, end_user = find_start_end(df_user)
    start_market, end_market = find_start_end(df_market)

    users = df_user["user_id"].unique()
    currencies = df_user["currency"].unique()
    user_balance = {
        user: 0
        for user in users
    }
    user_currencies = {
        user: {
            currency: 0
            for currency in currencies
        }
        for user in users
    }
    prices = {
        currency: 0
        for currency in currencies
    }

    num_timestamps = len(range(start_user, end_market, PERIOD)) + 1
    MIN_BALANCE = 0
    MAX_BALANCE = 1
    AVG_BALANCE = 2
    database = {
        user: np.zeros((num_timestamps, 3))
        for user in users
    }

    all_timestamps = []
    for i, row in df_user.iterrows():
        all_timestamps.append((row["timestamp"], i, 'u'))
    for i, row in df_market.iterrows():
        all_timestamps.append((row["timestamp"], i, 'm'))
    for i in range(start_user + PERIOD, end_market + PERIOD, PERIOD):
        all_timestamps.append((i, -1, 's'))
    all_timestamps.sort()

    for cur_timestamp in tqdm(all_timestamps):
        timestamp, ind, typ = cur_timestamp
        left_timestamp_pos = timestamp_pos(left_end(timestamp), start_user)
        if typ == 's':
            for user in users:
                database[user][left_timestamp_pos][AVG_BALANCE] = user_balance[user]
                database[user][left_timestamp_pos][MIN_BALANCE] = user_balance[user]
                database[user][left_timestamp_pos][MAX_BALANCE] = user_balance[user]
        elif typ == 'u':
            data = df_user.iloc[ind]
            user, currency, delta = data["user_id"], data["currency"], data["delta"]
            good_timestamp = timestamp >= start_user and timestamp <= end_market
            if good_timestamp:
                database[user][left_timestamp_pos][AVG_BALANCE] += delta * prices[currency] * (right_end(timestamp) - timestamp) / PERIOD
            user_balance[user] += delta * prices[currency]
            user_currencies[user][currency] += delta
            if good_timestamp:
                database[user][left_timestamp_pos][MAX_BALANCE] = max(database[user][left_timestamp_pos][MAX_BALANCE], user_balance[user])
                database[user][left_timestamp_pos][MIN_BALANCE] = min(database[user][left_timestamp_pos][MIN_BALANCE], user_balance[user])
        else:
            data = df_market.iloc[ind]
            symbol, price = data["symbol"], data["price"]
            currency = symbol[:3]
            good_timestamp = timestamp >= start_user and timestamp <= end_market
            for user in users:
                if good_timestamp:
                    database[user][left_timestamp_pos][AVG_BALANCE] += (price - prices[currency]) * user_currencies[user][currency] * (right_end(timestamp) - timestamp) / PERIOD
                user_balance[user] += (price - prices[currency]) * user_currencies[user][currency]
                if good_timestamp:
                    database[user][left_timestamp_pos][MAX_BALANCE] = max(database[user][left_timestamp_pos][MAX_BALANCE], user_balance[user])
                    database[user][left_timestamp_pos][MIN_BALANCE] = min(database[user][left_timestamp_pos][MIN_BALANCE], user_balance[user])
                prices[currency] = price

    mas = []
    for user in database.keys():
        user_data = database[user]
        for i in range(len(user_data)):
            mas.append([user, user_data[i][MIN_BALANCE], user_data[i][MAX_BALANCE], user_data[i][AVG_BALANCE], pos_timestamp(i, start_user)])
    columns = ["user_id", "minimum_balance", "maximum_balance", "average_balance", "start_timestamp"]
    df_res = pd.DataFrame(mas, columns=columns)

    q.put(df_res)
