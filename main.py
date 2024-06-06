import pandas as pd
from src import main
from multiprocessing import Manager, Pool
from config import PROCESSES, PATH_TO_MARKET, PATH_TO_USER, RES_PATH



if __name__ == "__main__":
    df_market = pd.read_csv(PATH_TO_MARKET)
    df_user = pd.read_csv(PATH_TO_USER)

    users = df_user["user_id"].unique()
    user_to_group = dict()
    pos = 0
    for i, user in enumerate(users):
        user_to_group[user] = i % PROCESSES
    ind = [[] for _ in range(PROCESSES)]
    for i, row in df_user.iterrows():
        pos = user_to_group[row["user_id"]]
        ind[pos].append(i)
    dfs = [df_user.loc[ind[i]].reset_index(drop=True) for i in range(PROCESSES)]

    manager = Manager()
    q = manager.Queue()
    pool_input = [(q, df_market, dfs[i]) for i in range(PROCESSES)]
    pool = Pool(processes=PROCESSES)
    pool.starmap(main, pool_input)
    pool.close()
    pool.join()

    dfs_res = [q.get() for i in range(PROCESSES)]
    df_res = pd.concat(dfs_res).reset_index(drop=True).round(4)
    df_res.to_csv(RES_PATH)
