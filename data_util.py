import pandas as pd

def calc_moves(symbols, stocks_df):
    df_list = []

    for sym in symbols:
        df = stocks_df.query("symbol == @sym").sort_values("date")

        moves = [0]
        for i in range(1, len(df)):
            m = ((df.iloc[i]['close'] / df.iloc[i-1]['close']) - 1) * 100
            moves.append(m)

        df['moves'] = moves

        df_list.append(df[1:]) # start at 1 to not include last day of 2024.

    # concat list of df into one dataframe
    con_dfs = pd.concat(df_list).reset_index()
    con_dfs = con_dfs.drop(columns=['index'])

    return con_dfs


# takes list of strings
# return dictionary of two lists. one of recently added stocks. the other of old stocks that need be to removed from db.
# if no new stocks (new list is same as list in db) return -1
def compare_snp(new_symbols):
    old_snp = pd.read_csv("data/csv/snp_list.csv")
    old_symbols = old_snp["symbol"]

    new_diff = [] # stocks that will need to have ytd scraped and added to stock table.
    old_diff = [] # stocks to be removed from db.

    for ns in new_symbols:
        if ns not in old_symbols:
            new_diff.append(ns)

    if len(new_diff) == 0:
        return -1
    
    for os in old_symbols:
        if os not in new_symbols:
            old_diff.append(os)


    return { "new_stocks": new_diff, "old_stocks": old_diff }