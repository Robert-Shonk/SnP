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
    con_dfs = con_dfs.drop(columns=['index', 'Unnamed: 0'])

    return con_dfs