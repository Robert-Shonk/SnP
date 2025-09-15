import pandas as pd

def agg_30_days(frame=None):
    if frame is None:
        frame = pd.read_csv('data/csv/30days.csv')

    stats = frame.groupby('symbol').agg({'close':['mean', 'std', 'min', 'max']}).reset_index()

    dates = []
    for sym in list(stats['symbol']):
        dates.append(pd.to_datetime(frame.query('symbol == @sym')['date'].iloc[0]))

    stats['dates'] = dates

    new_cols = []
    for col in stats.columns:
        if col[1] == '':
            new_cols.append(col[0])
        else:
            new_cols.append('_'.join(col)+'_30days')

    stats.columns = new_cols

    return stats


def sectors(snp=None, frame=None):
    if snp is None:
        snp = pd.read_csv('data/csv/snp_list.csv')

    if frame is None:
        frame = pd.read_csv('data/csv/30days.csv')

    symbols = snp['symbol']

    changes = []
    dates = []
    for sym in symbols:
        if '.' in sym:
            sym = sym.replace('.', '-')
        f = frame.query('symbol == @sym')
        changes.append(((f.iloc[0]['close'] / f.iloc[1]['close']) - 1)*100)
        dates.append(pd.to_datetime(f.query('symbol == @sym')['date'].iloc[0]))

    df_join = snp.join(frame, lsuffix='_snp', rsuffix='_30days').drop(columns=['symbol_30days'])
    df_join['change'] = changes
    df_join['dates'] = dates
    
    return df_join