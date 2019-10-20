"""

雅虎财经

"""
import os
from pathlib import Path

import pandas as pd

from cnswd.utils import data_root

YAHOO_ITEMS = ['annual_ebitda', 'annual_free_cash_flow', 'annual_total_assets',
               'quarterly_ebitda', 'quarterly_free_cash_flow', 'quarterly_total_assets']


def _fixed_data(df):
    df['date'] = df['date'].map(lambda x: pd.Timestamp(x))
    df['sid'] = df['symbol'].map(lambda x: int(x))
    df.loc[:, 'asof_date'] = df.loc[:, 'date']
    df.loc[:, 'timestamp'] = df.loc[:, 'date'] + pd.Timedelta(days=45)
    df.drop(columns=['symbol', 'date'], inplace=True)
    return df


def get_sub_dir(item):
    root = data_root('yahoo')
    sub_root = os.path.join(root, item)
    return Path(sub_root)


def read_item_data(item):
    p = get_sub_dir(item)
    fns = p.glob('??????.pkl')
    dfs = [pd.read_pickle(str(f)) for f in fns]
    df = pd.concat(dfs, sort=False)
    return _fixed_data(df).sort_values(['sid', 'asof_date'])
