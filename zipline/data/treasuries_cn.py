"""
国库券资金成本数据
"""
import pandas as pd

from cnswd.utils import sanitize_dates
from cnswd.reader import treasury
from cnswd.websource.treasuries import EARLIEST_POSSIBLE_DATE

TREASURY_COL_MAPS = {
    'm0': 'cash',
    'm1': '1month',
    'm2': '2month',
    'm3': '3month',
    'm6': '6month',
    'm9': '9month',
    'y1': '1year',
    'y3': '3year',
    'y5': '5year',
    'y7': '7year',
    'y10': '10year',
    'y15': '15year',
    'y20': '20year',
    'y30': '30year',
    'y40': '40year',
    'y50': '50year',
}


def earliest_possible_date():
    """
    The earliest date for which we can load data from this module.
    """
    return EARLIEST_POSSIBLE_DATE


def get_treasury_data(start_date, end_date):
    df = treasury(start_date, end_date)
    # 缺少2年数据，使用简单平均插值
    value = (df['y1'] + df['y3']) / 2
    df.insert(7, '2year', value)
    df.rename(columns=TREASURY_COL_MAPS, inplace=True)
    df.index = pd.DatetimeIndex(df.index)
    return df.tz_localize('UTC')
