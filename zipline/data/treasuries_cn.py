"""
国库券资金成本数据
"""
import pandas as pd

from cnswd.utils import sanitize_dates
from cnswd.websource.treasuries import EARLIEST_POSSIBLE_DATE
# from cnswd.store import TreasuryDateStore
from cnswd.mongodb import get_db


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


def get_treasury_data(start, end):
    """期间国库券利率

    Arguments:
        start {date like} -- 开始日期
        end {date like} -- 结束日期

    Returns:
        DataFrame -- 期间利率

    Example:
    >>> start, end = '2020-03-10', '2020-03-15'
    >>> get_treasury_data(start, end).iloc[:3,:5]
                                cash    1month    2month    3month    6month
    date
    2020-03-10 00:00:00+00:00  0.016000  0.016231  0.016610  0.016661  0.016991 
    2020-03-11 00:00:00+00:00  0.016000  0.016727  0.016996  0.017001  0.017211 
    2020-03-12 00:00:00+00:00  0.015742  0.016195  0.016993  0.016994  0.017625 
    2020-03-13 00:00:00+00:00  0.014287  0.016395  0.016699  0.016705  0.017953
    """
    start, end = sanitize_dates(start, end)
    db = get_db()
    collection = db['国债利率']
    predicate = {
        'date': {"$gte": start, "$lte": end}
    }
    projection = {"_id": 0}
    sort = [("日期", 1)]
    df = pd.DataFrame.from_records(
        collection.find(predicate, projection, sort=sort))
    # df.set_index('date', inplace=True)
    df.index = pd.DatetimeIndex(df.pop('date'))
    # 缺少2年数据，使用简单平均插值
    value = (df['y1'] + df['y3']) / 2
    df.insert(7, '2year', value)
    df.rename(columns=TREASURY_COL_MAPS, inplace=True)
    return df.tz_localize('UTC')
