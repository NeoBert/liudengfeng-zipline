"""
国库券资金成本数据
"""
import pandas as pd

from cnswd.utils import sanitize_dates
from cnswd.sql.szsh import Treasury
from cnswd.sql.base import session_scope
from cnswd.websource.treasuries import EARLIEST_POSSIBLE_DATE
TREASURY_COL_NAMES = ['date', '1month', '3month', '6month', '1year', '2year', '3year',
                      '5year', '7year', '10year', '20year', '30year']


def earliest_possible_date():
    """
    The earliest date for which we can load data from this module.
    """
    return EARLIEST_POSSIBLE_DATE


def get_treasury_data(start_date, end_date):
    start_date, end_date = sanitize_dates(start_date, end_date)
    # 确保为date类型
    start_date = pd.Timestamp(start_date).date()
    end_date = pd.Timestamp(end_date).date()
    with session_scope('szsh') as sess:
        query = sess.query(
            Treasury.date,
            Treasury.m1,
            Treasury.m3,
            Treasury.m6,
            Treasury.y1,
            Treasury.y3,
            Treasury.y5,
            Treasury.y7,
            Treasury.y10,
            Treasury.y20,
            Treasury.y30
        ).filter(Treasury.date.between(start_date, end_date))
        df = pd.DataFrame.from_records(query.all())
        # 缺少2年数据，使用简单平均插值
        value = (df.iloc[:,4] + df.iloc[:, 5]) / 2
        df.insert(5,'2year',value)
        df.columns = TREASURY_COL_NAMES
        df.set_index(keys='date', inplace=True)
        df.index = pd.DatetimeIndex(df.index)
    return df.tz_localize('UTC')
