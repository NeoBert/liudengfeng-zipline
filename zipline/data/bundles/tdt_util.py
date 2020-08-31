from cnswd.mongodb import get_db
import pandas as pd


def get_trading_dates():
    db = get_db()
    trading_dates = db['交易日历'].find_one()['tdates']
    trading_dates = pd.to_datetime(trading_dates, utc=True)
    return trading_dates


def search_most_recent_dt(dt, side='left'):
    """最接近指定方向的交易日期"""
    dts = get_trading_dates()
    loc = dts.searchsorted(dt, side=side)
    return dts[loc]
