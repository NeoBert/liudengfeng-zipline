from pathlib import Path

import pandas as pd
import pytest
from numpy.testing import assert_array_almost_equal
from trading_calendars import get_calendar

from zipline.data.sqldata import fetch_single_equity

DAILY_COLS = ['date', 'symbol', 'name',
              'close', 'high', 'low', 'open',
              'prev_close', 'change', 'change_pct',
              'turnover', 'volume', 'amount', 'total_cap', 'market_cap']
END_DATE = '2019-09-30'
TEST_CODES = ['000333', '300033', '600645', '688001']


def read_stock_daily(stock_code):
    pf = f'tests/resources/cndata/stock_daily/{stock_code}.csv'
    df = pd.read_csv(pf, encoding='gb2312', na_values=['-', None])
    df.columns = DAILY_COLS
    df = df.sort_values('date')
    df['change_pct'].fillna(0.0, inplace=True)
    return df


def _test_fetch_single_equity(stock_code, calendar):
    base_df = read_stock_daily(stock_code)
    start_id = 1
    if stock_code == '300033':
        start_id = 2
        start_date = pd.Timestamp(base_df.date.values[1])
    else:
        start_date = pd.Timestamp(base_df.date.values[0])
    end_date = pd.Timestamp(base_df.date.values[-1])
    actual_df = fetch_single_equity(stock_code, '1990-01-01', END_DATE)
    actual_df['change_pct'].fillna(0.0, inplace=True)
    # 测试正确复权
    assert_array_almost_equal(actual_df.b_close.pct_change()[
                              1:], actual_df['change_pct'][1:] / 100.0, decimal=4)
    assert_array_almost_equal(actual_df.b_close.pct_change()[
                              1:], base_df['change_pct'][start_id:] / 100.0, decimal=4)

    # 不得包含nan
    assert not actual_df.b_close.hasnans
    # 测试所有交易日内数据完整
    expected = len(calendar.sessions_in_range(start_date, end_date))
    actual = len(actual_df)
    assert expected == actual


def test_fetch_single_equity():
    calendar = get_calendar('XSHG')
    for stock_code in TEST_CODES:
        _test_fetch_single_equity(stock_code, calendar)


def test_adj():
    # '603279' 正好在2019-09-30除权
    df = fetch_single_equity('603279', '2019-01-01', END_DATE)
    cond = df['date'] == pd.Timestamp(END_DATE)
    assert df[cond].close.values[0] != df[cond].b_close.values[0]
    # 除最后一行外，其余各行都相等
    ohlc = df[['open', 'high', 'low', 'close']].iloc[:-1, :]
    adj_ohlc = df[['b_open', 'b_high', 'b_low', 'b_close']].iloc[:-1, :]
    assert_array_almost_equal(ohlc.values, adj_ohlc.values, decimal=2)
