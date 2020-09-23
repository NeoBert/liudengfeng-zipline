# 完成测试 ✔

import pandas as pd
import pytest
from numpy.testing import assert_array_almost_equal
from cnswd.utils import sanitize_dates
from trading_calendars import get_calendar
from zipline.data.bundles.wy_data import fetch_single_equity, WY_DAILY_COL_MAPS

START_DATE = '2000-01-01'
END_DATE = '2019-09-30'


def read_stock_daily(stock_code):
    start, end = sanitize_dates(START_DATE, END_DATE)
    pf = f'tests/resources/cndata/stock_daily/{stock_code}.csv'
    df = pd.read_csv(pf, encoding='gb2312', na_values=['-', None])
    df = df[WY_DAILY_COL_MAPS.keys()]
    df.rename(columns=WY_DAILY_COL_MAPS, inplace=True)
    df = df.sort_values('date')
    df['date'] = pd.to_datetime(df['date'])
    cond = df['date'].between(start, end)
    df = df.loc[cond, :]
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
    actual_df = fetch_single_equity(stock_code, START_DATE, END_DATE)
    actual_df['change_pct'].fillna(0.0, inplace=True)
    # 测试正确复权
    assert_array_almost_equal(actual_df.b_close.pct_change()[1:],
                              actual_df['change_pct'][1:] / 100.0,
                              decimal=4)
    # 测试交易日内数据完整
    assert_array_almost_equal(actual_df.b_close.pct_change()[1:],
                              base_df['change_pct'][start_id:] / 100.0,
                              decimal=4)

    # 不得包含nan
    assert not actual_df.b_close.hasnans
    # 测试所有交易日内数据完整
    expected = len(calendar.sessions_in_range(start_date, end_date))
    actual = len(actual_df)
    assert expected == actual


# 各市场板块至少选择一只股票
@pytest.mark.parametrize("code", ['000333', '300033', '600645', '688001'])
def test_fetch_single_equity(code):
    """以涨跌幅测试股票除权及数据完整性"""
    calendar = get_calendar('XSHG')
    _test_fetch_single_equity(code, calendar)


def test_adj():
    """测试除权调整"""
    # '603279' 正好在2019-09-30除权
    df = fetch_single_equity('603279', '2019-01-01', END_DATE)
    cond = df['date'] == pd.Timestamp(END_DATE)
    assert df[cond].close.values[0] != df[cond].b_close.values[0]
    # 除最后一行外，其余各行都相等
    ohlc = df[['open', 'high', 'low', 'close']].iloc[:-1, :]
    adj_ohlc = df[['b_open', 'b_high', 'b_low', 'b_close']].iloc[:-1, :]
    assert_array_almost_equal(ohlc.values, adj_ohlc.values, decimal=2)
