from datetime import time

import numpy as np
import pandas as pd
import pytest

from trading_calendars import get_calendar
from zipline.data._minute_bar_internal import (
    find_last_traded_position_internal, find_position_of_minute, minute_value)
from zipline.gens.sim_engine import NANOS_IN_MINUTE

TEST_CALENDAR_START = pd.Timestamp('2020-08-27', tz='UTC')
TEST_CALENDAR_STOP = pd.Timestamp('2020-08-28', tz='UTC')
MINUTES_PER_DAY = 240
LOCAL_TZ = 'Asia/Shanghai'


@pytest.fixture
def calendar():
    yield get_calendar('XSHG')


def test_minute_value(calendar):
    cal = calendar.schedule.loc[TEST_CALENDAR_START:TEST_CALENDAR_STOP]

    market_opens = cal.market_open
    market_open_values = market_opens.values.astype('datetime64[m]').astype(
        np.int64)

    # 测试期间的所有分钟
    all_minutes = calendar.minutes_window(market_opens.iloc[0],
                                          MINUTES_PER_DAY * len(cal))
    for i, expected in enumerate(all_minutes):
        actual = minute_value(market_open_values, i, MINUTES_PER_DAY)
        actual = pd.Timestamp(actual, unit='m', tz='utc')
        assert actual == expected


def test_find_position_of_minute(calendar):
    cal = calendar.schedule.loc[TEST_CALENDAR_START:TEST_CALENDAR_STOP]

    market_opens = cal.market_open
    market_closes = cal.market_close

    market_open_values = market_opens.values.astype('datetime64[m]').astype(
        np.int64)
    market_closes_values = market_closes.values.astype('datetime64[m]').astype(
        np.int64)

    # 测试期间的所有分钟
    all_minutes = calendar.minutes_window(market_opens.iloc[0],
                                          MINUTES_PER_DAY * len(cal))

    for i in range(len(all_minutes)):
        minute_dt = all_minutes[i]
        pos = find_position_of_minute(
            market_open_values,
            market_closes_values,
            minute_dt.value / NANOS_IN_MINUTE,
            MINUTES_PER_DAY,
            False,
        )
        assert i == pos


def test_convert_minute(calendar):
    cal = calendar.schedule.loc[TEST_CALENDAR_START:TEST_CALENDAR_STOP]

    market_opens = cal.market_open
    market_closes = cal.market_close

    market_open_values = market_opens.values.astype('datetime64[m]').astype(
        np.int64)
    market_closes_values = market_closes.values.astype('datetime64[m]').astype(
        np.int64)

    # 测试期间的所有分钟
    all_minutes = calendar.minutes_window(market_opens.iloc[0],
                                          MINUTES_PER_DAY * len(cal))
    # 首先查找分钟对应位置，然后根据位置找回分钟epoch，转换后测试是否一致
    for i in range(len(all_minutes)):
        minute_dt = all_minutes[i]
        pos = find_position_of_minute(
            market_open_values,
            market_closes_values,
            minute_dt.value / NANOS_IN_MINUTE,
            MINUTES_PER_DAY,
            False,
        )
        minute_epoch = minute_value(market_open_values, pos, MINUTES_PER_DAY)
        finded = pd.Timestamp(minute_epoch, tz='UTC', unit="m")
        assert minute_dt == finded


def test_NoDataOnDate(calendar):
    cal = calendar.schedule.loc[TEST_CALENDAR_START:TEST_CALENDAR_STOP]

    market_opens = cal.market_open
    market_closes = cal.market_close

    market_open_values = market_opens.values.astype('datetime64[m]').astype(
        np.int64)
    market_closes_values = market_closes.values.astype('datetime64[m]').astype(
        np.int64)
    # 午夜时分
    dt_1 = pd.Timestamp('2020-01-08', tz='UTC')

    try:
        find_position_of_minute(
            market_open_values,
            market_closes_values,
            dt_1.value / NANOS_IN_MINUTE,
            MINUTES_PER_DAY,
            False,
        )
    except ValueError:
        pass
    # `15：00` 为最后交易分钟
    dt_2 = pd.Timestamp('2020-08-28 15:02', tz=LOCAL_TZ).tz_convert('UTC')

    try:
        find_position_of_minute(
            market_open_values,
            market_closes_values,
            dt_2.value / NANOS_IN_MINUTE,
            MINUTES_PER_DAY,
            False,
        )
    except ValueError:
        pass
