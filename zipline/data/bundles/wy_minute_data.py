"""
网易分钟级别数据

后台任务提取股票、指数实时报价数据特点

+ 每分钟采集一次数据
+ 定时采集已经排除午休时段
+ 指数数据 2020-09-28 开始
"""
from concurrent.futures.thread import ThreadPoolExecutor
from functools import lru_cache, partial

import pandas as pd
from cnswd.mongodb import get_db
from cnswd.setting.constants import MAX_WORKER
from trading_calendars import get_calendar

from .wy_data import _fetch_single_index


INDEX_QUOTE_START = pd.Timestamp('2020-11-02')


def encode_index_code(x, offset=1000000):
    i = int(x) + offset
    return str(i).zfill(7)


def decode_index_code(x, offset=1000000):
    i = int(x) - offset
    return str(i).zfill(6)


@lru_cache(None)
def tminutes(start, end):
    calendar = get_calendar('XSHG')
    fmt = r"%Y-%m-%d"
    sessions = calendar.sessions_in_range(
        start.strftime(fmt), end.strftime(fmt))
    return calendar.minutes_for_sessions_in_range(
        sessions[0], sessions[-1]
    ).tz_convert(calendar.tz).tz_localize(None)


def _single_minutely_equity(one_day, code, db=None, is_index=False):
    if db is None:
        db = get_db('wy_index_quotes') if is_index else get_db('wy_quotes')
    name = one_day.strftime(r"%Y-%m-%d")
    if name not in db.list_collection_names():
        return pd.DataFrame()
    collection = db[name]
    # 存在延时
    start = one_day.replace(hour=9, minute=30)
    end = one_day.replace(hour=15, minute=1)
    predicate = {
        'code': code,
        'time': {'$gte': start, '$lte': end},
    }
    projection = {
        'datetime': '$time',
        'close': '$price',
        'open': 1,
        'high': 1,
        'low': 1,
        'volume': 1,
        '_id': 0
    }
    sort = [('datetime', 1)]
    cursor = collection.find(predicate, projection=projection, sort=sort)
    df = pd.DataFrame.from_records(cursor)
    if df.empty:
        return df
    df['datetime'] = df['datetime'].dt.floor('T')
    df.drop_duplicates(['datetime'], keep='last', inplace=True)
    df.set_index(['datetime'], inplace=True)
    return df


def _quote_to_ohlcv(df, one_day):
    m_index = tminutes(one_day, one_day)
    df = df.copy()
    df = df.reindex(m_index, method='bfill')

    resampled = df.resample('1T', label='right')
    ohlc = resampled['close'].ohlc()

    ohlc = ohlc.reindex(m_index, method='ffill')
    # 反应在实时报价的成交量为累积值
    v = df['volume'].diff()
    ohlcv = pd.concat([ohlc, v], axis=1)

    first_loc = df.index.indexer_at_time('09:31')
    if len(first_loc):
        ohlcv.iloc[0, :] = df.iloc[first_loc, :][ohlcv.columns].values[0]
    return ohlcv.sort_index()


def _fetch_single_minutely_equity(one_day, stock_code, db=None, is_index=False):
    """
    Notes:
    ------
        每天交易数据长度应为240
        index.tz is None 本地时区时间
    Examples
    --------
    >>> stock_code = '000333'
    >>> one_day = pd.Timestamp('2020-07-31 00:00:00', freq='B')
    >>> df = _fetch_single_minutely_equity(one_day, stock_code)
    >>> df
        open	high	low	close	volume
    2020-09-24 09:31:00	15.59	15.61	15.51	15.55	1601609.0
    2020-09-24 09:32:00	15.55	15.55	15.55	15.55	491256.0
    2020-09-24 09:33:00	15.55	15.55	15.55	15.55	279342.0
    2020-09-24 09:34:00	15.54	15.54	15.54	15.54	308431.0
    2020-09-24 09:35:00	15.51	15.51	15.51	15.51	376372.0
    ...	...	...	...	...	...
    2020-09-24 14:56:00	15.14	15.14	15.14	15.14	458404.0
    2020-09-24 14:57:00	15.13	15.13	15.13	15.13	350426.0
    2020-09-24 14:58:00	15.14	15.14	15.14	15.14	0.0
    2020-09-24 14:59:00	15.14	15.14	15.14	15.14	0.0
    2020-09-24 15:00:00	15.14	15.14	15.14	15.14	1547479.0
    240 rows × 5 columns
    """
    df = _single_minutely_equity(one_day, stock_code, db, is_index)
    cols = ['open', 'high', 'low', 'close', 'volume']
    index = tminutes(one_day, one_day)
    default = pd.DataFrame(0.0, columns=cols, index=index)
    if df.empty:
        return default
    try:
        return _quote_to_ohlcv(df, one_day)
    except ValueError:
        return default


def _index_daily_to_minute(code, one_day):
    """将指数日线数据转换为分钟级别数据"""
    cols = ['date', 'open', 'high', 'low', 'close', 'volume']
    index = tminutes(one_day, one_day)
    default = pd.DataFrame(
        0.0, columns=['open', 'high', 'low', 'close', 'volume'], index=index)
    try:
        df = _fetch_single_index(code, one_day, one_day)
    except KeyError:
        return default
    if df.empty:
        return default
    df = df[cols]
    df['date'] = df['date'].map(lambda x: x.replace(hour=9, minute=31))
    df.set_index('date', inplace=True)
    df = df.reindex(index, method='ffill')
    return df


def _index_minute_data(code, dates):
    # 日线 -> 分钟
    d_dates = [d for d in dates if d < INDEX_QUOTE_START]
    # 直接使用分钟数据
    m_dates = [d for d in dates if d >= INDEX_QUOTE_START]

    d_dfs = [_index_daily_to_minute(code, d) for d in d_dates]

    db = get_db('wy_index_quotes')
    code = decode_index_code(code)
    func = partial(_fetch_single_minutely_equity,
                   stock_code=code, db=db, is_index=True)
    with ThreadPoolExecutor(MAX_WORKER) as executor:
        m_dfs = executor.map(func, m_dates)

    dfs = d_dfs + [df for df in m_dfs if df is not None]
    return pd.concat(dfs).sort_index()


def fetch_single_minutely_equity(code, start, end):
    """
    从本地数据库读取单个股票期间分钟级别交易明细数据

    **注意** 
        交易日历分钟自9:31~11:30 13:01~15：00
        在数据库中，分钟级别成交数据分日期存储

    Parameters
    ----------
    code : str
        要获取数据的股票代码
    start_date : datetime-like
        自开始日期(包含该日)
    end_date : datetime-like
        至结束日期

    return
    ----------
    DataFrame: OHLCV列的DataFrame对象。

    Examples
    --------
    >>> stock_code = '000333'
    >>> start = '2020-06-29'
    >>> end = pd.Timestamp('2020-06-30')
    >>> df = fetch_single_minutely_equity(stock_code, start, end)
    >>> df.tail()
                        close   high    low   open  volume
    2018-04-19 14:56:00  51.55  51.56  51.50  51.55  376400
    2018-04-19 14:57:00  51.55  51.55  51.55  51.55   20000
    2018-04-19 14:58:00  51.55  51.55  51.55  51.55       0
    2018-04-19 14:59:00  51.55  51.55  51.55  51.55       0
    2018-04-19 15:00:00  51.57  51.57  51.57  51.57  353900
    """
    calendar = get_calendar('XSHG')
    fmt = r"%Y-%m-%d"
    dates = calendar.sessions_in_range(
        start.strftime(fmt), end.strftime(fmt)).tz_localize(None)
    cols = ['open', 'high', 'low', 'close', 'volume']

    # 指数分钟级别数据
    if len(code) == 7:
        return _index_minute_data(code, dates)

    db = get_db('wy_quotes')
    func = partial(_fetch_single_minutely_equity,
                   stock_code=code, db=db, is_index=False)
    with ThreadPoolExecutor(MAX_WORKER) as executor:
        dfs = executor.map(func, dates)
    return pd.concat(dfs).sort_index()
