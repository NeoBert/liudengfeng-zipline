"""

本地数据查询及预处理，适用于zipline ingest写入

"""

from sqlalchemy import func
import pandas as pd
import numpy as np
from cnswd.sql.base import session_scope
from cnswd.sql.szsh import StockDaily, CJMX
from cnswd.sql.szx import StockInfo, Dividend


DAILY_COLS = ['symbol', 'date',
              'open', 'high', 'low', 'close',
              'prev_close', 'change_pct',
              'volume', 'amount', 'turnover', 'cmv', 'tmv']
OHLCV_COLS = ['open', 'high', 'low', 'close', 'volume']
MINUTELY_COLS = ['symbol', 'date'] + OHLCV_COLS

ADJUSTMENT_COLS = ['symbol', 'date', 's_ratio', 'z_ratio', 'amount',
                   'declared_date', 'record_date', 'ex_date', 'pay_date']


def get_exchange(code):
    """股票所在交易所编码"""
    # https://www.iso20022.org/10383/iso-10383-market-identifier-codes
    if code[0] in ('0', '2', '3'):
        return "深圳证券交易所"
    elif code[0] in ('6', '9'):
        return "上海证券交易所"
    else:
        return 'unknown'.upper()


def _stock_basic_info():
    """股票基础信息
    
    Returns:
        DataFrame -- 六列数据框

    Examples
    --------
    >>> df = _stock_basic_info()
    >>> df.head()
        symbol start_date end_date status stock_type exchange
    0     002301 2009-10-21     None   正常上市         A股   深交所中小板
    1     002055 2006-07-25     None   正常上市         A股   深交所中小板
    2     600000 1999-11-10     None   正常上市         A股      上交所
    3     601966 2016-07-06     None   正常上市         A股      上交所
    4     603556 2016-11-10     None   正常上市         A股      上交所    
    """
    col_names = ['symbol', 'start_date', 'end_date',
                 'status', 'stock_type', 'exchange']
    with session_scope('szx') as sess:
        query = sess.query(
            StockInfo.股票代码,
            StockInfo.上市日期,
            StockInfo.摘牌日期,
            StockInfo.上市状态,
            StockInfo.股票类别,
            StockInfo.上市地点,
        )
        df = pd.DataFrame.from_records(query.all())
        df.columns = col_names
        return df


def get_latest_short_name():
    """
    获取股票最新股票简称

    Examples
    --------
    >>> df = get_end_dates()
    >>> df.head()
         symbol  asset_name
    0     000001    平安银行
    1     000002    万 科Ａ
    2     000003   PT金田Ａ
    3     000004    国农科技
    4     000005    世纪星源     
    """
    col_names = ['symbol', 'last_date', 'asset_name']
    with session_scope('szsh') as sess:
        query = sess.query(
            StockDaily.股票代码,
            func.max(StockDaily.日期),
            StockDaily.名称
        ).group_by(
            StockDaily.股票代码
        )
        df = pd.DataFrame.from_records(query.all())
        df.columns = col_names
        return df.iloc[:, [0, 2]]


def _stock_first_and_last():
    """
    自股票日线交易数据查询开始交易及结束交易日期

    Examples
    --------
    >>> df = _stock_first_and_last()
    >>> df.head()
        symbol first_traded last_traded
    0     000001   1991-04-03  2018-12-21
    1     000002   1991-01-29  2018-12-21
    2     000003   1991-01-02  2002-04-26
    3     000004   1991-01-02  2018-12-21
    4     000005   1991-01-02  2018-12-21   
    """
    col_names = ['symbol', 'first_traded', 'last_traded']
    with session_scope('szsh') as sess:
        query = sess.query(
            StockDaily.股票代码,
            func.min(StockDaily.日期),
            func.max(StockDaily.日期)
        ).group_by(
            StockDaily.股票代码
        )
        df = pd.DataFrame.from_records(query.all())
        df.columns = col_names
        return df


def gen_asset_metadata(only_in=True, only_A=True):
    """
    生成股票元数据

    Paras
    -----
    only_in : bool
        是否仅仅包含当前在市的股票，默认为真。
    only_A : bool
        是否仅仅为A股股票(即：不包含B股股票)，默认为不包含。

    Examples
    --------
    >>> df = gen_asset_metadata()
    >>> df.head()
        symbol start_date   end_date exchange asset_name first_traded last_traded auto_close_date
    0     000001 1991-04-03 2018-12-21    深交所主板       平安银行   1991-04-03  2018-12-21      2018-12-22
    1     000002 1991-01-29 2018-12-21    深交所主板       万 科Ａ   1991-01-29  2018-12-21      2018-12-22
    2     000004 1991-01-14 2018-12-21    深交所主板       国农科技   1991-01-02  2018-12-21      2018-12-22
    3     000005 1990-12-10 2018-12-21    深交所主板       世纪星源   1991-01-02  2018-12-21      2018-12-22
    4     000006 1992-04-27 2018-12-21    深交所主板       深振业Ａ   1992-04-27  2018-12-21      2018-12-22
    """
    f_and_l = _stock_first_and_last()
    latest_name = get_latest_short_name()
    s_and_e = _stock_basic_info()
    df = s_and_e.merge(
        latest_name, 'left', on='symbol'
    ).merge(
        f_and_l, 'left', on='symbol'
    )
    # 剔除已经退市
    if only_in:
        df = df[df.status != '已经退市']
    del df['status']
    # 剔除非A股部分
    if only_A:
        df = df[df.stock_type == 'A股']
    del df['stock_type']
    # 对于未退市的结束日期，以最后交易日期代替
    df.loc[df.end_date.isna(), 'end_date'] = df.loc[df.end_date.isna(),
                                                    'last_traded']
    df.sort_values('symbol', inplace=True)
    df.reset_index(inplace=True, drop=True)
    # df['exchange'] = df['symbol'].map(get_exchange)
    df['auto_close_date'] = df['last_traded'].map(
        lambda x: x + pd.Timedelta(days=1))
    return df


def _fill_zero(df):
    """填充因为停牌ohlc可能存在的0值"""
    # 将close放在第一列
    ohlc_cols = ['close', 'open', 'high', 'low']
    ohlc = df[ohlc_cols].copy()
    ohlc.replace(0.0, np.nan, inplace=True)
    ohlc.close.fillna(method='ffill', inplace=True)
    # 按列填充
    ohlc.fillna(method='ffill', axis=1, inplace=True)
    for col in ohlc_cols:
        df[col] = ohlc[col]
    return df


def fetch_single_equity(stock_code, start, end):
    """
    从本地数据库读取股票期间日线交易数据

    注
    --
    1. 除OHLCV外，还包括涨跌幅、成交额、换手率、流通市值、总市值、流通股本、总股本
    2. 使用bcolz格式写入时，由于涨跌幅存在负数，必须剔除该列！！！

    Parameters
    ----------
    stock_code : str
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
    >>> symbol = '000333'
    >>> start_date = '2017-4-1'
    >>> end_date = pd.Timestamp('2018-4-16')
    >>> df = fetch_single_equity(symbol, start_date, end_date)
    >>> df.iloc[:,:8]
        symbol        date   open   high    low  close  prev_close  change_pct
    0  000333  2018-04-02  53.30  55.00  52.68  52.84       54.53     -3.0992
    1  000333  2018-04-03  52.69  53.63  52.18  52.52       52.84     -0.6056
    2  000333  2018-04-04  52.82  54.10  52.06  53.01       52.52      0.9330
    3  000333  2018-04-09  52.91  53.31  51.00  51.30       53.01     -3.2258
    4  000333  2018-04-10  51.45  52.80  51.18  52.77       51.30      2.8655
    5  000333  2018-04-11  52.78  53.63  52.41  52.98       52.77      0.3980
    6  000333  2018-04-12  52.91  52.94  51.84  51.87       52.98     -2.0951
    7  000333  2018-04-13  52.40  52.47  51.01  51.32       51.87     -1.0603
    8  000333  2018-04-16  51.31  51.80  49.15  49.79       51.32     -2.9813    
    """
    start = pd.Timestamp(start).date()
    end = pd.Timestamp(end).date()
    with session_scope('szsh') as sess:
        query = sess.query(
            StockDaily.股票代码,
            StockDaily.日期,
            StockDaily.开盘价,
            StockDaily.最高价,
            StockDaily.最低价,
            StockDaily.收盘价,
            StockDaily.前收盘,
            StockDaily.涨跌幅,
            StockDaily.成交量,
            StockDaily.成交金额,
            StockDaily.换手率,
            StockDaily.流通市值,
            StockDaily.总市值
        ).filter(
            StockDaily.股票代码 == stock_code,
            StockDaily.日期.between(start, end)
        )
        df = pd.DataFrame.from_records(query.all())
        if df.empty:
            return pd.DataFrame(columns=DAILY_COLS+['circulating_share', 'total_share'])
        df.columns = DAILY_COLS
        df = _fill_zero(df)
        df['circulating_share'] = df.cmv / df.close
        df['total_share'] = df.tmv / df.close
        return df


def _handle_minutely_data(df, exclude_lunch):
    """
    完成单个日期股票分钟级别数据处理
    """
    ohlcv = pd.Series(data=df['price'].values,
                      index=df.datetime).resample('T').ohlc()
    ohlcv.fillna(method='ffill', inplace=True)
    # 成交量原始数据单位为手，换为股
    volumes = pd.Series(data=df['volume'].values,
                        index=df.datetime).resample('T').sum() * 100
    ohlcv.insert(4, 'volume', volumes)
    if exclude_lunch:
        # 默认包含上下界
        # 与交易日历保持一致，自31分开始
        pre = ohlcv.between_time('9:25', '9:31')

        def key(x): return x.date()
        grouped = pre.groupby(key)
        opens = grouped['open'].first()
        highs = grouped['high'].max()
        lows = grouped['low'].min()  # 考虑是否存在零值？
        closes = grouped['close'].last()
        volumes = grouped['volume'].sum()
        index = pd.to_datetime([str(x) + ' 9:31' for x in opens.index])
        add = pd.DataFrame({'open': opens.values,
                            'high': highs.values,
                            'low': lows.values,
                            'close': closes.values,
                            'volume': volumes.values
                            },
                           index=index)
        am = ohlcv.between_time('9:32', '11:30')
        pm = ohlcv.between_time('13:00', '15:00')
        return pd.concat([add, am, pm])
    else:
        return ohlcv


def fetch_single_minutely_equity(stock_code, start, end, exclude_lunch=True):
    """
    从本地数据库读取单个股票期间分钟级别交易明细数据

    **注意** 性能原因，超过一定周期的数据，转移至备份数据库。只能查询到近期数据。

    注
    --
    1. 仅包含OHLCV列
    2. 原始数据按分钟进行汇总，first(open),last(close),max(high),min(low),sum(volume)

    Parameters
    ----------
    stock_code : str
        要获取数据的股票代码
    start_date : datetime-like
        自开始日期(包含该日)
    end_date : datetime-like
        至结束日期
    exclude_lunch ： bool
        是否排除午休时间，默认”是“

    return
    ----------
    DataFrame: OHLCV列的DataFrame对象。

    Examples
    --------
    >>> symbol = '000333'
    >>> start_date = '2018-4-1'
    >>> end_date = pd.Timestamp('2018-4-19')
    >>> df = fetch_single_minutely_equity(symbol, start_date, end_date)
    >>> df.tail()
                        close   high    low   open  volume
    2018-04-19 14:56:00  51.55  51.56  51.50  51.55  376400
    2018-04-19 14:57:00  51.55  51.55  51.55  51.55   20000
    2018-04-19 14:58:00  51.55  51.55  51.55  51.55       0
    2018-04-19 14:59:00  51.55  51.55  51.55  51.55       0
    2018-04-19 15:00:00  51.57  51.57  51.57  51.57  353900
    """
    col_names = ['symbol', 'datetime', 'price', 'volume']
    start = pd.Timestamp(start).date()
    end = pd.Timestamp(end).date()
    with session_scope('szsh') as sess:
        query = sess.query(
            CJMX.股票代码,
            CJMX.成交时间,
            CJMX.成交价,
            CJMX.成交量,
        ).filter(
            CJMX.股票代码 == stock_code,
            CJMX.成交时间.between(start, end)
        )
        df = pd.DataFrame.from_records(query.all())
        if df.empty:
            return pd.DataFrame(columns=OHLCV_COLS)
        df.columns = col_names
        return _handle_minutely_data(df, exclude_lunch)


def fetch_single_quity_adjustments(stock_code, start, end):
    """
    从本地数据库读取股票期间分红派息数据

    Parameters
    ----------
    stock_code : str
        要获取数据的股票代码
    start : datetime-like
        自开始日期
    end : datetime-like
        至结束日期

    return
    ----------
    DataFrame对象

    Examples
    --------
    >>> # 需要除去数值都为0的无效行
    >>> fetch_single_quity_adjustments('000333', '2010-4-1', '2018-4-16')
    symbol       date  s_ratio  z_ratio  amount declared_date record_date    ex_date   pay_date
    0  000333 2015-06-30      0.0      0.0     0.0           NaT         NaT        NaT        NaT
    1  000333 2015-12-31      0.0      0.5     1.2    2016-04-27  2016-05-05 2016-05-06 2016-05-06
    2  000333 2016-06-30      0.0      0.0     0.0           NaT         NaT        NaT        NaT
    3  000333 2016-12-31      0.0      0.0     1.0    2017-04-22  2017-05-09 2017-05-10 2017-05-10
    4  000333 2017-06-30      0.0      0.0     0.0           NaT         NaT        NaT        NaT
    5  000333 2017-12-31      0.0      0.0     1.2    2018-04-24  2018-05-03 2018-05-04 2018-05-04
    """
    start = pd.Timestamp(start).date()
    end = pd.Timestamp(end).date()
    with session_scope('szx') as sess:
        query = sess.query(
            Dividend.股票代码,
            Dividend.分红年度,
            Dividend.送股比例,
            Dividend.转增比例,
            Dividend.派息比例人民币,
            Dividend.股东大会预案公告日期,
            Dividend.A股股权登记日,
            Dividend.A股除权日,
            Dividend.派息日A,
        ).filter(
            Dividend.股票代码 == stock_code,
            Dividend.分红年度.between(start, end)
        )
        df = pd.DataFrame.from_records(query.all())
        if df.empty:
            # 返回一个空表
            return pd.DataFrame(columns=ADJUSTMENT_COLS)
        df.columns = ADJUSTMENT_COLS
        # nan以0代替
        df['s_ratio'].fillna(value=0.0, inplace=True)
        df['z_ratio'].fillna(value=0.0, inplace=True)
        df['amount'].fillna(value=0.0, inplace=True)
        # 调整为每股比例
        df['s_ratio'] = df['s_ratio'] / 10.0
        df['z_ratio'] = df['z_ratio'] / 10.0
        df['amount'] = df['amount'] / 10.0
        return df
