"""

本地数据查询及预处理，适用于zipline ingest写入

读取本地数据

注：只选A股股票。注意股票总体在`ingest`及`fundamental`必须保持一致。
"""
import re
from functools import lru_cache

import numpy as np
import pandas as pd

from cnswd.query_utils import query, query_stmt, Ops
from cnswd.reader import asr_data, calendar, daily_history, stock_list, minutely_history
from cnswd.utils import sanitize_dates

WY_DAILY_COL_MAPS = {
    '日期': 'date',
    '股票代码': 'symbol',
    '收盘价': 'close',
    '最高价': 'high',
    '最低价': 'low',
    '开盘价': 'open',
    '前收盘': 'prev_close',
    '涨跌幅': 'change_pct',
    '换手率': 'turnover',
    '成交量': 'volume',
    '成交金额': 'amount',
    '总市值': 'total_cap',
    '流通市值': 'market_cap',
}

SZX_ADJUSTMENT_COLS = {
    '股票代码': 'symbol',
    '分红年度': 'date',
    '送股比例': 's_ratio',
    '转增比例': 'z_ratio',
    '派息比例(人民币)': 'amount',
    '股东大会预案公告日期': 'declared_date',
    'A股股权登记日': 'record_date',
    'A股除权日': 'ex_date',
    '派息日(A)': 'pay_date'
}


def get_exchange(code):
    """股票所在交易所编码"""
    # https://www.iso20022.org/10383/iso-10383-market-identifier-codes
    if code.startswith('688'):
        return "上交所科创板"
    elif code.startswith('6'):
        return "上交所"
    elif code.startswith('002'):
        return "深交所中小板"
    elif code.startswith('3'):
        return "深交所创业板"
    elif code.startswith('0'):
        return "深交所主板"
    else:
        raise ValueError(f'股票代码：{code}错误')


def _select_only_a(df, code_col):
    """选择A股数据
    
    Arguments:
        df {DataFrame} -- 数据框
        code_col {str} -- 代表股票代码的列名称
    
    Returns:
        DataFrame -- 筛选出来的a股数据
    """
    cond1 = df[code_col].str.startswith('2')
    cond2 = df[code_col].str.startswith('9')
    df = df.loc[~(cond1 | cond2), :]
    return df


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
    col_names = {
        '股票代码': 'symbol',
        '上市日期': 'start_date',
        '摘牌日期': 'end_date',
        '上市状态': 'status',
        '证券类别': 'stock_type',
        '上市地点': 'exchange'
    }
    df = stock_list()
    df.drop_duplicates('股票代码', inplace=True)
    # 剔除未上市、未交易的无效股票
    cond1 = ~ df['上市日期'].isnull()
    cond2 = df['上市日期'] <= pd.Timestamp('today')
    df = df.loc[cond1 & cond2, :]
    df = df[col_names.keys()]
    df.rename(columns=col_names, inplace=True)
    # 原始数据无效。确保类型正确
    df['end_date'] = pd.NaT
    return df  #.iloc[:2, :]  # TT


def _stock_first_and_last(code):
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
    try:
        df = daily_history(code, None, None)
        return pd.DataFrame(
            {
                'symbol':
                df['股票代码'].values[0],
                'asset_name':
                df['名称'].values[-1],  # 最新简称
                'first_traded':
                pd.Timestamp(df['日期'].values[0], tz='UTC'),
                # 适应于分钟级别的数据
                'last_traded':
                pd.Timestamp(df['日期'].values[-1], tz='UTC') +
                pd.Timedelta(days=1),
            },
            index=[0])
    except Exception:
        # 新股无数据
        return pd.DataFrame()


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
    s_and_e = _stock_basic_info()
    # 剔除非A股部分
    s_and_e = _select_only_a(s_and_e, 'symbol')
    f_and_ls = [_stock_first_and_last(code) for code in s_and_e.symbol.values]
    f_and_l = pd.concat(f_and_ls)
    df = s_and_e.merge(f_and_l, 'left', on='symbol')
    # 剔除已经退市
    if only_in:
        df = df[df.status != '已经退市']
    del df['status']
    del df['stock_type']
    # 对于未退市的结束日期，以最后交易日期代替
    df.loc[df.end_date.isna(), 'end_date'] = df.loc[df.end_date.isna(),
                                                    'last_traded']
    df.sort_values('symbol', inplace=True)
    df.reset_index(inplace=True, drop=True)
    df['exchange'] = df['symbol'].map(get_exchange)
    df['auto_close_date'] = df['last_traded'].map(
        lambda x: x + pd.Timedelta(days=1))
    return df


@lru_cache(None)
def _tdates():
    return calendar()


def _fill_zero(df, first_col='close'):
    """填充因为停牌ohlc可能存在的0值"""
    ohlc = ['close', 'open', 'high', 'low']
    ohlc_cols = [first_col] + list(set(ohlc).difference([first_col]))
    ohlc = df[ohlc_cols].copy()
    ohlc.replace(0.0, np.nan, inplace=True)
    if 'prev_close' in df.columns:
        ohlc.loc[ohlc.close.isna(), 'close'] = df.loc[ohlc.close.isna(),
                                                      'prev_close']
    # 按列填充
    ohlc.fillna(method='ffill', axis=1, inplace=True)
    for col in ohlc_cols:
        df[col] = ohlc[col]
    return df


def _get_valid_data(df):
    """截取首日上市交易后的数据"""
    volumes = df['volume'].values
    prev_closes = df['prev_close'].values
    for loc in range(len(df)):
        volume = volumes[loc]
        prev_close = prev_closes[loc]
        if volume > 0.0 and prev_close > 0.0:
            break
    return df.iloc[loc:, :]


def _add_back_prices(raw_df):
    """为原始数据添加后复权价格"""
    raw_df = _get_valid_data(raw_df)
    # 首个前收盘、收盘价、成交量均有效
    first_pre_close = raw_df['prev_close'].values[0] > 0.
    first_close = raw_df['close'].values[0] > 0.
    first_volume = raw_df['volume'].values[0] > 0.
    symbol = raw_df['symbol'].values[0]
    assert first_pre_close and first_close and first_volume, f'{symbol} 首发交易数据无效'
    prev_close = raw_df['prev_close'].values[0]

    # 累计涨跌幅调整系数（为百分比）
    cc = (raw_df['change_pct'].fillna(0.0) / 100 + 1).cumprod()

    b_close = prev_close * cc
    adj = b_close / raw_df['close']
    raw_df.loc[:, 'b_close'] = b_close.round(4)
    raw_df.loc[:, 'b_open'] = (raw_df['open'] * adj).round(4)
    raw_df.loc[:, 'b_high'] = (raw_df['high'] * adj).round(4)
    raw_df.loc[:, 'b_low'] = (raw_df['low'] * adj).round(4)
    return raw_df


def _reindex(df, dts):
    df.set_index('date', inplace=True)
    res = df.reindex(dts, method='ffill')
    res.reset_index(inplace=True)
    return res.rename(columns={"index": "date"})


def _fetch_single_equity(stock_code, start, end):
    """读取本地原始数据"""
    df = daily_history(stock_code, start, end)
    if df.empty:
        return df
    # 截取所需列
    df = df[WY_DAILY_COL_MAPS.keys()]
    df.rename(columns=WY_DAILY_COL_MAPS, inplace=True)
    df.sort_values('date', inplace=True)
    return df


def fetch_single_equity(stock_code, start, end):
    """
    从本地数据库读取股票期间日线交易数据

    注
    --
    1. 除OHLCV外，还包括涨跌幅、成交额、换手率、流通市值、总市值、流通股本、总股本
    2. 添加后复权价格，使用复权价在图中去除间隙断层
    3. 使用bcolz格式写入时，由于涨跌幅存在负数，必须剔除该列

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
    >>> # 600710 股票代码重用
    >>> stock_code = '600710'
    >>> start_date = '2016-03-29'
    >>> end_date = pd.Timestamp('2017-07-31')
    >>> df = fetch_single_equity(stock_code, start_date, end_date)
    >>> df.iloc[-6:,:8]
              date	symbol	open	high	low	close	prev_close	change_pct
    322	2017-07-24	600710	9.36	9.36	9.36	9.36	9.36	NaN
    323	2017-07-25	600710	9.36	9.36	9.36	9.36	9.36	NaN
    324	2017-07-26	600710	9.36	9.36	9.36	9.36	9.36	NaN
    325	2017-07-27	600710	9.36	9.36	9.36	9.36	9.36	NaN
    326	2017-07-28	600710	9.36	9.36	9.36	9.36	9.36	NaN
    327	2017-07-31	600710	9.25	9.64	7.48	7.55	9.31	-18.9044
    """
    start, end = sanitize_dates(start, end)
    # 首先提起全部数据，确保自IPO以来复权价一致
    df = _fetch_single_equity(stock_code, None, None)
    if df.empty:
        return df
    # 恢复0股价
    df = _fill_zero(df)
    # 添加复权价格
    df = _add_back_prices(df)
    cond = df['date'].between(start, end)
    df = df.loc[cond, :]
    t_start, t_end = df['date'].values[0], df['date'].values[-1]
    # 判断数据长度是否缺失
    dts = [t for t in _tdates() if t >= t_start and t <= t_end]
    dts = pd.to_datetime(dts)
    # 填充停牌数据
    df = _reindex(df, dts)
    assert len(df) == len(dts), f"股票：{stock_code}，期间{t_start} ~ {t_end} 数据不足"
    df.loc[:, 'shares_outstanding'] = df.market_cap / df.close
    df.loc[:, 'total_shares'] = df.total_cap / df.close
    return df


# 注意定期整理分钟级别的数据
@lru_cache(None)
def _all_data(start, end):
    df = minutely_history(None, start, end)
    df.rename(columns={
        '时间': 'datetime',
        '代码': 'symbol',
        '今开': 'open',
        '最高': 'high',
        '最低': 'low',
        '最新价': 'close',
        '成交量': 'volume',
    },
              inplace=True)
    df.sort_values('symbol', inplace=True)

    return df


def fetch_single_minutely_equity(stock_code, start, end):
    """
    从本地数据库读取单个股票期间分钟级别交易明细数据

    **注意** 
    1. 性能原因，超过一定周期的数据，转移至备份数据库。只能查询到近期数据。
    2. 交易日历分钟自9：31~11：31 13：01~15：01
    
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
    cols = ['datetime', 'open', 'high', 'low', 'close', 'volume']
    df = _all_data(start, end)
    cond = df['symbol'] == stock_code
    try:
        ret = df[cond][cols]
        # 可能存在重复
        ret.drop_duplicates('datetime', keep='last', inplace=True)
        ret.set_index('datetime', inplace=True)
        # # 将11：31、15：01 -> 11:30 15:00
        am = ret.between_time('09:31', '11:31')
        pm = ret.between_time('13:00', '15:01')
        return pd.concat([am, pm]).sort_index()
    except Exception:
        return pd.DataFrame()


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
    start, end = sanitize_dates(start, end)
    df = asr_data('5', stock_code, start, end)
    if df.empty:
        # 返回一个空表
        return pd.DataFrame(columns=SZX_ADJUSTMENT_COLS)
    df.rename(columns=SZX_ADJUSTMENT_COLS, inplace=True)
    df = df[SZX_ADJUSTMENT_COLS.values()]
    # nan以0代替
    df['s_ratio'].fillna(value=0.0, inplace=True)
    df['z_ratio'].fillna(value=0.0, inplace=True)
    df['amount'].fillna(value=0.0, inplace=True)
    # 调整为每股比例
    df['s_ratio'] = df['s_ratio'] / 10.0
    df['z_ratio'] = df['z_ratio'] / 10.0
    df['amount'] = df['amount'] / 10.0
    return df
