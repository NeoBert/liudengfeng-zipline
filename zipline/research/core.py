"""
用途：
    1. notebook研究
    2. 测试
"""
import os
import warnings

import pandas as pd
from cnswd.utils import data_root, ensure_list, sanitize_dates
from trading_calendars import get_calendar

from zipline.assets import Asset, Equity
from zipline.data.bundles.core import load
from zipline.data.data_portal import DataPortal
from zipline.pipeline import Pipeline
from zipline.pipeline.data import EquityPricing
from zipline.pipeline.domain import CN_EQUITIES
from zipline.pipeline.engine import SimplePipelineEngine
from zipline.pipeline.fundamentals.reader import Fundamentals
from zipline.pipeline.loaders import CNEquityPricingLoader
from zipline.pipeline.loaders.blaze import global_loader, BlazeLoader

with warnings.catch_warnings():
    warnings.filterwarnings(
        'ignore',
    )
    from collections import Iterable


def get_bundle_data(bundle='cndaily'):
    # 使用测试集时，更改为cntdaily。加快运行速度
    return load(bundle)


def get_asset_finder(bundle='cndaily'):
    bundle_data = load(bundle)
    return bundle_data.asset_finder


def gen_pipeline_loader(bundle='cndaily'):
    bundle_data = load(bundle)
    return CNEquityPricingLoader(bundle_data.equity_daily_bar_reader,
                                 bundle_data.adjustment_reader)


def gen_data_portal(bundle='cndaily'):
    bundle_data = get_bundle_data(bundle)
    trading_calendar = get_calendar('SZSH')
    return DataPortal(
        bundle_data.asset_finder,
        trading_calendar,
        trading_calendar.first_session,
        equity_daily_reader=bundle_data.equity_daily_bar_reader,
        adjustment_reader=bundle_data.adjustment_reader)


def init_engine(get_loader, asset_finder):
    """
    Construct and store a PipelineEngine from loader.
    """
    engine = SimplePipelineEngine(
        get_loader,
        asset_finder,
        default_domain=CN_EQUITIES,
    )
    return engine


def get_loader(column):
    if column in EquityPricing.columns:
        return gen_pipeline_loader()
    # # 简单处理
    elif Fundamentals.has_column(column):
        return BlazeLoader()
    raise ValueError("`PipelineLoader`没有注册列 %s." % column)


def to_tdates(start, end):
    """修正交易日期"""
    calendar = get_calendar('SZSH')
    dates = calendar.all_sessions
    # 修正日期
    start, end = sanitize_dates(start, end)
    # 定位交易日期
    start_date = dates[dates.get_loc(start, method='bfill')]
    end_date = dates[dates.get_loc(end, method='ffill')]
    if start_date > end_date:
        start_date = end_date
    return dates, start_date, end_date


def symbols(symbols_, symbol_reference_date=None, handle_missing='log'):
    """
    Convert a or a list of str and int into a list of Asset objects.
    
    Parameters:	
        symbols_ (str, int or iterable of str and int)
            Passed strings are interpreted as ticker symbols and 
            resolved relative to the date specified by symbol_reference_date.
        symbol_reference_date (str or pd.Timestamp, optional)
            String or Timestamp representing a date used to resolve symbols 
            that have been held by multiple companies. Defaults to the current time.
        handle_missing ({'raise', 'log', 'ignore'}, optional)
            String specifying how to handle unmatched securities. Defaults to ‘log’.

    Returns:	

    list of Asset objects – The symbols that were requested.
    """
    symbols_ = ensure_list(symbols_)

    allowed_dtype = [str, int, Asset, Equity]

    res = {0: [], 1: [], 2: [], 3: []}
    for s in symbols_:
        try:
            pos = allowed_dtype.index(type(s))
            res[pos].append(s)
        except ValueError:
            raise Exception(
                '{} is not str、int or zipline.assets.Asset'.format(s))

    if symbol_reference_date is not None:
        asof_date = pd.Timestamp(symbol_reference_date, tz='UTC')
    else:
        asof_date = pd.Timestamp('today', tz='UTC')
    finder = get_asset_finder()
    res[0] = finder.lookup_symbols(res[0], asof_date)
    res[1] = finder.retrieve_all(res[1])

    ret = []
    for s in res.values():
        ret.extend(s)
    return ret


def run_pipeline(pipe, start, end):
    _, start_date, end_date = to_tdates(start, end)
    asset_finder = get_asset_finder()
    engine = init_engine(get_loader, asset_finder)
    df = engine.run_pipeline(pipe, start_date, end_date)
    return df


def prices(assets,
           start,
           end,
           frequency='daily',
           price_field='price',
           symbol_reference_date=None,
           start_offset=0):
    """
    获取指定股票期间收盘价(复权处理)
    Parameters:	

        assets (int/str/Asset or iterable of same)
            Identifiers for assets to load. Integers are interpreted as sids. 
            Strings are interpreted as symbols.
        start (str or pd.Timestamp)
            Start date of data to load.
        end (str or pd.Timestamp)
            End date of data to load.
        frequency ({'minute', 'daily'}, optional)
            Frequency at which to load data. Default is ‘daily’.
        price_field ({'open', 'high', 'low', 'close', 'price'}, optional)
            Price field to load. ‘price’ produces the same data as ‘close’, 
            but forward-fills over missing data. Default is ‘price’.
        symbol_reference_date (pd.Timestamp, optional)
            Date as of which to resolve strings as tickers. Default is the current day.
        start_offset (int, optional)
            Number of periods before start to fetch. Default is 0. 
            This is most often useful for calculating returns.

    Returns:	

    prices (pd.Series or pd.DataFrame)
        Pandas object containing prices for the requested asset(s) and dates.

    Data is returned as a pd.Series if a single asset is passed.

    Data is returned as a pd.DataFrame if multiple assets are passed.   
    """
    msg = 'frequency只能选daily'
    assert frequency == 'daily', msg

    valid_fields = ('open', 'high', 'low', 'close', 'price', 'volume',
                    'amount', 'cmv', 'tmv', 'total_share', 'turnover')
    msg = '只接受单一字段，有效字段为{}'.format(valid_fields)
    assert isinstance(price_field, str), msg

    dates, start_date, end_date = to_tdates(start, end)

    if start_offset:
        start_date -= start_offset * dates.freq

    start_loc = dates.get_loc(start_date)
    end_loc = dates.get_loc(end_date)
    bar_count = end_loc - start_loc + 1

    assets = symbols(assets, symbol_reference_date=symbol_reference_date)
    data_portal = gen_data_portal()
    return data_portal.get_history_window(assets, end_date, bar_count, '1d',
                                          price_field, frequency)


def returns(assets,
            start,
            end,
            periods=1,
            frequency='daily',
            price_field='price',
            symbol_reference_date=None):
    """
    Fetch returns for one or more assets in a date range.
    Parameters:	

        assets (int/str/Asset or iterable of same)
            Identifiers for assets to load.
            Integers are interpreted as sids.
            Strings are interpreted as symbols.
        start (str or pd.Timestamp)
            Start date of data to load.
        end (str or pd.Timestamp)
            End date of data to load.
        periods (int, optional)
            Number of periods over which to calculate returns. 
            Default is 1.
        frequency ({'minute', 'daily'}, optional)
            Frequency at which to load data. Default is ‘daily’.
        price_field ({'open', 'high', 'low', 'close', 'price'}, optional)
            Price field to load. ‘price’ produces the same data as ‘close’, 
            but forward-fills over missing data. Default is ‘price’.
        symbol_reference_date (pd.Timestamp, optional)
            Date as of which to resolve strings as tickers. 
            Default is the current day.

    Returns:	

    returns (pd.Series or pd.DataFrame)
        Pandas object containing returns for the requested asset(s) and dates.

    Data is returned as a pd.Series if a single asset is passed.

    Data is returned as a pd.DataFrame if multiple assets are passed.
    """
    df = prices(assets, start, end, frequency, price_field,
                symbol_reference_date, periods)
    return df.pct_change(periods).dropna(how='all')


def volumes(assets,
            start,
            end,
            frequency='daily',
            symbol_reference_date=None,
            start_offset=0,
            use_amount=False):
    """
    获取资产期间成交量(或成交额)

    Parameters
    ----------
        assets (int/str/Asset or iterable of same)
            Identifiers for assets to load. Integers are interpreted as sids. 
            Strings are interpreted as symbols.
        start (str or pd.Timestamp)
            Start date of data to load.
        end (str or pd.Timestamp)
            End date of data to load.
        frequency ({'minute', 'daily'}, optional)
            Frequency at which to load data. Default is ‘daily’.
        symbol_reference_date (pd.Timestamp, optional)
            Date as of which to resolve strings as tickers. Default is the current day.
        start_offset (int, optional)
            Number of periods before start to fetch. Default is 0. 
            This is most often useful for calculating returns.
        use_amount：bool
            是否使用成交额字段。默认为否。
            如使用成交额，则读取期间成交额数据。

    Returns:	

    volumes (pd.Series or pd.DataFrame)
        Pandas object containing volumes for the requested asset(s) and dates.

    Data is returned as a pd.Series if a single asset is passed.

    Data is returned as a pd.DataFrame if multiple assets are passed.
    """
    field = 'amount' if use_amount else 'volume'
    return prices(assets, start, end, frequency, field, symbol_reference_date,
                  start_offset)


def ohlcv(asset,
          start=pd.datetime.today() - pd.Timedelta(days=365),
          end=pd.datetime.today()):
    """获取单个股票期间ohlcv五列数据框"""
    fields = ['open', 'high', 'low', 'close', 'volume']
    dfs = []
    # 取单个股票价格数据，如果输入为Equity实例，转换为列表形式
    if isinstance(asset, Equity):
        asset = [asset]
    for field in fields:
        # 取单个股票价格数据，必须以列表形式输入[asset]
        df = prices(asset, start, end, price_field=field)
        dfs.append(df)
    res = pd.concat(dfs, axis=1)
    res.columns = fields
    return res.dropna()  # 在当天交易时，如果数据尚未刷新，当天会含有na
