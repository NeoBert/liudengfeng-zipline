import pandas as pd
from collections.abc import Iterable
from cnswd.utils import ensure_list, sanitize_dates
from zipline.assets import Asset, Equity
from zipline.data.benchmarks_cn import get_cn_benchmark_returns
from zipline.data.treasuries_cn import get_treasury_data, TREASURY_COL_MAPS
from .factory import _asset_finder, _data_portal, _trading_calendar
from zipline.pipeline.domain import CN_EQUITIES


OHLCV = ('open', 'high', 'low', 'close', 'volume')


def trading_sessions(start, end):
    """期间交易日"""
    calendar = _trading_calendar()
    sessions = calendar.sessions_in_range(start, end)
    if len(sessions) < 1:
        raise ValueError(f"期间{start} ~ {end} 无交易日")
    return sessions


def symbols(symbols_,
            symbol_reference_date=None,
            country=None,
            handle_missing='log'):
    """
    Convert a or a list of str and int into a list of Asset objects.

    Parameters:	
        symbols_ (str, int or iterable of str and int)
            Passed strings are interpreted as ticker symbols and 
            resolveXSHGd relative to the date specified by symbol_reference_date.
        symbol_reference_date (str or pd.Timestamp, optional)
            String or Timestamp representing a date used to resolve symbols 
            that have been held by multiple companies. Defaults to the current time.
        handle_missing ({'raise', 'log', 'ignore'}, optional)
            String specifying how to handle unmatched securities. Defaults to ‘log’.

    Returns:	

    list of Asset objects – The symbols that were requested.
    """
    if isinstance(symbols_, (Asset, Equity)):
        symbols_ = [symbols_]
    else:
        symbols_ = ensure_list(symbols_)

    finder = _asset_finder()

    if symbol_reference_date is not None:
        asof_date = pd.Timestamp(symbol_reference_date, tz='UTC')
    else:
        asof_date = pd.Timestamp('today', tz='UTC')

    ret = []
    for symbol in symbols_:
        if isinstance(symbol, str):
            res = finder.lookup_symbol(symbol, asof_date, country_code=country)
            ret.append(res)
        elif isinstance(symbol, int):
            res = finder.retrieve_asset(symbol)
            ret.append(res)
        elif isinstance(symbol, Asset):
            ret.append(symbol)
    return ret


def symbol(symbol_,
           symbol_reference_date=None,
           country=None,
           handle_missing='log'):
    """单只股票"""
    return symbols(symbol_, symbol_reference_date, country, handle_missing)[0]


def prices(assets,
           start,
           end,
           frequency='daily',
           price_field='price',
           symbol_reference_date=None,
           start_offset=0,
           adjust=False):
    """
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
    """
    msg = "Only support frequency == 'daily'"
    assert frequency == 'daily', msg
    valid_fields = ('open', 'high', 'low', 'close', 'price', 'volume',
                    'amount', 'market_cap', 'total_cap', 'shares_outstanding',
                    'total_shares', 'turnover')
    adj_fields = ('b_open', 'b_high', 'b_low', 'b_close')
    msg = '只接受单一字段，有效字段为{}'.format(valid_fields)
    assert isinstance(price_field, str), msg
    if adjust:
        msg = '当查询调整股价时，有效字段为{}'.format(adj_fields)
        assert isinstance(price_field,
                          str) and (price_field in adj_fields), msg
    data_portal, calendar = _data_portal()

    sessions = trading_sessions(start, end)
    start = sessions[0]
    end = sessions[-1]

    if start_offset:
        start -= start_offset * calendar.day

    dates = calendar.all_sessions
    start_loc = dates.get_loc(start)
    end_loc = dates.get_loc(end)
    bar_count = end_loc - start_loc + 1

    assets = symbols(assets, symbol_reference_date=symbol_reference_date)

    return data_portal.get_history_window(assets, end, bar_count, '1d',
                                          price_field, 'daily')


def volumes(assets, start, end, frequency='daily', symbol_reference_date=None):
    """
    获取股票期间成交量

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

    Returns:	

    volumes (pd.Series or pd.DataFrame)
        Pandas object containing volumes for the requested asset(s) and dates.

    Data is returned as a pd.Series if a single asset is passed.

    Data is returned as a pd.DataFrame if multiple assets are passed.
    """
    field = 'volume'
    return prices(assets, start, end, frequency, field, symbol_reference_date)


def returns(assets,
            start,
            end,
            periods=1,
            frequency='daily',
            price_field='price',
            symbol_reference_date=None):
    """
    获取股票期间收益率

    Parameters
    ----------
        assets (int/str/Asset or iterable of same)
            Identifiers for assets to load. Integers are interpreted as sids. 
            Strings are interpreted as symbols.
        start (str or pd.Timestamp)
            Start date of data to load.
        end (str or pd.Timestamp)
            End date of data to load.
        periods(int)
            周期数
        frequency ({'minute', 'daily'}, optional)
            Frequency at which to load data. Default is 'daily'.
        symbol_reference_date (pd.Timestamp, optional)
            Date as of which to resolve strings as tickers. Default is the current day.

    Returns:	

    收益率 (pd.Series or pd.DataFrame)
        Pandas object containing volumes for the requested asset(s) and dates.

    Data is returned as a pd.Series if a single asset is passed.

    Data is returned as a pd.DataFrame if multiple assets are passed.
    """
    df = prices(assets, start, end, frequency, price_field,
                symbol_reference_date, periods)
    return df.pct_change(periods).iloc[periods:]


def treasury_returns(symbol, start, end):
    """国库券收益率

    Arguments:
        symbol {str} -- 期间代码，如"m1","y1"
        start {datatime-like} -- 开始时间
        end {datatime-like} -- 结束时间
    """
    assert symbol in [x for x in TREASURY_COL_MAPS.values() if x != 'date']
    calendar = _trading_calendar()

    start = pd.Timestamp(start, tz='utc')
    if not calendar.is_session(start):
        # this is not a trading session, advance to the next session
        start = calendar.minute_to_session_label(
            start,
            direction='next',
        )

    end = pd.Timestamp(end, tz='utc')
    if not calendar.is_session(end):
        # this is not a trading session, advance to the previous session
        end = calendar.minute_to_session_label(
            end,
            direction='previous',
        )
    df = get_treasury_data(start, end)
    return df.loc[start:end, symbol]


def benchmark_returns(symbol, start, end):
    """基准收益率

    Arguments:
        symbol {str} -- 股指代码
        start {datatime-like} -- 开始时间
        end {datatime-like} -- 结束时间
    """
    calendar = _trading_calendar()

    start = pd.Timestamp(start, tz='utc')
    if not calendar.is_session(start):
        # this is not a trading session, advance to the next session
        start = calendar.minute_to_session_label(
            start,
            direction='next',
        )

    end = pd.Timestamp(end, tz='utc')
    if not calendar.is_session(end):
        # this is not a trading session, advance to the previous session
        end = calendar.minute_to_session_label(
            end,
            direction='previous',
        )
    s = get_cn_benchmark_returns(symbol)
    return s[start:end]


def _is_single(x):
    if isinstance(x, (Asset, Equity)):
        return True
    if isinstance(x, str):
        return True
    elif isinstance(x, Iterable):
        if len(x) > 1:
            return False
        else:
            return True
    raise TypeError(f'期望输入类型为`str`或`可迭代对象`，实际输入类型为：{type(x)}')


def get_pricing(symbols_,
                start_date,
                end_date,
                symbol_reference_date=None,
                frequency='daily',
                fields='close',
                handle_missing='raise',
                start_offset=0):
    """
    Load a table of historical trade data.

    Parameters
    ----------
    symbols : Object (or iterable of objects) convertible to Asset
        Valid input types are Asset, Integral, or basestring.  In the case that
        the passed objects are strings, they are interpreted
        as ticker symbols and resolved relative to the date specified by
        symbol_reference_date.

    start_date : str or pd.Timestamp, optional
        String or Timestamp representing a start date or start intraday minute
        for the returned data. Defaults to '2013-01-03'.

    end_date : str or pd.Timestamp, optional
        String or Timestamp representing an end date or end intraday minute for
        the returned data. Defaults to '2014-01-03'.

    symbol_reference_date : str or pd.Timestamp, optional
        String or Timestamp representing a date used to resolve symbols that
        have been held by multiple companies. Defaults to the current time.

    frequency : {'daily', 'minute'}, optional
        Resolution of the data to be returned.

    fields : str or list, optional
        String or list drawn from {'price', 'open', 'high', 'low',
        'close', 'volume',...及附加列}.  Default behavior is to return all fields.

    handle_missing : {'raise', 'log', 'ignore'}, optional
        String specifying how to handle unmatched securities.
        Defaults to 'raise'.

    start_offset : int, optional
        Number of periods before ``start`` to fetch.
        Default is 0.

    Returns
    -------
    pandas Panel/DataFrame/Series
        The pricing data that was requested.  See note below.

    Notes
    -----
    If a list of symbols is provided, data is returned in the form of a pandas
    Panel object with the following indices::

        items = fields
        major_axis = TimeSeries (start_date -> end_date)
        minor_axis = symbols

    If a string is passed for the value of `symbols` and `fields` is None or a
    list of strings, data is returned as a DataFrame with a DatetimeIndex and
    columns given by the passed fields.

    If a list of symbols is provided, and `fields` is a string, data is
    returned as a DataFrame with a DatetimeIndex and a columns given by the
    passed `symbols`.
    """
    single_asset = _is_single(symbols_)
    single_field = _is_single(fields)
    assets = symbols(symbols_, symbol_reference_date)
    if single_field:
        is_adj = fields.startswith('b_')
        ret = prices(symbols_, start_date, end_date, 'daily', fields,
                     symbol_reference_date, start_offset, is_adj)
        if single_asset:
            ret = pd.Series(ret.values[:, 0],
                            index=ret.index.get_level_values(0),
                            name=assets[0])
        else:
            ret.index.name = fields
    else:
        dfs = []
        for field in fields:
            is_adj = field.startswith('b_')
            df = prices(symbols_, start_date, end_date, 'daily', field,
                        symbol_reference_date, start_offset, is_adj)
            if single_asset:
                dfs.append(df)
            else:
                dfs.append(df.stack())
        ret = pd.concat(dfs, axis=1)
        ret.columns = fields
    return ret


def get_forward_returns(factor, periods, domain=CN_EQUITIES):
    """Get forward returns for the dates and assets in ``factor``, calculated
    over the given periods.

    Parameters
    ----------
    factor : pd.Series
        The factor whose dates and assets to use.

    periods : list[int]
        The periods over which to calculate the forward returns.
        Example: [1, 5, 10]

    domain : zipline.pipeline.domain.EquityCalendarDomain
        The domain to use when calculating forward returns. Should match the
        domain used when calculating ``factor``.

        For convenience, domains can imported from
        ``quantopian.pipeline.domain``.

    Returns
    -------
    result : pd.DataFrame

    A dataframe of computed forward returns containing one column per
    requested period. It is indexed first by date, then by asset."""
    assert isinstance(periods, list)
    tdates = sorted(factor.index.levels[0].unique())
    start, end = tdates[0], tdates[-1]
    assets = factor.index.levels[1].unique()
    res = [returns(assets, start, end, p, price_field='b_close').shift(-p).stack()
           for p in periods]
    df = pd.concat(res, axis=1)
    df.columns = [f"{p}D" for p in periods]
    df.index.set_names(['date', 'asset'], inplace=True)
    return df
