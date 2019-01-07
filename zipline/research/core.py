import pandas as pd

from cnswd.utils import ensure_list, sanitize_dates
from zipline.assets import Asset, Equity
from zipline.data.benchmarks_cn import get_cn_benchmark_returns

from .factory import _asset_finder, _data_portal, _trading_calendar
from .reader import get_pricing as _get_pricing


def to_tdates(start, end):
    """修正交易日期"""
    calendar = _trading_calendar()
    dates = calendar.all_sessions
    # 修正日期
    start, end = sanitize_dates(start, end)
    # 定位交易日期
    start_date = dates[dates.get_loc(start, method='bfill')]
    end_date = dates[dates.get_loc(end, method='ffill')]
    if start_date > end_date:
        start_date = end_date
    return dates, start_date, end_date


def symbols(symbols_, symbol_reference_date=None, country=None, handle_missing='log'):
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
    symbols_ = ensure_list(symbols_)

    finder = _asset_finder()

    if symbol_reference_date is not None:
        asof_date = pd.Timestamp(symbol_reference_date, tz='UTC')
    else:
        asof_date = pd.Timestamp('today', tz='UTC')

    ret = []
    for symbol in symbols_:
        if isinstance(symbol, str):
            res = finder.lookup_symbol(
                symbol, asof_date, country_code=country
            )
            ret.append(res)
        elif isinstance(symbol, int):
            res = finder.retrieve_asset(symbol)
            ret.append(res)
        elif isinstance(symbol, Asset):
            ret.append(symbol)
    return ret


def symbol(symbol_, symbol_reference_date=None, country=None, handle_missing='log'):
    """单只股票"""
    return symbols(symbol_, symbol_reference_date, country, handle_missing)[0]


def prices(assets,
           start,
           end,
           frequency='daily',
           price_field='price',
           symbol_reference_date=None,
           start_offset=0):
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

    valid_fields = ('open', 'high', 'low', 'close', 'price', 'volume')
    msg = '只接受单一字段，有效字段为{}'.format(valid_fields)
    assert isinstance(price_field, str), msg

    data_portal, calendar = _data_portal()

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

    if start_offset:
        start -= start_offset * calendar.day

    dates = calendar.all_sessions
    start_loc = dates.get_loc(start)
    end_loc = dates.get_loc(end)
    bar_count = end_loc - start_loc + 1

    assets = symbols(assets, symbol_reference_date=symbol_reference_date)

    return data_portal.get_history_window(
        assets, end, bar_count, '1d', price_field, 'daily'
    )


def volumes(assets,
            start,
            end,
            frequency='daily',
            symbol_reference_date=None):
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
    df = prices(assets,
                start,
                end,
                frequency,
                price_field,
                symbol_reference_date,
                periods)
    return df.pct_change(periods).iloc[periods:]


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


def get_pricing(assets,
                start_date,
                end_date,
                fields='close',
                symbol_reference_date=None,
                handle_missing='raise',
                start_offset=0):

    calendar = _trading_calendar()
    fields = ensure_list(fields)
    start = pd.Timestamp(start_date, tz='utc')
    if not calendar.is_session(start):
        # this is not a trading session, advance to the next session
        start = calendar.minute_to_session_label(
            start,
            direction='next',
        )

    end = pd.Timestamp(end_date, tz='utc')
    if not calendar.is_session(end):
        # this is not a trading session, advance to the previous session
        end = calendar.minute_to_session_label(
            end,
            direction='previous',
        )

    start -= start_offset * calendar.day

    assets = symbols(assets, symbol_reference_date)
    sids = [
        asset.sid
        for asset in assets
    ]
    ret = _get_pricing(
        sids, start, end, fields
    )

    reduce_condition1 = len(sids) == 1
    reduce_condition2 = len(fields) == 1

    if reduce_condition1 & (not reduce_condition2):
        ret = ret.unstack()
        ret.columns = fields
    elif (not reduce_condition1) & reduce_condition2:
        ret = ret.unstack()
        ret.columns = assets
    elif not (reduce_condition1 | reduce_condition2):
        ret.index.set_levels(assets, 1, inplace=True)

    return ret
