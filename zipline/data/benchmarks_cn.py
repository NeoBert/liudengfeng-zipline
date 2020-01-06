import pandas as pd
from cnswd.reader import daily_history


def get_cn_benchmark_returns(symbol='000300'):
    """获取基准收益率

    Parameters
    ----------
    symbol : str
        Benchmark symbol for which we're getting the returns.
    
    Returns:
        Series -- 基准收益率
    """
    data = daily_history(symbol, None, None, True)
    s = pd.Series(data['涨跌幅'].values / 100.0,
                  index=pd.DatetimeIndex(data['日期'].values))
    return s.sort_index().tz_localize('UTC').dropna()
