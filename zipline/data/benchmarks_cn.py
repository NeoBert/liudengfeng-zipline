import pandas as pd

from cnswd.store import WyIndexDailyStore


def get_cn_benchmark_returns(symbol='000300'):
    """获取基准收益率

    Parameters
    ----------
    symbol : str
        Benchmark symbol for which we're getting the returns.

    Returns:
        Series -- 基准收益率
    """
    with WyIndexDailyStore() as store:
        data = store.query(codes=symbol)
    s = pd.Series(data['涨跌幅'].values / 100.0,
                  index=data.index.get_level_values(0))
    return s.sort_index().tz_localize('UTC').dropna()
