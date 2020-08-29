import pandas as pd
from cnswd.mongodb import get_db


def get_cn_benchmark_returns(symbol='000300'):
    """获取基准收益率

    Parameters
    ----------
    symbol : str
        Benchmark symbol for which we're getting the returns.

    Returns:
        Series -- 基准收益率
    """
    db = get_db('wy_index_daily')
    collection = db[symbol]
    projection = {'_id': 0, '日期': 1, '涨跌幅': 1}
    df = pd.DataFrame.from_records(collection.find(projection=projection))
    index = pd.DatetimeIndex(df['日期'].values)
    s = pd.Series(df['涨跌幅'].values / 100.0,
                  index=index)
    return s.sort_index().tz_localize('UTC').sort_index().dropna()
