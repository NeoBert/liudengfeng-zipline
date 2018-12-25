import pandas as pd
from cnswd.sql.base import session_scope
from cnswd.sql.szsh import IndexDaily


def get_cn_benchmark_returns(symbol):
    """获取基准收益率

    Parameters
    ----------
    symbol : str
        Benchmark symbol for which we're getting the returns.
    
    Returns:
        Series -- 基准收益率
    """

    with session_scope('szsh') as sess:
        query = sess.query(
            IndexDaily.日期, 
            IndexDaily.涨跌幅
        ).filter(
            IndexDaily.指数代码 == symbol
        )
        df = pd.DataFrame.from_records(query.all())
        s = pd.Series(
            data=df[1].values / 100,
            index=pd.DatetimeIndex(df[0].values)
        )
        return s.sort_index().tz_localize('UTC').dropna()
