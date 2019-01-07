import pandas as pd
from cnswd.utils import ensure_list
from ..data.sqldata import fetch_single_equity


def get_pricing(sids, start, end, fields):
    """查询股价数据
    
    Arguments:
        sids {整数列表或单个整数} -- 要查询的股票代码。单个整数或字符串，或列表
        start {date-like} -- 开始日期
        end {date-like} -- 结束日期
        fields {str列表} -- 字段列表

    Returns:
        pd.DataFrame: A MultiIndex DataFrame indexed by date (level 0) and asset (level 1)
    """
    _sids = ensure_list(sids)
    _fields = ensure_list(fields)
    df = pd.concat([fetch_single_equity(str(sid).zfill(6), start, end)
                    for sid in _sids])
    df = df[['symbol', 'date'] + _fields]  # _fields为列表，返回DataFrame对象
    df.set_index('date', inplace=True)
    df = df.tz_localize('utc')
    df.set_index('symbol', append=True, inplace=True)
    df.sort_index(level=0, inplace=True)
    return df
