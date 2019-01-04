import pandas as pd
from cnswd.utils import ensure_list
from ..data.sqldata import fetch_single_equity


def get_pricing(sids, start, end, fields):
    _sids = ensure_list(sids)
    _fields = ensure_list(fields)
    df = pd.concat([fetch_single_equity(str(sid).zfill(6), start, end) for sid in _sids])
    res = df[['symbol','date'] + _fields] # _fields务必为列表，返回DataFrame对象
    if len(_sids) >= 1 and len(_fields) == 1:
        return res.pivot(index='date', columns='symbol', values=_fields[0]).tz_localize('utc')
    else:
        raise NotImplementedError('只支持单字段，其他待完成')


