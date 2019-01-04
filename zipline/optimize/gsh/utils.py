import numpy as np
import pandas as pd


class NotFoundAsset(Exception):
    """找不到对应资产异常"""
    pass


def ensure_series(x):
    if isinstance(x, pd.Series):
        return x
    if isinstance(x, dict):
        return pd.Series(x)
    raise TypeError('输入不是pd.Series或dict')


def ensure_dict(x):
    if isinstance(x, dict):
        return x
    if isinstance(x, pd.Series):
        return x.to_dict()    
    raise TypeError('输入不是pd.Series或dict')


def check_series_or_dict(x, name):
    """检查输入类型。输入若不是序列或者字典类型其中之一，则触发类型异常"""
    is_s = isinstance(x, pd.Series)
    is_d = isinstance(x, dict)
    if not (is_s or is_d):
        raise TypeError('%s不是pd.Series或dict' % name)


def get_ix(assets1, assets2):
    """
    查找公共序列中各自的位置序号
    assets必须可迭代
    """
    index = assets1.intersection(assets2)
    ix1 = assets1.get_indexer(index)
    ix2 = assets2.get_indexer(index)
    
    return ix1, ix2


def get_ix2(index, assets, ignore=True):
    """
    从指定序列中查找assets位置序号
    assets必须可迭代
    """
    ix = index.get_indexer(assets)
    if -1 in ix:
        if ignore:
            ix = ix[ix != -1]
        else:
            raise NotFoundAsset('无法找到资产：{}'.format(
                pd.Index(assets).difference(index)))
    return ix


def non_null_data_args(f):
    def new_f(*args, **kwds):
        for el in args:
            null_checker(el)
        for el in kwds.values():
            null_checker(el)
        return f(*args, **kwds)

    return new_f
