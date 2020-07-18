import pandas as pd
import os
from zipline.utils.paths import zipline_path
from zipline.pipeline.fundamentals.localdata import get_cn_industry
from zipline.pipeline.fundamentals.constants import CN_TO_SECTOR, SECTOR_NAMES
from .core import symbol
from zipline.assets.assets import SymbolNotFound


def get_ff_factors(n):
    """读取3因子或5因子数据"""
    assert n in (3, 5), "仅支持3因子或5因子"
    file_name = f"ff{n}"
    root_dir = zipline_path(['factors'])
    result_path_ = os.path.join(root_dir, f'{file_name}.pkl')
    return pd.read_pickle(result_path_)


def get_sector_mappings(to_symbol=True):
    df = get_cn_industry(True)
    codes = df['sid'].map(lambda x: str(x).zfill(6)).values
    if to_symbol:
        keys = []
        for code in codes:
            try:
                keys.append(symbol(code))
            except SymbolNotFound:
                pass
    else:
        keys = codes
    names = df['国证一级行业编码'].map(
        CN_TO_SECTOR, na_action='ignore').map(SECTOR_NAMES).values
    return {
        c: v for c, v in zip(keys, names)
    }
