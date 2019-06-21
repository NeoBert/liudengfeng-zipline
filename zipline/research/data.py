import pandas as pd
import os
from zipline.utils.paths import zipline_path


def get_ff_factors(n):
    """读取3因子或5因子数据"""
    assert n in (3, 5), "仅支持3因子或5因子"
    file_name = f"ff{n}"
    root_dir = zipline_path(['factors'])
    result_path_ = os.path.join(root_dir, f'{file_name}.pkl')
    return pd.read_pickle(result_path_)
