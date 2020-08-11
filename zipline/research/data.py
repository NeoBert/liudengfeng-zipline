import pandas as pd
import os
from zipline.utils.paths import zipline_path
from zipline.pipeline.fundamentals.localdata import get_cn_industry
from zipline.pipeline.fundamentals.constants import CN_TO_SECTOR, SECTOR_NAMES
from .core import symbol
from zipline.assets.assets import SymbolNotFound
import random
from cnswd.mongodb import get_db
from trading_calendars import get_calendar

MATCH_ONLY_A = {
    '$match': {
        '$expr': {
            '$in': [
                {
                    '$substrBytes': [
                        '$股票代码', 0, 1
                    ]
                }, [
                    '0', '3', '6'
                ]
            ]
        }
    }
}


def random_sample_codes(n, only_A=True):
    """随机选择N个股票代码"""
    db = get_db('cninfo')
    coll = db['基本资料']
    calendar = get_calendar('XSHG')
    last_session = calendar.actual_last_session
    projection = {'_id': 0, '股票代码': 1}
    pipeline = [
        {'$match': {'上市日期': {'$lte': last_session}}},
        {'$project': projection},
    ]
    if only_A:
        pipeline.insert(0, MATCH_ONLY_A)
    cursor = coll.aggregate(pipeline)
    codes = [d['股票代码'] for d in list(cursor)]
    return random.sample(codes, n)


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
