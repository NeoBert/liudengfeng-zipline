import pandas as pd
import os
from zipline.utils.paths import zipline_path
from zipline.pipeline.fundamentals.localdata_wy import get_cn_industry, get_sw_industry, get_zjh_industry
from zipline.pipeline.fundamentals.constants import CN_TO_SECTOR, SECTOR_NAMES
from .core import symbol
from zipline.assets.assets import SymbolNotFound
import random
from cnswd.mongodb import get_db
from trading_calendars import get_calendar
import re

CODE_PAT = re.compile(r"\d{6}")

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
NUM_MAPS = {
    1: '一',
    2: '二',
    3: '三',
    4: '四'
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
    """部门映射"""
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


def get_cn_industry_maps(level=1, to_symbol=True):
    """国证行业分级映射

    Args:
        level (int, optional): 行业层级[1,2,3,4]. Defaults to 1.
        to_symbol (bool, optional): 是否转换为Equity. Defaults to True.

    Returns:
        dict: key:股票代码或Equity, value:行业分类名称
    """
    assert level in (1, 2, 3, 4)
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
    col = f"国证{NUM_MAPS[level]}级行业"
    names = df[col].values
    return {
        c: v for c, v in zip(keys, names)
    }


def get_sw_industry_maps(level=1, to_symbol=True):
    """申万行业分级映射

    Args:
        level (int, optional): 行业层级[1,2,3]. Defaults to 1.
        to_symbol (bool, optional): 是否转换为Equity. Defaults to True.

    Returns:
        dict: key:股票代码或Equity, value:行业分类名称
    """
    assert level in (1, 2, 3)
    df = get_sw_industry(True)
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
    col = f"申万{NUM_MAPS[level]}级行业"
    names = df[col].values
    return {
        c: v for c, v in zip(keys, names)
    }


def get_zjh_industry_maps(level=1, to_symbol=True):
    """证监会行业分级映射

    Args:
        level (int, optional): 行业层级[1,2]. Defaults to 1.
        to_symbol (bool, optional): 是否转换为Equity. Defaults to True.

    Returns:
        dict: key:股票代码或Equity, value:行业分类名称
    """
    assert level in (1, 2)
    df = get_zjh_industry(True)
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
    col = f"证监会{NUM_MAPS[level]}级行业"
    names = df[col].values
    return {
        c: v for c, v in zip(keys, names)
    }


def _get_concept_maps(collection):
    projection = {
        '_id': 0,
        '概念名称': 1,
        '股票列表': 1,
    }
    pipeline = [
        {
            '$project': projection
        },
    ]
    cursor = collection.aggregate(pipeline)
    return {record['概念名称']: [c for c in record['股票列表'] if CODE_PAT.match(c)] for record in cursor}


def get_concept_maps(by='all', to_symbol=True):
    """概念对应股票列表

    Args:
        by (str, optional): 分类单位. Defaults to 'all'.
            all 代表合并
        to_symbol (bool, optional): 转换为Equity. Defaults to True.

    Returns:
        dict: 以概念名称为键，股票列表为值
    """
    assert by in ('ths', 'tct', 'all')
    db = get_db()
    if by == 'ths':
        collection = db['同花顺概念']
        return _get_concept_maps(collection)
    elif by == 'tct':
        collection = db['腾讯概念']
        return _get_concept_maps(collection)
    else:
        ths = _get_concept_maps(db['同花顺概念'])
        tct = _get_concept_maps(db['腾讯概念'])
        keys = set(list(ths.keys()) + list(tct.keys()))
        res = {}
        for key in keys:
            p1 = ths.get(key, [])
            p2 = tct.get(key, [])
            v = set(p1 + p2)
            if to_symbol:
                v = [symbol(s) for s in v]
            res[key] = v
        return res
