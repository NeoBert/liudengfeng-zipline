# 上市日期
# 分类
# 1. 行业
#   1.1 申万行业
#   1.2 同花顺行业
#   1.3 证监会行业
# 2. 地域
# 3. 概念
import re
import warnings

import akshare as ak
import numpy as np
import pandas as pd
from toolz.dicttoolz import merge

from cnswd.mongodb import get_db

from ..common import AD_FIELD_NAME, SID_FIELD_NAME, TS_FIELD_NAME
from .constants import (CN_TO_SECTOR, SECTOR_NAMES, SUPER_SECTOR_NAMES,
                        SW_SECTOR_NAMES)
from .utils import get_bcolz_col_names

THOUSAND_PAT = re.compile(",")
NUM_MAPS = {
    1: '一级',
    2: '二级',
    3: '三级',
    4: '四级',
}

# region 基础信息


def _listing_date():
    """上市日期"""
    sh_df = ak.stock_info_sh_name_code(indicator="主板A股")
    sh = {code: pd.to_datetime(dt, errors='coerce') for code, dt in zip(
        sh_df['SECURITY_CODE_A'], sh_df['LISTING_DATE'])}
    sz_df = ak.stock_info_sz_name_code(indicator="A股列表")
    sz = {code: pd.to_datetime(dt, errors='coerce') for code, dt in zip(
        sz_df['A股代码'], sz_df['A股上市日期'])}
    return merge(sh, sz)


def get_ipo():
    # 大量股票上市日期为空
    db = get_db('wy')
    collection = db['IPO资料']
    docs = collection.find(
        {},
        projection={
            '_id': 0,
            '股票代码': 1,
            '上市日期': 1,
        }
    )
    df = pd.DataFrame.from_records(docs)
    df['上市日期'] = pd.to_datetime(df['上市日期'], errors='coerce')
    wy_dates = {code: pd.to_datetime(dt, errors='coerce') for code, dt in zip(
        df['股票代码'], df['上市日期'])}
    ipo_dates = _listing_date()
    dates = merge(wy_dates, ipo_dates)

    def f(code):
        try:
            return dates[code]
        except KeyError:
            return pd.NaT
    df['上市日期'] = df['股票代码'].map(f)
    df.dropna(inplace=True)
    df.rename(columns={'股票代码': 'sid'}, inplace=True)
    df['sid'] = df['sid'].astype('int64')
    df[AD_FIELD_NAME] = df['上市日期'] - pd.Timedelta(days=1)
    return df


def get_region():
    """地域分类列表"""
    db = get_db()
    collection = db['地域分类']
    pipeline = [
        {
            '$unwind': {
                'path': '$股票列表'
            }
        }, {
            '$project': {
                '_id': 0,
                'sid': "$股票列表",
                '地域': '$名称',
            }
        }
    ]
    docs = collection.aggregate(pipeline)
    df = pd.DataFrame.from_records(docs)
    df['sid'] = df['sid'].astype('int64')
    return df


def get_info():
    """上市日期、地域分类

    始终使用最新的地域分类，不考虑其中的动态变化。
    """
    ipo = get_ipo()
    region = get_region()

    df = ipo.set_index('sid').join(
        region.set_index('sid'),
    )
    df.reset_index(inplace=True)
    return df


def _to_float(x):
    # 注册资本单位统一为万元
    if pd.isna(x):
        return x
    if x[-2:] != '万元':
        raise ValueError(f"注册资本单位错误，期望万元，实际为{x[-2:]}")
    return float(THOUSAND_PAT.sub('', x[:-2])) * 10000


def _gsjj():
    # price data 含 动态注册资本
    # 舍弃
    db = get_db('wy')
    collection = db['公司简介']
    docs = collection.find(
        {},
        projection={
            '_id': 0,
            '股票代码': 1,
            '注册资本': 1,
        }
    )
    df = pd.DataFrame.from_records(docs)
    df['注册资本'] = df['注册资本'].map(_to_float)
    return df.set_index('股票代码')

# endregion

# region 行业分类


def get_sw_industry():
    """不同于其他分类，申万提供起始日期可以作为`asof_date`,
    因此单独成表"""
    db = get_db('stockdb')
    collection = db['申万行业分类']
    docs = collection.find(
        {},
        projection={
            '_id': 0,
            'sid': '$股票代码',
            '申万一级行业': '$行业名称',
            AD_FIELD_NAME: '$起始日期',
        }
    )
    df = pd.DataFrame.from_records(docs)
    df['sid'] = df['sid'].astype('int64')
    sw_code_maps = {v: k for k, v in SW_SECTOR_NAMES.items()}
    df['sw_sector'] = df['申万一级行业'].map(
        lambda x: sw_code_maps[x], na_action='ignore').astype('int64')
    # 舍弃asof_date为空部分
    df = df[df[AD_FIELD_NAME].notna()]
    return df


def get_ths_industry():
    """同花顺一级行业分类列表"""
    db = get_db()
    collection = db['同花顺行业分类']
    pipeline = [
        {
            '$unwind': {
                'path': '$股票列表'
            }
        }, {
            '$project': {
                '_id': 0,
                'sid': "$股票列表",
                '同花顺一级行业': '$名称',
            }
        }
    ]
    docs = collection.aggregate(pipeline)
    df = pd.DataFrame.from_records(docs)
    df['sid'] = df['sid'].astype('int64')
    return df


def get_zjh_industry():
    """证监会一级行业分类列表"""
    db = get_db()
    collection = db['证监会行业分类']
    pipeline = [
        {
            '$unwind': {
                'path': '$股票列表'
            }
        }, {
            '$project': {
                '_id': 0,
                'sid': "$股票列表",
                '证监会一级行业': '$名称',
            }
        }
    ]
    docs = collection.aggregate(pipeline)
    df = pd.DataFrame.from_records(docs)
    df['sid'] = df['sid'].astype('int64')
    return df


def get_bom_maps(cate, pattern):
    r"""行业父类编码映射

    Example:

    >>> pattern = re.compile(r"^Z\d{2}$")
    >>> get_bom_maps('国证行业分类', pattern)
    {'Z01': '能源',
     'Z02': '原材料',
     'Z03': '工业',
     'Z04': '可选消费',
     'Z05': '主要消费',
     'Z06': '医药卫生',
     'Z07': '金融',
     'Z08': '信息技术',
     'Z09': '电信业务',
     'Z10': '公用事业',
     'Z11': '房地产'}
    """
    db = get_db()
    collection = db['分类BOM']
    pipeline = [
        {
            '$match': {
                '分类方式': cate,
                '分类编码': {
                    '$regex': pattern
                }
            }
        },
        {
            '$project': {'_id': 0}
        }
    ]
    maps = {}
    for d in collection.aggregate(pipeline):
        maps[d['分类编码']] = d['分类名称']
    return maps


def get_industry_stock_list(cate):
    """巨潮数据中心：行业分类"""
    db = get_db()
    collection = db['股票分类']
    pipeline = [
        {
            '$match': {
                '分类方式': cate
            }
        }, {
            '$unwind': {
                'path': '$股票列表'
            }
        }, {
            '$project': {
                '_id': 0,
                '分类编码': 1,
                '分类名称': 1,
                '股票代码': "$股票列表"
            }
        }
    ]
    ds = collection.aggregate(pipeline)
    df = pd.DataFrame.from_records(ds)
    return df


def get_cn_industry():
    """获取国证四级行业分类"""
    cate = '国证行业分类'
    col_names = {
        '分类编码': '国证四级行业编码',
        '分类名称': '国证四级行业',
        '股票代码': 'sid',
    }
    df = get_industry_stock_list('国证行业分类')
    if df.empty:
        msg = '在本地数据库中无法获取行业分类数据。\n'
        msg += '这将导致股票分类数据缺失。\n'
        msg += '运行`stock clsf`提取网络数据并存储在本地数据库。'
        warnings.warn(msg)
        return pd.DataFrame(columns=col_names.values())
    df.rename(columns=col_names, inplace=True)
    df['sid'] = df['sid'].map(lambda x: int(x))
    for level in (1, 2, 3):
        pattern_str = r"^Z\d{" + str(level*2) + "}$"
        pattern = re.compile(pattern_str)
        maps = get_bom_maps(cate, pattern)
        digit = level * 2 + 1
        u_num = NUM_MAPS[level]
        code_col = '国证{}行业编码'.format(u_num)
        name_col = '国证{}行业'.format(u_num)
        df[code_col] = df['国证四级行业编码'].map(lambda x: x[:digit])
        df[name_col] = df['国证四级行业编码'].map(lambda x: maps[x[:digit]])
    return df


def supper_sector_code_map(sector_code):
    """行业分类映射超级行业分类"""
    return int(str(sector_code)[0])


def get_sector():
    cn_industry = get_cn_industry()
    # Sector自定义因子，代码为整数
    cn_industry['sector_code'] = cn_industry['国证一级行业编码'].map(
        CN_TO_SECTOR, na_action='ignore').astype('int64')
    cn_industry['super_sector_code'] = cn_industry['sector_code'].map(
        supper_sector_code_map, na_action='ignore').astype('int64')
    cols = ['sid', '国证一级行业', 'sector_code', 'super_sector_code']
    return cn_industry[cols]


def get_industry():
    """申万、同花顺、证监会、国证、Sector行业分类

    使用申万行业分类时间作为基准。
    """
    sw_industry = get_sw_industry()
    ths_industry = get_ths_industry()
    zjh_industry = get_zjh_industry()
    sector = get_sector()

    df = sw_industry.set_index('sid').join(
        ths_industry.set_index('sid'),
    ).join(
        zjh_industry.set_index('sid'),
    ).join(
        sector.set_index('sid'),
    )
    df.reset_index(inplace=True)
    return df

# endregion

# region 股票概念


def get_ths_concept():
    """同花顺股票概念"""
    db = get_db()
    collection = db['同花顺概念']
    pipeline = [
        {
            '$unwind': {
                'path': '$股票列表'
            }
        }, {
            '$project': {
                '_id': 0,
                '概念名称': 1,
                'asof_date': "$日期",
                'sid': "$股票列表",
            }
        }
    ]
    docs = collection.aggregate(pipeline)
    df = pd.DataFrame.from_records(docs)
    df = pd.pivot_table(df,
                        values='概念名称',
                        index=['asof_date', 'sid'],
                        columns='概念名称',
                        aggfunc=np.count_nonzero,
                        fill_value=0)
    # 规范列名称，列名称不得以下划线、数字开头
    # 且名称中不得含 '.'字符
    d = get_bcolz_col_names(df.columns)
    df.columns = get_bcolz_col_names(d.values())
    df = df.astype(bool).reset_index()
    # 选择股票 【原始数据中包含非法记录】
    cond = df['sid'].str.match(r"\d{6}")
    df = df[cond]
    df['sid'] = df['sid'].astype('int64')
    return df, d

# endregion
