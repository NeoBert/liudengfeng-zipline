"""

预处理数据

TODO：由于最终会丢失时区信息，写入时所有时间列均不带时区。
"""
import warnings

import numpy as np
import pandas as pd

from ..common import AD_FIELD_NAME, SID_FIELD_NAME, TS_FIELD_NAME
from .constants import CN_TO_SECTOR, SECTOR_NAMES, SUPER_SECTOR_NAMES
from .localdata import (field_code_concept_maps, get_cn_industry,
                        get_concept_info, get_investment_rating_data,
                        get_short_name_changes, get_stock_info,
                        get_sw_industry, get_zjh_industry)

# ========================辅助函数========================= #


def _investment_score(x):
    """投资评级分数"""
    if x == '买入':
        return 5
    elif x == '增持':
        return 4
    elif x == '中性':
        return 3
    elif x == '减持':
        return 2
    elif x == '卖出':
        return 1
    elif x in ('None', np.nan, None, '-', '不评级'):
        return 0
    raise ValueError(f'无效值{x}')


def _to_dt(s, target_tz):
    """输入序列转换为DatetimeIndex(utc)"""
    ix = pd.DatetimeIndex(s)
    if ix.tz:
        return ix.tz_convert(target_tz)
    else:
        return ix.tz_localize(target_tz)


def _normalize_ad_ts_sid(df, ndays=0, nhours=8, target_tz='utc'):
    """规范`asof_date`、`timestamp`、`sid`

    操作：
        股票代码 -> sid(int64)
        date(date) -> asof_date(timestamp)
        date(date) + ndays -> timestamp(timestamp)
        确保timestamp >= asof_date
    """
    if AD_FIELD_NAME in df.columns:
        # 如果asof_date存在，则只需要转换数据类型及目标时区
        df[AD_FIELD_NAME] = _to_dt(df[AD_FIELD_NAME],
                                   target_tz) + pd.Timedelta(hours=nhours)
    else:
        raise ValueError('数据必须包含"{}"列'.format(AD_FIELD_NAME))

    if TS_FIELD_NAME in df.columns:
        # 如果timestamp存在，则只需要转换数据类型及目标时区
        df[TS_FIELD_NAME] = _to_dt(df[TS_FIELD_NAME], target_tz)
    else:
        # 如果timestamp**不存在**，则需要在asof_date基础上调整ndays
        if ndays != 0:
            df[TS_FIELD_NAME] = df[AD_FIELD_NAME] + pd.Timedelta(days=ndays)
        else:
            # 确保 df[TS_FIELD_NAME] >= df[AD_FIELD_NAME]
            df[TS_FIELD_NAME] = df[AD_FIELD_NAME] + \
                pd.Timedelta(hours=nhours+1)

    if SID_FIELD_NAME not in df.columns:
        if '证券代码' in df.columns:
            df.rename(columns={"证券代码": "sid"}, inplace=True)
        if '股票代码' in df.columns:
            df.rename(columns={"股票代码": "sid"}, inplace=True)
    df[SID_FIELD_NAME] = df[SID_FIELD_NAME].map(lambda x: int(x))
    return df


def _fillna(df, col_dtypes):
    """规范列类型及缺失值"""
    default_miss = {'int64': -1, 'bool': False, 'str': '未知'}
    for col, dtype in col_dtypes.items():
        if df[col].hasnans:
            values = {}
            values[col] = default_miss[dtype]
            df.fillna(value=values, inplace=True)
        df[col] = df[col].astype(dtype)
    return df


def _handle_cate(df, col_pat, maps):
    """指定列更改为编码，输出更改后的表对象及类别映射"""
    cols = df.columns[df.columns.str.startswith(col_pat)]
    for col in cols:
        values = {col: '未知'}
        df.fillna(values, inplace=True)
        c = df[col].astype('category')
        df[col] = c.cat.codes.astype('int64')
        maps[col] = {k: v for k, v in enumerate(c.cat.categories)}
    return df, maps


def supper_sector_code_map(sector_code):
    """行业分类映射超级行业分类"""
    if sector_code == -1:
        return -1
    return int(str(sector_code)[0])


def get_static_info_table():
    """
    股票静态信息合并表
    """
    stocks = get_stock_info()
    sw_industry = get_sw_industry()
    cn_industry = get_cn_industry()
    zjh_industry = get_zjh_industry()

    # Sector自定义因子，代码为整数
    cn_industry['sector_code'] = cn_industry['国证一级行业编码'].map(
        CN_TO_SECTOR, na_action='ignore').astype('int64')
    cn_industry['super_sector_code'] = cn_industry['sector_code'].map(
        supper_sector_code_map, na_action='ignore').astype('int64')
    concept = get_concept_info()

    df = stocks.set_index('sid').join(
        sw_industry.set_index('sid'),
    ).join(
        cn_industry.set_index('sid'),
    ).join(
        zjh_industry.set_index('sid'),
    ).join(
        concept.set_index('sid'),
    )
    maps = {}
    _, name_maps = field_code_concept_maps()
    maps['概念'] = name_maps

    # 规范列数据类型及填充无效值
    bool_cols = df.columns[df.columns.str.match(r'A\d{3}')]
    col_dtypes = {col: 'bool' for col in bool_cols}
    col_dtypes.update({
        'sector_code': 'int64',
        'super_sector_code': 'int64',
        'sw_sector': 'int64',
        '上市地点': 'str',
        '会计师事务所': 'str',
        '律师事务所': 'str',
    })
    industry_cols = {col: 'str' for col in df.columns if '级行业' in col}
    col_dtypes.update(industry_cols)
    df = _fillna(df, col_dtypes)
    return df, maps


def get_investment_rating():
    """
    投资评级数据

    注：将研究机构、研究员转换为类别，加快读取速度
    """
    maps = {}
    df = get_investment_rating_data()
    df = _normalize_ad_ts_sid(df)
    cate_cols_pat = ['研究机构简称', '研究员名称']
    for col_pat in cate_cols_pat:
        df, maps = _handle_cate(df, col_pat, maps)
    df['投资评级'] = df['投资评级'].map(_investment_score)  # 转换为整数值
    return df, maps


def get_short_name_history():
    """股票简称更改历史"""
    maps = {}
    df = get_short_name_changes()
    cate_cols_pat = ['股票简称']
    for col_pat in cate_cols_pat:
        df, maps = _handle_cate(df, col_pat, maps)
    return df, maps
