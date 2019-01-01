"""

预处理数据

"""
import warnings
import numpy as np
import pandas as pd

from .constants import SECTOR_NAMES, SUPER_SECTOR_NAMES
from ..common import AD_FIELD_NAME, SID_FIELD_NAME, TS_FIELD_NAME
from .sql import get_stock_info, get_cn_industry, get_concept_info, field_code_concept_maps

# ========================辅助函数========================= #


def _to_dt(s, target_tz):
    """输入序列转换为DatetimeIndex(utc)"""
    ix = pd.DatetimeIndex(s)
    if ix.tz:
        return ix.tz_convert(target_tz)
    else:
        return ix.tz_localize(target_tz)


def _normalize_ad_ts_sid(df, ndays=0, nhours=8, target_tz='utc'):
    """通用转换
    股票代码 -> sid(int64)
    date(date) -> asof_date(timestamp)
    date(date) + ndays -> timestamp(timestamp)
    确保timestamp >= asof_date
    """
    if AD_FIELD_NAME in df.columns:
        # 如果asof_date存在，则只需要转换数据类型及目标时区
        df[AD_FIELD_NAME] = _to_dt(df[AD_FIELD_NAME], target_tz)
        df[AD_FIELD_NAME] = df[AD_FIELD_NAME] + pd.Timedelta(hours=nhours)
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

    if SID_FIELD_NAME in df.columns:
        df[SID_FIELD_NAME] = df[SID_FIELD_NAME].map(lambda x: int(x))

    return df


def _fillna(df, start_names, default):
    """
    修改无效值
    为输入df中以指定列名称开头的列，以默认值代替nan
    """
    # 找出以指定列名词开头的列
    col_names = []
    for col_pat in start_names:
        for col in df.columns[df.columns.str.startswith(col_pat)]:
            col_names.append(col)
    # 替换字典
    values = {}
    for col in col_names:
        values[col] = default
    df.fillna(value=values, inplace=True)


def _handle_cate(df, col_pat, maps):
    """指定列更改为编码，输出更改后的表对象及类别映射"""
    cols = df.columns[df.columns.str.startswith(col_pat)]
    for col in cols:
        c = df[col].astype('category')
        df[col] = c.cat.codes.astype('int64')
        maps[col] = {k: v for k, v in enumerate(c.cat.categories)}
        maps[col].update({-1: '未定义'})
    return df, maps


def sector_code_map(industry_code):
    """
    国证行业分类映射为部门行业分类
    
    国证一级行业分10类，转换为sector共11组，单列出房地产。
    """
    if industry_code[:3] == 'Z01':
        return 309
    if industry_code[:3] == 'Z02':
        return 101
    if industry_code[:3] == 'Z03':
        return 310
    if industry_code[:3] == 'Z04':
        return 205
    if industry_code[:3] == 'Z05':
        return 102
    if industry_code[:3] == 'Z06':
        return 206
    if industry_code.startswith('Z07'):
        if industry_code[:5] == 'Z0703':
            return 104
        else:
            return 103
    if industry_code[:3] == 'Z08':
        return 311
    if industry_code[:3] == 'Z09':
        return 308
    if industry_code[:3] == 'Z10':
        return 207
    return -1


def supper_sector_code_map(sector_code):
    """行业分类映射超级行业分类"""
    if sector_code == -1:
        return -1
    return int(str(sector_code)[0])


def get_static_info_table():
    """股票静态信息合并表"""
    stocks = get_stock_info()
    cn_industry = get_cn_industry()
    cn_industry['sector_code'] = cn_industry['国证四级行业编码'].map(sector_code_map)
    cn_industry['super_sector_code'] = cn_industry['sector_code'].map(
        supper_sector_code_map)
    concept = get_concept_info()
    df = stocks.join(
        cn_industry.set_index('sid'), on='sid'
    ).join(
        concept.set_index('sid'), on='sid'
    )
    maps = {}
    _, name_maps = field_code_concept_maps()
    cate_cols_pat = ['市场', '省份', '城市', '证监会', '国证', '申万']
    for col_pat in cate_cols_pat:
        df, maps = _handle_cate(df, col_pat, maps)
    maps['概念'] = name_maps
    maps['部门'] = SECTOR_NAMES
    maps['超级部门'] = SUPER_SECTOR_NAMES
    # 填充无效值
    bool_cols = df.columns[df.columns.str.match(r'A\d{3}')]
    _fillna(df, bool_cols, False)
    _fillna(df, cate_cols_pat, -1)
    return df, maps
