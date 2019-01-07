"""

预处理数据

"""
import warnings

import numpy as np
import pandas as pd

from ..common import AD_FIELD_NAME, SID_FIELD_NAME, TS_FIELD_NAME
from .constants import SECTOR_NAMES, SUPER_SECTOR_NAMES
from .sql import (field_code_concept_maps, get_cn_industry, get_concept_info,
                  get_investment_rating_data, get_stock_info)

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
    elif x in ('-', '不评级'):
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
        df[AD_FIELD_NAME] = _to_dt(df[AD_FIELD_NAME], target_tz) + pd.Timedelta(hours=nhours)
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


def _fill_missing_value(df, start_names, default):
    """
    修改无效值
    为输入df中以指定列名称开头的列，以默认值代替nan

    与默认值一致
        整数        默认值 -1
        浮点        默认值nan
        对象(含str) `未知`
        时间        NaT
        逻辑        False
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
    # 对象类别需要填充缺失值
    for col in df.columns:
        cond_1 = pd.core.dtypes.common.is_object_dtype(df[col])
        cond_2 = col not in col_names
        if cond_1 & cond_2:
            values[col] = '未知'
    df.fillna(value=values, inplace=True)


def _handle_cate(df, col_pat, maps):
    """指定列更改为编码，输出更改后的表对象及类别映射"""
    cols = df.columns[df.columns.str.startswith(col_pat)]
    for col in cols:
        c = df[col].astype('category')
        df[col] = c.cat.codes.astype('int64')
        maps[col] = {k: v for k, v in enumerate(c.cat.categories)}
        maps[col].update({0: '未知'})
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
    """
    股票静态信息合并表

    注：总行数约4000行，无需将str转换为类别。
    """
    stocks = get_stock_info()
    cn_industry = get_cn_industry()
    # 类似Sector自定义因子，代码为整数
    cn_industry['sector_code'] = cn_industry['国证四级行业编码'].map(sector_code_map).astype('int64')
    cn_industry['super_sector_code'] = cn_industry['sector_code'].map(
        supper_sector_code_map).astype('int64')
    # cn_industry['部门'] = cn_industry['sector_code'].map(SECTOR_NAMES)
    # cn_industry['超级部门'] = cn_industry['super_sector_code'].map(
    #     SUPER_SECTOR_NAMES)
    # del cn_industry['sector_code']
    # del cn_industry['super_sector_code']
    concept = get_concept_info()
    df = stocks.join(
        cn_industry.set_index('sid'), on='sid'
    ).join(
        concept.set_index('sid'), on='sid'
    )
    maps = {}
    _, name_maps = field_code_concept_maps()
    maps['概念'] = name_maps
    # 填充无效值
    bool_cols = df.columns[df.columns.str.match(r'A\d{3}')]
    _fill_missing_value(df, bool_cols, False)
    # _fill_missing_value(df, cate_cols_pat, 0)
    return df, maps


def get_investment_rating():
    """
    投资评级数据

    注：行数超过50万行，需要将研究机构、研究员转换为类别。
    """
    maps = {}
    df = get_investment_rating_data()
    cate_cols_pat = ['研究机构简称', '研究员名称', '是否首次评级', '评价变化',  '前一次投资评级']
    for col_pat in cate_cols_pat:
        df, maps = _handle_cate(df, col_pat, maps)
    df['投资评级'] = df['投资评级经调整'] # 统一评级
    df['投资评级'] = df['投资评级'].map(_investment_score) # 转换为整数值
    del df['投资评级经调整']
    # 填充无效值
    _fill_missing_value(df, cate_cols_pat, -1)
    return df, maps
