# 上市日期、注册资本
from cnswd.mongodb import get_db
import pandas as pd
import re
import akshare as ak
from toolz.dicttoolz import merge


T_PAT = re.compile(",")


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
    return df.set_index('股票代码')


def _to_float(x):
    # 注册资本单位统一为万元
    if pd.isna(x):
        return x
    if x[-2:] != '万元':
        raise ValueError(f"注册资本单位错误，期望万元，实际为{x[-2:]}")
    return float(T_PAT.sub('', x[:-2])) * 10000


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


# def get_info():
#     """基础信息

#     包含 B股
#     以股票代码为索引
#     """
#     df1 = _gsjj()
#     df2 = _ipo()
#     df = pd.concat([df1, df2], axis=1, sort=True)
#     return df.dropna()
