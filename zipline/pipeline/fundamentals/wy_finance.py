# 财务报告、财务指标、TTM
import pandas as pd
import re
from cnswd.mongodb import get_db

from ..common import AD_FIELD_NAME, TS_FIELD_NAME

UNIT_PAT = re.compile(r"\((万)?元\)$|\(%\)")
TO_DORP_PAT = re.compile(r'[ :：、()]')


def _normalized_col_name(x):
    """规范列财务报告项目在`pipeline`中的列名称

    去除列名称中的前导数字，中间符号，保留文字及尾部数字
    """
    x = re.sub(UNIT_PAT, '', x)
    x = re.sub(TO_DORP_PAT, '', x)
    return x

# region 提取公告日期


def get_ggrq():
    """提取财务报告公告日期"""
    db = get_db('wy')
    collection = db['预约披露']
    docs = collection.find(
        projection={
            '_id': 0,
            'sid': '$股票代码',
            TS_FIELD_NAME: '$实际披露',
            AD_FIELD_NAME: '报告年度',
        }
    )
    df = pd.DataFrame.from_records(docs)
    df['sid'] = df['sid'].astype('int64')
    df.drop_duplicates(subset=[AD_FIELD_NAME, 'sid'],
                       keep='last', inplace=True)
    df.set_index([AD_FIELD_NAME, 'sid'], inplace=True)
    return df


# endregion

# region 财务报告

def _add_ggrq(df, ggrq):
    """添加公告日期"""
    # 涉及到修正财报情形?
    df.drop_duplicates(subset=[AD_FIELD_NAME, 'sid'],
                       keep='last', inplace=True)
    df.set_index([AD_FIELD_NAME, 'sid'], inplace=True)
    df[TS_FIELD_NAME] = ggrq.copy().reindex(df.index)
    df.reset_index(inplace=True)
    cond = df[TS_FIELD_NAME].isna()
    # 无公告日期在报告期基础上加 45 天
    df.loc[cond, TS_FIELD_NAME] = df.loc[cond,
                                         AD_FIELD_NAME] + pd.Timedelta(days=45)
    return df


def _get_p_data(name):
    """报告期财务报告"""
    db = get_db('wy')
    collection = db[name]
    docs = collection.find(
        projection={
            '_id': 0,
            '更新时间': 0,
        }
    )
    df = pd.DataFrame.from_records(docs)
    # 规范列名称
    df.columns = df.columns.map(_normalized_col_name)
    df.rename(columns={'股票代码': 'sid',  '报告日期': AD_FIELD_NAME},
              inplace=True)
    df['sid'] = df['sid'].astype('int64')
    return df

# endregion

# region 单季度财务指标


def get_q_indicator(name):
    """单季度财务指标"""
    db = get_db('wy')
    collection = db[name]
    docs = collection.find(
        projection={
            '_id': 0,
            '更新时间': 0,
        }
    )
    df = pd.DataFrame.from_records(docs)
    # 规范列名称
    df.columns = df.columns.map(_normalized_col_name)
    df.rename(columns={'股票代码': 'sid',  '报告日期': AD_FIELD_NAME},
              inplace=True)
    df['sid'] = df['sid'].astype('int64')
    return df

# endregion

# region 业绩预告


def get_yjyg():
    """业绩预告"""
    db = get_db('wy')
    collection = db['业绩预告']
    docs = collection.find(
        projection={
            '_id': 0,
            '更新时间': 0,
            '预测内容': 0
        }
    )
    df = pd.DataFrame.from_records(docs)
    # TODO:业绩预告 存在 公告日期 < 报告日期
    df.rename(columns={'股票代码': 'sid',  '报告日期': AD_FIELD_NAME, '公告日期': TS_FIELD_NAME},
              inplace=True)
    df['sid'] = df['sid'].astype('int64')
    return df

# endregion
