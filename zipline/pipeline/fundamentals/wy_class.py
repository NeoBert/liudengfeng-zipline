# 分类
# 1. 行业
#   1.1 申万行业
#   1.2 同花顺行业
#   1.3 证监会行业
# 2. 地域
# 3. 概念
import pandas as pd

from cnswd.mongodb import get_db

from ..common import AD_FIELD_NAME, SID_FIELD_NAME, TS_FIELD_NAME


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
    # 舍弃asof_date为空部分
    df = df[df[AD_FIELD_NAME].notna()]
    return df


def get_ths_industry():
    pass


def get_zjh_industry():
    pass


def get_region():
    pass


def get_concept():
    pass
