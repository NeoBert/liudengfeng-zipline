from concurrent.futures.thread import ThreadPoolExecutor

import pandas as pd

from cnswd.mongodb import get_db
from cnswd.setting.constants import MAX_WORKER

from ..common import AD_FIELD_NAME, TS_FIELD_NAME

# region 动态数据


def _fix_name(old):
    """全角修正为半角"""
    new = old.replace('Ａ', 'A')
    new = new.replace('Ｂ', 'B')
    new = new.replace('Ｈ', 'H')
    return new


def _change_hist(code):
    # 深发展Ａ -> 深发展A
    db = get_db('wy_stock_daily')
    collection = db[code]
    if collection.estimated_document_count() == 0:
        return pd.DataFrame()
    docs = collection.find(
        projection={
            '_id': 0,
            '股票简称': '$名称',
            AD_FIELD_NAME: '$日期'
        },
        sort=[('日期', 1), ('名称', 1,)]
    )
    df = pd.DataFrame.from_records(docs)
    df['股票简称'] = df['股票简称'].map(_fix_name)
    cond = df['股票简称'] != df['股票简称'].shift(1)
    df = df.loc[cond, :]
    df['sid'] = int(code)
    return df


def get_short_name_changes():
    """股票简称变动历史"""
    db = get_db('wy_stock_daily')
    codes = db.list_collection_names()
    # 3878只股票 用时 48s
    with ThreadPoolExecutor(MAX_WORKER) as pool:
        r = pool.map(_change_hist, codes)
    df = pd.concat(r, ignore_index=True)
    return df


def get_margin_data():
    """融资融券数据"""
    db = get_db('wy')
    collection = db['融资融券']
    projection = {
        '_id': 0,
        '股票简称': 0,
        '更新时间': 0,
    }
    # sort = [('股票代码', 1), ('交易日期', 1)]
    df = pd.DataFrame.from_records(
        collection.find(projection=projection))
    df.rename(columns={'交易日期': AD_FIELD_NAME, '股票代码': 'sid'}, inplace=True)
    df['sid'] = df['sid'].astype('int64')
    # 设置晚8小时
    # df['asof_date'] = df['timestamp'] - pd.Timedelta(hours=8)
    df.sort_values(['sid', AD_FIELD_NAME], inplace=True, ignore_index=True)
    return df


def _to_timestamp(year):
    return pd.Period(year=year, freq='Y').to_timestamp()


def get_dividend_data():
    """现金股利"""
    db = get_db('wy')
    collection = db['分红配股']
    # 使用股权登记日作为 asof_date
    # 此指标仅用于计算年度股息之用，不涉及到所谓知晓日期
    pipeline = [
        {
            '$project': {
                '_id': 0,
                'sid': '$股票代码',
                '分红年度': 1,
                AD_FIELD_NAME: '$股权登记日',
                '每股派息': '$派息(每10股)',
            }
        }
    ]
    docs = collection.aggregate(pipeline)
    df = pd.DataFrame.from_records(docs)
    # 2019 -> Timestamp('2019-01-01 00:00:00')
    df['分红年度'] = df['分红年度'].map(_to_timestamp)
    # 首先将日期缺失值默认为分红年度后一个季度
    cond = df['asof_date'].isnull()
    df.loc[cond, 'asof_date'] = df.loc[cond, '分红年度'] + pd.Timedelta(days=45)
    # 重要：对未分派的记录，不得舍弃
    # 派息NaN -> 0.0 不影响实际意义，加快读写速度
    values = {'每股派息': 0.0}
    df.fillna(value=values, inplace=True)
    # 数值更改为每股派息
    df['每股派息'] = df['每股派息'] / 10.0
    df.sort_values(['sid', 'asof_date'], inplace=True, ignore_index=True)
    df['sid'] = df['sid'].astype('int64')
    return df


# endregion
def _handle_cate(df, col_pat, maps):
    """指定列更改为编码，输出更改后的表对象及类别映射"""
    cols = df.columns[df.columns.str.startswith(col_pat)]
    for col in cols:
        values = {col: ''}  # 类别缺失统一以 空白字符串 替代
        df.fillna(values, inplace=True)
        c = df[col].astype('category')
        df[col] = c.cat.codes.astype('int64')
        maps[col] = {k: v for k, v in enumerate(c.cat.categories)}
    return df, maps


def get_investment_rating_data():
    """投资评级

    备注

    大量字符写入时间极长，转换为类别，加快写入速度。
    """
    db = get_db('wy')
    collection = db['投资评级']
    pipeline = [
        {
            '$project': {
                '_id': 0,
                'sid': '$股票代码',
                AD_FIELD_NAME: '$评级日期',
                '评级': '$最新评级',
                '分析师': 1,
                '评级机构': 1,
            }
        }
    ]
    docs = collection.aggregate(pipeline)
    df = pd.DataFrame.from_records(docs)
    # 可能数据没有清洗干净
    cond = df['sid'].str.match(r"\d{6}")
    df = df[cond]
    df['sid'] = df['sid'].astype('int64')

    # 至少相差一小时
    # df['asof_date'] -= pd.Timedelta(hours=1)
    cate_cols_pat = ['评级机构', '分析师']
    maps = {}
    for col_pat in cate_cols_pat:
        df, maps = _handle_cate(df, col_pat, maps)
    return df, maps
