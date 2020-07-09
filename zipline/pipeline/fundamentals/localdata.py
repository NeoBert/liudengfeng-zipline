"""

查询本地数据

尽管`bcolz`最终会丢失时区信息，但写入时依旧将时间列转换为UTC时区。
除asof_date、timestamp列外，其余时间列无需转换

"""

import re
import warnings
from concurrent.futures.thread import ThreadPoolExecutor

import numpy as np
import pandas as pd

from cnswd.cninfo.utils import _rename
from cnswd.mongodb import get_db
from cnswd.setting.constants import MAX_WORKER
from cnswd.utils.tools import filter_a

from ..common import AD_FIELD_NAME, TS_FIELD_NAME
from .constants import SW_SECTOR_MAPS

# from cnswd.store import (ClassifyTreeStore, DataBrowseStore, MarginStore,
#                          TctGnStore, WyStockDailyStore)
LOCAL_TZ = 'Asia/Shanghai'
warnings.filterwarnings('ignore')

STOCK_PAT = re.compile(r"^\d{6}$")
A_STOCK_PAT = re.compile(r"^[036]\d{5}$")

NUM_MAPS = {
    1: '一级',
    2: '二级',
    3: '三级',
    4: '四级',
}

TO_DORP_PAT_0 = re.compile(r'^[（]?[一二三四五六七八九][）]?([(（]\d[)）])?[、]?')
TO_DORP_PAT_1 = re.compile(r'^[1-9]、|[（()][1-9][)）]')
TO_DORP_PAT_2 = re.compile(r'[、：（）-]|\_|\(|\)')

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

# region 辅助函数


def _to_timestamp(df):
    # 无需 tz 信息
    for col in [AD_FIELD_NAME, TS_FIELD_NAME]:
        if col in df.columns:
            # df[col] = df[col].map(lambda x: pd.Timestamp(
            #     x, tz=LOCAL_TZ).tz_convert('UTC').to_pydatetime())
            df[col] = df[col].map(pd.Timestamp)
    return df


def _normalized_col_name(x):
    """规范列财务报告项目在`pipeline`中的列名称

    去除列名称中的前导数字，中间符号，保留文字及尾部数字
    """
    # 去除前导序号
    x = re.sub(TO_DORP_PAT_0, '', x)
    x = re.sub(TO_DORP_PAT_1, '', x)
    x = re.sub(TO_DORP_PAT_2, '', x)
    return x


def _select_only_a(df, only_A, code_col='股票代码'):
    """仅含A股数据"""
    if only_A:
        cond1 = df[code_col].str.startswith('2')
        cond2 = df[code_col].str.startswith('9')
        df = df.loc[~(cond1 | cond2), :]
    return df


# endregion

# region 静态数据


def get_stock_info(only_A=True):
    """股票基础信息"""
    db = get_db('cninfo')
    collection = db['基本资料']
    projection = {
        '_id': 0,
        '股票代码': 1,
        '上市日期': 1,
        # 与行业分类重复
        # '申万行业一级名称': 1,
        # '申万行业二级名称': 1,
        # '申万行业三级名称': 1,
        # '证监会一级行业名称': 1,
        # '证监会二级行业名称': 1,
        '省份': 1,
        '城市': 1,
        '注册资本': 1,
        '上市状态': 1,
        '律师事务所': 1,
        '会计师事务所': 1,
        '上市地点': 1,
    }
    sort = {'股票代码': 1}
    pipeline = [
        {'$project': projection},
        {'$sort': sort}
    ]
    if only_A:
        pipeline.insert(0, MATCH_ONLY_A)
    df = pd.DataFrame.from_records(
        collection.aggregate(pipeline))
    df.drop_duplicates('股票代码', inplace=True)
    # 剔除未上市、未交易的无效股票
    cond1 = ~ df['上市日期'].isnull()
    cond2 = df['上市日期'] <= pd.Timestamp('today')
    df = df.loc[cond1 & cond2, :]
    df['asof_date'] = df['上市日期'] - pd.Timedelta(days=1)
    df = _to_timestamp(df)
    # 注册资本转换 -> 十分位数
    df['注册资本十分位数'] = pd.qcut(np.log(df['注册资本'].values), 10, labels=False)
    df.rename(columns={'股票代码': 'sid'}, inplace=True)
    df['sid'] = df['sid'].map(lambda x: int(x))
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


def get_industry_stock_list(cate, only_A):
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
    if only_A:
        pipeline.append(MATCH_ONLY_A)
    ds = collection.aggregate(pipeline)
    df = pd.DataFrame.from_records(ds)
    return df


def get_cn_industry(only_A=True):
    """获取国证四级行业分类"""
    cate = '国证行业分类'
    col_names = {
        '分类编码': '国证四级行业编码',
        '分类名称': '国证四级行业',
        '股票代码': 'sid',
    }
    df = get_industry_stock_list('国证行业分类', only_A)
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


def get_sw_industry(only_A=True):
    """获取申万行业三级分类"""
    cate = "申万行业分类"
    col_names = {
        '分类编码': '申万三级行业编码',
        '分类名称': '申万三级行业',
        '股票代码': 'sid',
    }
    df = get_industry_stock_list(cate, only_A)
    if df.empty:
        msg = '在本地数据库中无法获取行业分类数据。\n'
        msg += '这将导致股票分类数据缺失。\n'
        msg += '运行`stock clsf`提取网络数据并存储在本地数据库。'
        warnings.warn(msg)
        return pd.DataFrame(columns=col_names.values())
    df.rename(columns=col_names, inplace=True)
    # S90 为无效数据
    cond = df['申万三级行业编码'] == 'S90'
    df = df[~cond]
    df['sid'] = df['sid'].map(lambda x: int(x))
    for level in (1, 2):
        pattern_str = r"^S\d{" + str(level*2) + "}$"
        pattern = re.compile(pattern_str)
        maps = get_bom_maps(cate, pattern)
        digit = level * 2 + 1
        u_num = NUM_MAPS[level]
        code_col = '申万{}行业编码'.format(u_num)
        name_col = '申万{}行业'.format(u_num)
        df[code_col] = df['申万三级行业编码'].map(lambda x: x[:digit])
        df[name_col] = df['申万三级行业编码'].map(lambda x: maps.get(x[:digit], '综合'))
    sw_code_maps = {v: k for k, v in SW_SECTOR_MAPS.items()}
    df['sw_sector'] = df['申万一级行业编码'].map(
        lambda x: sw_code_maps[x]).astype('int64')
    return df


def get_zjh_industry(only_A=True):
    """获取证监会行业二级分类"""
    cate = '证监会行业分类'
    col_names = {
        '分类编码': '证监会二级行业编码',
        '分类名称': '证监会二级行业',
        '股票代码': 'sid',
    }
    df = get_industry_stock_list(cate, only_A)
    if df.empty:
        msg = '在本地数据库中无法获取行业分类数据。\n'
        msg += '这将导致股票分类数据缺失。\n'
        msg += '运行`stock clsf`提取网络数据并存储在本地数据库。'
        warnings.warn(msg)
        return pd.DataFrame(columns=col_names.values())
    df.rename(columns=col_names, inplace=True)
    # 混杂了申万编码，剔除
    cond = df['证监会二级行业编码'].str.len() == 3
    df = df[cond]
    df['sid'] = df['sid'].map(lambda x: int(x))
    for level in (1, ):
        pattern_str = r"^[A-R]$"
        pattern = re.compile(pattern_str)
        maps = get_bom_maps(cate, pattern)
        digit = (level-1) * 2 + 1
        u_num = NUM_MAPS[level]
        code_col = '证监会{}行业编码'.format(u_num)
        name_col = '证监会{}行业'.format(u_num)
        df[code_col] = df['证监会二级行业编码'].map(lambda x: x[:digit])
        df[name_col] = df['证监会二级行业编码'].map(lambda x: maps.get(x[:digit], '综合'))
    return df


def concept_categories():
    """概念类别映射{代码:名称}"""
    db = get_db()
    collection = db['同花顺概念']
    pipeline = [
        {
            '$project': {
                '_id': 0,
                '概念编码': 1,
                '概念名称': 1,
            }
        }
    ]
    ds = collection.aggregate(pipeline)
    df = pd.DataFrame.from_records(ds)
    try:
        df.columns = ['code', 'name']
    except ValueError:
        raise NotImplementedError('本地数据库中"股票概念数据"为空，请运行`stock thsgn`')
    df.sort_values('code', inplace=True)
    return df.set_index('code').to_dict()['name']


def field_code_concept_maps():
    """
    概念映射二元组

    Returns
    -------
    res ： 元组
        第一项：原始概念编码 -> 数据集字段编码（新编码）
        第二项：数据集字段编码 -> 概念名称

    Example
    -------
    第一项：{'00010002': 'A001', '00010003': 'A002', '00010004': 'A003', ...
    第二项：{'A001': '参股金融', 'A002': '可转债', 'A003': '上证红利'...

    """
    vs = concept_categories()
    no, key = pd.factorize(list(vs.keys()), sort=True)
    id_maps = {v: 'A{}'.format(str(k + 1).zfill(3)) for k, v in zip(no, key)}
    name_maps = {v: vs[k] for (k, v) in id_maps.items()}
    return id_maps, name_maps


def get_concept_info(only_A=True):
    """股票概念编码信息

    Keyword Arguments:
        only_A {bool} -- 只包含A股代码 (default: {True})

    Returns:
        pd.DataFrame -- 股票概念编码信息表

    Example:
    >>> get_concept_info().head(3)
    sid   A001   A002   A003   A004   A005  ...   A205
      1  False  False  False  False  False  ...  False
      2  False  False  False  False  False  ...  False
      4  False  False  False   True  False  ...  False
    """
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
                '概念编码': 1,
                # '概念名称': 1,
                '股票列表': 1
            }
        }
    ]
    ds = collection.aggregate(pipeline)

    def func(x):
        if only_A:
            return A_STOCK_PAT.match(x['股票列表'])
        else:
            return STOCK_PAT.match(x['股票列表'])

    ds = filter(func, ds)
    df = pd.DataFrame.from_records(ds)
    df.rename(columns={'股票列表': 'sid'}, inplace=True)

    out = pd.pivot_table(df,
                         values='概念编码',
                         index='sid',
                         columns='概念编码',
                         aggfunc=np.count_nonzero,
                         fill_value=0)

    id_maps, _ = field_code_concept_maps()
    out.rename(columns=id_maps, inplace=True)
    out = out.astype('bool').reset_index()
    out['sid'] = out['sid'].map(lambda x: int(x))
    return out


# endregion

# region 动态数据

def _change_hist(code):
    # 深发展Ａ -> 深发展A
    db = get_db('wy_stock_daily')
    collection = db[code]
    if collection.estimated_document_count() == 0:
        return pd.DataFrame()
    records = collection.find(
        projection={'_id': 0, '名称': 1, '日期': 1},
        sort=[('日期', 1), ('名称', 1,)])
    df = pd.DataFrame.from_records(records)
    df['名称'] = df['名称'].map(_rename)
    cond = df['名称'] != df['名称'].shift(1)
    df = df.loc[cond, :]
    df.rename(columns={'日期': 'asof_date', '名称': '股票简称'}, inplace=True)
    df['sid'] = int(code)
    return df


def get_short_name_changes(only_A=True):
    """股票简称变动历史"""
    db = get_db('wy_stock_daily')
    codes = db.list_collection_names()
    if only_A:
        codes = filter_a(codes)
    # 3878只股票 用时 48s
    with ThreadPoolExecutor(MAX_WORKER) as pool:
        r = pool.map(_change_hist, codes)
    df = pd.concat(r, ignore_index=True)
    return df


def get_margin_data(only_A=True):
    """融资融券数据"""
    db = get_db('cninfo')
    collection = db['融资融券明细']
    projection = {
        '_id': 0,
        '股票简称': 0,
    }
    # sort = [('股票代码', 1), ('交易日期', 1)]
    df = pd.DataFrame.from_records(
        collection.find(projection=projection))
    df = _select_only_a(df, only_A, '股票代码')
    df.rename(columns={'交易日期': 'timestamp', '股票代码': 'sid'}, inplace=True)
    df['sid'] = df['sid'].map(lambda x: int(x))
    # 设置晚8小时
    df['asof_date'] = df['timestamp'] - pd.Timedelta(hours=8)
    df.sort_values(['sid', 'timestamp'], inplace=True, ignore_index=True)
    return df


def get_dividend_data(only_A=True):
    """现金股利"""
    db = get_db('cninfo')
    collection = db['分红指标']
    # 使用股权登记日作为 asof_date
    # 此指标仅用于计算年度股息之用，不涉及到所谓知晓日期
    pipeline = [
        {
            '$project': {
                '_id': 0,
                '股票代码': 1,
                '分红年度': 1,
                'A股股权登记日': 1,
                '派息比例(人民币)': 1
            }
        }
    ]
    if only_A:
        pipeline.insert(0, MATCH_ONLY_A)
    ds = collection.aggregate(pipeline)
    df = pd.DataFrame.from_records(ds)
    cols = {'股票代码': 'sid', 'A股股权登记日': 'asof_date', '派息比例(人民币)': '每股派息'}
    df.rename(columns=cols, inplace=True)
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
    df['sid'] = df['sid'].map(lambda x: int(x))
    # datetime -> timestamp
    df = _to_timestamp(df)
    return df


# endregion

# region 定期财务报告

# 废弃
# def _fix_sid_ad_ts(df, col='报告年度', ndays=45):
#     """
#     修复截止日期、公告日期。
#     如果`asof_date`为空，则使用`col`的值
#         `timestamp`在`col`的值基础上加`ndays`天"""
#     df['sid'] = df['sid'].map(lambda x: int(x))
#     cond = df.asof_date.isna()
#     df.loc[cond, 'asof_date'] = df.loc[cond, col]
#     df.loc[cond, 'timestamp'] = df.loc[cond, col] + pd.Timedelta(days=ndays)
#     # 由于存在数据不完整的情形，当timestamp为空，在asof_date基础上加ndays
#     cond1 = df.timestamp.isna()
#     df.loc[cond1,
#            'timestamp'] = df.loc[cond1, 'asof_date'] + pd.Timedelta(days=ndays)
#     # 1991-12-31 时段数据需要特别修正
#     cond2 = df.timestamp.map(lambda x: x.is_quarter_end)
#     cond3 = df.asof_date == df.timestamp
#     df.loc[cond2 & cond3,
#            'timestamp'] = df.loc[cond2 & cond3,
#                                  'asof_date'] + pd.Timedelta(days=ndays)


def _periodly_report(only_A, item_name):
    # 一般而言，定期财务报告截止日期与报告年度相同
    # 但不排除数据更正等情形下，报告年度与截止日期不一致
    to_drop = [
        '_id', '股票简称', '机构名称', '合并类型编码', '合并类型', '报表来源编码', '报表来源',
        '备注', '截止日期', '开始日期'
    ]
    db = get_db('cninfo')
    collection = db[item_name]
    pipeline = [
        {
            '$project': {k: 0 for k in to_drop}
        }
    ]
    if only_A:
        pipeline.insert(0, MATCH_ONLY_A)
    ds = collection.aggregate(pipeline)
    df = pd.DataFrame.from_records(ds)
    # 规范列名称
    df.columns = df.columns.map(_normalized_col_name)
    df.rename(columns={
        "股票代码": "sid",
        "报告年度": "asof_date",
        "公告日期": "timestamp"
    },
        inplace=True)
    df['sid'] = df['sid'].map(lambda x: int(x))
    df.sort_values(['sid', 'asof_date'], inplace=True)
    return df


def get_p_balance_data(only_A=True):
    """报告期资产负债表"""
    item_name = '个股报告期资产负债表'
    df = _periodly_report(only_A, item_name)
    return df


def get_p_income_data(only_A=True):
    """报告期利润表"""
    item_name = '个股报告期利润表'
    df = _periodly_report(only_A, item_name)
    return df


def get_p_cash_flow_data(only_A=True):
    """报告期现金流量表"""
    item_name = '个股报告期现金表'
    df = _periodly_report(only_A, item_name)
    return df


def _financial_report_announcement_date(only_A):
    """
    获取财报公告日期，供其他计算类型的表使用

    注：
        季度报告、财务指标根据定期报告计算得来，数据中不含公告日期。
        使用定期报告的公告日期作为`timestamp`
    """
    db = get_db('cninfo')
    collection = db['个股报告期资产负债表']
    pipeline = [
        {
            '$project': {
                '_id': 0,
                '股票代码': 1,
                '公告日期': 1,
                '报告年度': 1,
            }
        }
    ]
    if only_A:
        pipeline.insert(0, MATCH_ONLY_A)
    ds = collection.aggregate(pipeline)
    df = pd.DataFrame.from_records(ds)
    df.sort_values(['股票代码', '报告年度'], inplace=True, ignore_index=True)
    return df


def _get_report(only_A, item_name, to_drop, col='报告年度', keys=['股票代码', '报告年度']):
    """
    获取财务报告数据

    使用报告期资产负债表的公告日期
    """
    if '_id' not in to_drop:
        to_drop.append('_id')

    db = get_db('cninfo')
    collection = db[item_name]
    pipeline = [
        {
            '$project': {k: 0 for k in to_drop}
        }
    ]
    if only_A:
        pipeline.insert(0, MATCH_ONLY_A)
    ds = collection.aggregate(pipeline)
    df = pd.DataFrame.from_records(ds)
    dates = _financial_report_announcement_date(only_A)
    if col != '报告年度':
        # 处理行业排名
        df['报告年度'] = df.pop(col)
    # 合并使用 公告日期
    df = df.join(dates.set_index(keys), on=keys)
    # 规范列名称
    df.columns = df.columns.map(_normalized_col_name)

    df.rename(columns={
        "股票代码": "sid",
        "报告年度": "asof_date",
        "公告日期": "timestamp"
    },
        inplace=True)
    df['sid'] = df['sid'].map(lambda x: int(x))
    df.sort_values(['sid', 'asof_date'], inplace=True)
    return df


# endregion

# region 单季度财务报告


def get_q_income_data(only_A=True):
    """个股单季财务利润表"""
    item_name = '个股单季财务利润表'
    to_drop = ['股票简称', '开始日期', '合并类型编码', '合并类型', '备注']
    df = _get_report(only_A, item_name, to_drop)
    return df


def get_q_cash_flow_data(only_A=True):
    """个股单季现金流量表"""
    item_name = '个股单季现金流量表'
    to_drop = ['股票简称', '开始日期', '合并类型编码', '合并类型', '备注']
    df = _get_report(only_A, item_name, to_drop)
    return df


# endregion

# region TTM


def get_ttm_income_data(only_A=True):
    """个股TTM财务利润表"""
    item_name = '个股TTM财务利润表'
    to_drop = ['股票简称', '开始日期', '合并类型编码', '合并类型', '备注']
    df = _get_report(only_A, item_name, to_drop)
    return df


def get_ttm_cash_flow_data(only_A=True):
    """个股TTM现金流量表"""
    item_name = '个股TTM现金流量表'
    to_drop = ['股票简称', '开始日期', '合并类型编码', '合并类型', '备注']
    df = _get_report(only_A, item_name, to_drop)
    return df


# endregion

# region 财务指标


def get_periodly_financial_indicator_data(only_A=True):
    """个股报告期指标表"""
    item_name = '个股报告期指标表'
    to_drop = [
        '股票简称', '机构名称', '开始日期', '数据来源编码', '数据来源', 'last_refresh_time', '备注'
    ]
    df = _get_report(only_A, item_name, to_drop)
    return df


def get_financial_indicator_ranking_data(only_A=True):
    """
    财务指标行业排名

    级别说明：申银万国二级行业
    """
    item_name = '财务指标行业排名'
    to_drop = ['股票简称', '行业ID', '行业级别', '级别说明', '备注']
    df = _get_report(only_A, item_name, to_drop)
    return df


def get_quarterly_financial_indicator_data(only_A=True):
    """个股单季财务指标"""
    item_name = '个股单季财务指标'
    to_drop = ['股票简称', '开始日期', '合并类型编码', '合并类型', '备注']
    df = _get_report(only_A, item_name, to_drop)
    return df


# endregion

# region 业绩预告


def get_performance_forecaste_data(only_A=True):
    """上市公司业绩预告"""
    item_name = '上市公司业绩预告'
    # 简化写入量，保留`业绩类型`
    to_drop = ['_id', '股票简称', '业绩类型编码', '业绩变化原因', '报告期最新记录标识', '备注']
    db = get_db('cninfo')
    collection = db[item_name]
    pipeline = [
        {
            '$project': {k: 0 for k in to_drop}
        }
    ]
    if only_A:
        pipeline.insert(0, MATCH_ONLY_A)
    ds = collection.aggregate(pipeline)
    df = pd.DataFrame.from_records(ds)

    # 业绩预告反映未来事件

    cond = df['公告日期'].isnull()
    df.loc[cond, '公告日期'] = df.loc[cond, '报告年度'] - pd.Timedelta(days=45)
    # 保留`报告年度`列
    df.rename(columns={
        "股票代码": "sid",
        # "报告年度": "asof_date",
        "公告日期": "timestamp",
    }, inplace=True)
    # 将 asof_date 定义为前一分钟
    df['asof_date'] = df['timestamp'] - pd.Timedelta(minutes=1)
    df['sid'] = df['sid'].map(lambda x: int(x))
    # 深证信原始数据中 股票代码  "002746"
    # 公告日期  2013-10-13 报告年度 2016-09-30 
    # 即做出提前三年的业绩预告，有违常理，需删除
    # 一般而言，业绩预告不会领先报告年度一个季度发布
    cond = df['timestamp'] - df['asof_date'] < pd.Timedelta(days=90)
    df = df.loc[cond, :]
    return df


# endregion

# region 股东股本


def get_shareholding_concentration_data(only_A=True):
    """持股集中度"""
    item_name = '持股集中度'
    df = _get_report(only_A, item_name, [], col='截止日期')
    df.rename(columns={
        "Ａ股户数": "A股户数",
        "Ｂ股户数": "B股户数",
        "Ｈ股户数": "H股户数",
    },
        inplace=True)
    # 更改为逻辑类型
    df['前十大股东'] = df['前十大股东'] == '前十大股东'
    df.sort_values(['sid', 'asof_date'], inplace=True)
    return df


# endregion

# region 投资评级


def get_investment_rating_data(only_A=True):
    """投资评级"""
    item_name = '投资评级'
    to_drop = ['_id', '前一次投资评级', '股票简称', '投资评级',
               '评级变化', '是否首次评级', "目标价格（下限）", "目标价格（上限）"]
    db = get_db('cninfo')
    collection = db[item_name]
    pipeline = [
        {
            '$project': {k: 0 for k in to_drop}
        }
    ]
    if only_A:
        pipeline.insert(0, MATCH_ONLY_A)
    ds = collection.aggregate(pipeline)
    df = pd.DataFrame.from_records(ds)

    df.rename(columns={
        "股票代码": "sid",
        "发布日期": "asof_date",
        "投资评级（经调整）": "投资评级",
    },
        inplace=True)
    df.dropna(subset=['投资评级'], inplace=True)
    df['timestamp'] = df['asof_date']
    df['asof_date'] -= pd.Timedelta(hours=1)
    df['sid'] = df['sid'].map(lambda x: int(x))
    return df


# endregion
