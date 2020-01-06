"""

查询本地数据

"""
import re
import warnings

import numpy as np
import pandas as pd

from cnswd.query_utils import Ops, query, query_stmt
from cnswd.reader import (asr_data, classify_bom, classify_tree, margin,
                          stock_list, ths_gn)
from cnswd.utils import data_root

from .constants import SW_SECTOR_MAPS

NUM_MAPS = {
    1: '一级',
    2: '二级',
    3: '三级',
    4: '四级',
}

TO_REPL_PAT = re.compile(r'[、：（）()-]')  # 不符合列名称命名规则，替换为`_`
TO_DORP_PAT = re.compile(r'^_{1,}|^[1-9]|^[一二三四五六七八九][、]?|[^_]_{2,}\w|_{1,}$')

# region 辅助函数


def _normalized_col_name(x):
    """规范列财务报告项目在`pipeline`中的列名称
    注：
        去除以大写数字开头的部分
    更改示例：
        四2_其他原因对现金的影响      ->  其他原因对现金的影响
        五_现金及现金等价物净增加额    ->  现金及现金等价物净增加额
    不变示例
        现金及现金等价物净增加额2           ->  现金及现金等价物净增加额2
        加_公允价值变动净收益               ->  加_公允价值变动净收益
        其中_对联营企业和合营企业的投资收益  ->  其中_对联营企业和合营企业的投资收益
    """
    # 首先替代
    x = re.sub(TO_REPL_PAT, '_', x)
    # 然后除去前缀及尾缀`_`
    x = re.sub(TO_DORP_PAT, '', x)
    if x.startswith('_'):
        x = x[1:]
    if x.endswith('_'):
        x = x[:-1]
    return x


def _select_only_a(df, only_A, code_col='股票代码'):
    """仅含A股数据"""
    if only_A:
        cond1 = df[code_col].str.startswith('2')
        cond2 = df[code_col].str.startswith('9')
        df = df.loc[~(cond1 | cond2), :]
    return df


# endregion

# region 信息


def get_stock_info(only_A=True):
    """股票基础信息"""
    df = stock_list()
    # 舍弃原行业分类信息
    to_drops = [
        '机构名称', '摘牌日期', 'ISIN代码', '英文名称', '英文简称', '经营范围', '公司简介', '公司传真',
        '公司电子邮件地址', '公司电话', '公司网站', '办公地址', '总经理', '法定代表人', '注册地址', '董秘传真',
        '董秘电话', '董事会秘书', '董事长', '董秘邮箱', '证券事务代表', '邮编'
    ]
    df.drop(columns=to_drops, inplace=True, errors='ignore')
    # 剔除没有上市日期的股票
    cond = df['上市日期'].isnull()
    df = df[~cond]
    df = _select_only_a(df, only_A, '股票代码')
    if 'index' in df.columns:
        df.drop(columns='index', inplace=True)
    df['asof_date'] = df['上市日期'] - pd.Timedelta(days=1)
    df.drop(columns=['上市日期'], inplace=True)
    # 注册资本转换 -> 十分位数
    df['注册资本十分位数'] = pd.qcut(np.log(df['注册资本'].values), 10, labels=False)
    df = df.sort_values('股票代码')
    df.rename(columns={'股票代码': 'sid'}, inplace=True)
    df['sid'] = df['sid'].map(lambda x: int(x))
    return df


def get_cn_bom():
    """国证行业分类编码表"""
    df = classify_bom()
    cond = df['分类编码'].str.startswith('Z')
    df = df[cond][['分类编码', '分类名称']]
    return df.sort_values('分类编码')


def _get_cn_industry(only_A, level, bom):
    """国证四级行业分类"""
    assert level in (1, 2, 3, 4), '国证行业只有四级分类'
    u_num = NUM_MAPS[level]
    col_names = ['sid', '国证{}行业'.format(u_num), '国证{}行业编码'.format(u_num)]
    df = classify_tree()
    cond = df['平台类别'] == '137003'
    df = df[cond]
    df = _select_only_a(df, only_A, '证券代码')
    if df.empty:
        msg = '在本地数据库中无法获取国证行业{}分类数据。\n'.format(u_num)
        msg += '这将导致股票分类数据缺失。\n'
        msg += '运行`stock db-classify`提取网络数据并存储在本地数据库。'
        warnings.warn(msg)
        return pd.DataFrame(columns=col_names)
    # 层级对应的编码位数长度
    digit = level * 2 + 1
    col_name = '国证{}行业编码'.format(u_num)
    df[col_name] = df['分类编码'].map(lambda x: x[:digit])
    df['国证{}行业'.format(u_num)] = df[col_name].map(lambda x: bom.at[x, '分类名称'])
    return df[['证券代码', col_name, '国证{}行业'.format(u_num)]]


def get_cn_industry(only_A=True):
    """获取国证四级行业分类"""
    bom = get_cn_bom()
    bom.set_index('分类编码', inplace=True)
    df1 = _get_cn_industry(only_A, 1, bom)
    df2 = _get_cn_industry(only_A, 2, bom)
    df3 = _get_cn_industry(only_A, 3, bom)
    df4 = _get_cn_industry(only_A, 4, bom)
    df = df1.join(df2.set_index('证券代码'),
                  on='证券代码').join(df3.set_index('证券代码'),
                                  on='证券代码').join(df4.set_index('证券代码'),
                                                  on='证券代码')
    df.rename(columns={'证券代码': 'sid'}, inplace=True)
    df['sid'] = df['sid'].map(lambda x: int(x))
    return df


def get_sw_industry(only_A=True):
    """获取申万一级行业分类编码"""
    code_maps = {v: k for k, v in SW_SECTOR_MAPS.items()}
    df = classify_tree()
    cond = df['平台类别'] == '137004'
    df = df[cond]
    df = _select_only_a(df, only_A, '证券代码')
    df['sw_sector'] = df['分类编码'].map(lambda x: code_maps[x[:3]]).astype(
        'int64')
    df = df[['证券代码', 'sw_sector']]
    df.rename(columns={'证券代码': 'sid'}, inplace=True)
    df['sid'] = df['sid'].map(lambda x: int(x))
    return df


def concept_categories():
    """概念类别映射{代码:名称}"""
    df = ths_gn()
    df = df.drop_duplicates(subset='概念编码')[['概念编码', '概念']]
    try:
        df.columns = ['code', 'name']
    except ValueError:
        raise NotImplementedError('本地数据库中"股票概念数据"为空，需要刷新')
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
    id_maps, _ = field_code_concept_maps()
    df = ths_gn()[['股票代码', '概念编码']]
    # 暂无成份股数据
    df = df[~df['股票代码'].str.startswith('暂无')]
    df = _select_only_a(df, only_A, '股票代码')
    df.columns = ['sid', '概念']
    out = pd.pivot_table(df,
                         values='概念',
                         index='sid',
                         columns='概念',
                         aggfunc=np.count_nonzero,
                         fill_value=0)
    out.rename(columns=id_maps, inplace=True)
    df = out.astype('bool').reset_index()
    df.rename(columns={'股票代码': 'sid'}, inplace=True)
    df['sid'] = df['sid'].map(lambda x: int(x))
    return df


# endregion

# region 动态数据


def get_short_name_changes(only_A=True):
    """股票简称变动历史"""
    p = data_root('wy_stock')
    fs = p.glob('*.h5')
    code_pattern = re.compile(r'\d{6}')

    def f(fp):
        code = re.findall(code_pattern, str(fp))[0]
        # 原始数据中，股票代码中有前缀`'`
        code = f"'{code}"
        if only_A and code[0] in ('2', '9'):
            return pd.DataFrame()
        stmt = query_stmt(*[('股票代码', Ops.eq, code)])
        try:
            df = query(fp, stmt)[['名称', '股票代码', '日期']]
            df.columns = ['股票简称', 'sid', 'asof_date']
            df['sid'] = df['sid'].map(lambda x: x[1:])
            df['sid'] = df['sid'].map(lambda x: int(x))
            df.sort_values(['sid', 'asof_date'], inplace=True)
            df.drop_duplicates(subset=['股票简称', 'sid'], inplace=True)
        except KeyError:
            # 新股无数据
            df = pd.DataFrame()
        return df

    res = map(f, fs)
    df = pd.concat(res)
    return df


def get_margin_data(only_A=True):
    """融资融券数据"""
    df = margin(None, None, None)
    df.rename(columns={'交易日期': 'asof_date'}, inplace=True)
    df = _select_only_a(df, only_A, '股票代码')
    return df


def get_dividend_data(only_A=True):
    """现金股利"""
    cols = ['股票代码', '分红年度', '董事会预案公告日期', '派息比例(人民币)']
    df = asr_data('5', None, None, None)[cols]
    cond = df['派息比例(人民币)'] > 0
    df = df[cond]
    df = df[~df['董事会预案公告日期'].isnull()]
    df.columns = ['股票代码', '分红年度', 'asof_date', '每股人民币派息']
    df['每股人民币派息'] = df['每股人民币派息'] / 10.0
    df.sort_values(['股票代码', 'asof_date'], inplace=True)
    return df


# endregion

# region 定期财务报告


def _fix_sid_ad_ts(df, col='报告年度', ndays=45):
    """
    修复截止日期、公告日期。
    如果`asof_date`为空，则使用`col`的值
        `timestamp`在`col`的值基础上加`ndays`天"""
    df['sid'] = df['sid'].map(lambda x: int(x))
    cond = df.asof_date.isna()
    df.loc[cond, 'asof_date'] = df.loc[cond, col]
    df.loc[cond, 'timestamp'] = df.loc[cond, col] + pd.Timedelta(days=ndays)
    # 由于存在数据不完整的情形，当timestamp为空，在asof_date基础上加ndays
    cond1 = df.timestamp.isna()
    df.loc[cond1,
           'timestamp'] = df.loc[cond1, 'asof_date'] + pd.Timedelta(days=ndays)
    # 1991-12-31 时段数据需要特别修正
    cond2 = df.timestamp.map(lambda x: x.is_quarter_end)
    cond3 = df.asof_date == df.timestamp
    df.loc[cond2 & cond3,
           'timestamp'] = df.loc[cond2 & cond3,
                                 'asof_date'] + pd.Timedelta(days=ndays)


def _periodly_report(only_A, level):
    # 一般而言，定期财务报告截止日期与报告年度相同
    # 但不排除数据更正等情形下，报告年度与截止日期不一致
    to_drop = [
        '股票简称', '机构名称', '合并类型编码', '合并类型', '报表来源编码', '报表来源',
        'last_refresh_time', '备注'
    ]
    df = asr_data(level, None, None, None)
    df = _select_only_a(df, only_A, '股票代码')
    df.drop(to_drop, axis=1, inplace=True, errors='ignore')
    df.rename(columns={
        "股票代码": "sid",
        "截止日期": "asof_date",
        "公告日期": "timestamp"
    },
              inplace=True)
    # 修复截止日期
    _fix_sid_ad_ts(df)
    # 规范列名称
    df.columns = df.columns.map(_normalized_col_name)
    df.sort_values(['sid', 'asof_date'], inplace=True)
    return df


def get_p_balance_data(only_A=True):
    """报告期资产负债表"""
    level = '7.1.1'
    df = _periodly_report(only_A, level)
    return df


def get_p_income_data(only_A=True):
    """报告期利润表"""
    level = '7.1.2'
    df = _periodly_report(only_A, level)
    return df


def get_p_cash_flow_data(only_A=True):
    """报告期现金流量表"""
    level = '7.1.3'
    df = _periodly_report(only_A, level)
    return df


def _financial_report_announcement_date():
    """
    获取财报公告日期，供其他计算类型的表使用(使用资产负债表公告日期)

    注：
        季度报告、财务指标是依据定期报告计算得来，并没有实际的公告日期。
        以其利润表定期报告的公告日期作为`asof_date`
    """
    col_names = ['股票代码', '公告日期', '截止日期']
    df = asr_data('7.1.1', None, None, None)[col_names]
    return df


def _get_report(only_A, level, to_drop, col='截止日期'):
    """
    获取财务报告数据

    使用报告期资产负债表的公告日期
    """
    df = asr_data(level, None, None, None)
    df = _select_only_a(df, only_A, '股票代码')
    df.drop(to_drop, axis=1, inplace=True, errors='ignore')
    asof_dates = _financial_report_announcement_date()
    keys = ['股票代码', '截止日期']
    if col != '截止日期':
        # 处理行业排名
        df['报告年度'] = df[col]
        # 原始数据列名称更改为'截止日期'
        df.rename(columns={col: '截止日期'}, inplace=True)
    # 合并使用 公告日期
    df = df.join(asof_dates.set_index(keys), on=keys)
    df.rename(columns={
        "股票代码": "sid",
        "截止日期": "asof_date",
        "公告日期": "timestamp"
    },
              inplace=True)
    # 修复截止日期
    _fix_sid_ad_ts(df)
    # 规范列名称
    df.columns = df.columns.map(_normalized_col_name)
    df.sort_values(['sid', 'asof_date'], inplace=True)
    return df


# endregion

# region 季度报告


def get_q_income_data(only_A=True):
    """季度利润表"""
    level = '7.3.1'
    to_drop = ['股票简称', '开始日期', '合并类型编码', '合并类型', 'last_refresh_time', '备注']
    df = _get_report(only_A, level, to_drop)
    return df


def get_q_cash_flow_data(only_A=True):
    """季度现金流量表"""
    level = '7.3.2'
    to_drop = ['股票简称', '开始日期', '合并类型编码', '合并类型', 'last_refresh_time', '备注']
    df = _get_report(only_A, level, to_drop)
    return df


# endregion

# region TTM


def get_ttm_income_data(only_A=True):
    """TTM财务利润表"""
    level = '7.4.1'
    to_drop = ['股票简称', '开始日期', '合并类型编码', '合并类型', 'last_refresh_time', '备注']
    df = _get_report(only_A, level, to_drop)
    return df


def get_ttm_cash_flow_data(only_A=True):
    """TTM现金流量表"""
    level = '7.4.2'
    to_drop = ['股票简称', '开始日期', '合并类型编码', '合并类型', 'last_refresh_time', '备注']
    df = _get_report(only_A, level, to_drop)
    return df


# endregion

# region 财务指标


def get_periodly_financial_indicator_data(only_A=True):
    """报告期指标表"""
    level = '7.2.1'
    to_drop = [
        '股票简称', '机构名称', '开始日期', '数据来源编码', '数据来源', 'last_refresh_time', '备注'
    ]
    df = _get_report(only_A, level, to_drop)
    return df


def get_financial_indicator_ranking_data(only_A=True):
    """
    财务指标行业排名

    申银万国二级行业
    """
    level = '7.2.2'
    to_drop = ['股票简称', '行业ID', '行业级别', '级别说明', 'last_refresh_time', '备注']
    df = _get_report(only_A, level, to_drop, col='报告期')
    return df


def get_quarterly_financial_indicator_data(only_A=True):
    """单季财务指标"""
    level = '7.3.3'
    to_drop = ['股票简称', '开始日期', '合并类型编码', '合并类型', 'last_refresh_time', '备注']
    df = _get_report(only_A, level, to_drop)
    return df


# endregion

# region 业绩预告

# TODO:需要结合公告日期与报告年度进行处理，否则只会提取此前的数据，不能真实反映当前期间的预告
def get_performance_forecaste_data(only_A=True):
    """上市公司业绩预告"""
    level = '4.1'
    # 简化写入量，保留`业绩类型`
    to_drop = ['股票简称', '业绩类型编码', '业绩预告内容', '业绩变化原因', '报告期最新记录标识', '备注']
    df = asr_data(level, None, None, None)
    df = _select_only_a(df, only_A, '股票代码')
    for col in to_drop:
        if col in df.columns:
            del df[col]
    # 业绩预告只提供`asof_date`
    df.rename(columns={
        "股票代码": "sid",
        "公告日期": "asof_date",
    },
              inplace=True)
    return df


# endregion

# region 股东股本


def get_shareholding_concentration_data(only_A=True):
    """持股集中度"""
    level = '2.5'
    df = _get_report(only_A, level, [])
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
    level = '3'
    to_drop = ['序号', '股票简称', '投资评级', '备注']
    df = asr_data(level, None, None, None)
    df = _select_only_a(df, only_A, '股票代码')
    for col in to_drop:
        if col in df.columns:
            del df[col]
    df.rename(columns={
        "股票代码": "sid",
        "发布日期": "asof_date",
        "投资评级（经调整）": "投资评级",
        "目标价格（下限）": "价格下限",
        "目标价格（上限）": "价格上限",
    },
              inplace=True)
    df.dropna(subset=['投资评级'], inplace=True)
    return df


# endregion
