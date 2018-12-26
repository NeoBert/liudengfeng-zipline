"""

预处理数据

特别说明
    1. 关于国证行业分类，理论上可以查询到任一日股票行业，简化为静态信息，
       即截止到当日股票所处的行业。如股票历史上存在转行，可能会影响回测。
"""
import warnings
import numpy as np
import pandas as pd
from sqlalchemy import func

from cnswd.sql.base import session_scope, get_engine
from cnswd.sql.cn import THSGN, StockDaily, TradingCalendar
from cnswd.sql.szx import (StockInfo,
                           Classification,
                           Dividend,
                           Quote,
                           ShareholdingConcentration,
                           TtmIncomeStatement,
                           TtmCashFlowStatement,
                           PeriodlyBalanceSheet,
                           PeriodlyIncomeStatement,
                           PeriodlyCashFlowStatement,
                           QuarterlyFinancialIndicator,
                           PeriodlyFinancialIndicator,
                           PerformanceForecaste,
                           FinancialIndicatorRanking,
                           )

from .constants import SECTOR_NAMES, SUPER_SECTOR_NAMES
from ..common import AD_FIELD_NAME, SID_FIELD_NAME, TS_FIELD_NAME

NUM_MAPS = {
    1: '一级',
    2: '二级',
    3: '三级',
    4: '四级',
}


def _to_dt(s, target_tz):
    """输入序列转换为DatetimeIndex(utc)"""
    ix = pd.DatetimeIndex(s)
    if ix.tz:
        return ix.tz_convert(target_tz)
    else:
        return ix.tz_localize(target_tz)


# >>>>>>>>>>>>>>>>>>>辅助函数<<<<<<<<<<<<<<<<<<<<


def _normalize_ad_ts_sid(df, ndays=0, nhours=8, target_tz='utc'):
    """通用转换
    股票代码 -> sid(int64)
    date(date) -> asof_date(timestamp)
    date(date) + ndays -> timestamp(timestamp)
    确保asof_date <= timestamp
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
            # 确保 df[AD_FIELD_NAME] <= df[TS_FIELD_NAME]
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


# >>>>>>>>>>>>>>>>>>>静态信息<<<<<<<<<<<<<<<<<<<<


def stock_info(only_A=True):
    """股票基础信息"""
    with session_scope('szx') as sess:
        query = sess.query(
            StockInfo.股票代码,
            StockInfo.上市日期,
            StockInfo.上市地点,
            StockInfo.省份,
            StockInfo.城市,
            StockInfo.证监会一级行业名称,
            StockInfo.证监会二级行业名称,
            StockInfo.申万行业一级名称,
            StockInfo.申万行业二级名称,
            StockInfo.申万行业三级名称,
            StockInfo.注册资本,
        ).filter(
            # 剔除没有上市日期的代码
            StockInfo.上市日期.isnot(None)
        )
        if only_A:
            query = query.filter(
                ~StockInfo.股票代码.startswith('2'),
                ~StockInfo.股票代码.startswith('9'),
            )
        df = pd.DataFrame.from_records(query.all())
        df.columns = ['sid', 'asof_date', '市场', '省份', '城市',
                      '证监会一级行业', '证监会二级行业',
                      '申万一级行业', '申万二级行业', '申万三级行业',
                      'capital']
        # 注册资本转换 -> 十分位数
        df['注册资本十分位数'] = pd.qcut(
            np.log(df.capital.values), 10, labels=False)
        del df['capital']
        return df.sort_values('sid')


def _get_cn_industry(only_A=True, level=4, keep=True):
    """国证四级行业分类"""
    assert level in (1, 2, 3, 4), '国证行业只有四级分类'
    u_num = NUM_MAPS[level]
    col_names = ['sid', '国证{}行业'.format(u_num), '国证{}行业代码'.format(u_num)]
    with session_scope('szx') as sess:
        query = sess.query(
            Classification.股票代码,
            Classification.分类名称,
            Classification.分类编码
        ).filter(
            Classification.平台类别 == '137003'
        )
        if only_A:
            query = query.filter(
                ~Classification.股票代码.startswith('2'),
                ~Classification.股票代码.startswith('9'),
            )
        df = pd.DataFrame.from_records(query.all())
        if df.empty:
            msg = '在本地数据库中无法获取国证行业{}分类数据。\n'.format(u_num)
            msg += '这将导致股票分类数据缺失。\n'
            msg += '请在命令行运行`stock szx-cf`提取网络数据并存储在本地数据库。'
            warnings.warn(msg)
            return pd.DataFrame(columns=col_names)
        df.columns = col_names
        # 层级对应的编码位数长度
        digit = level * 2 + 1
        col_name = '国证{}行业代码'.format(u_num)
        df = df[df[col_name].str.len() == digit]
        df.sort_values('sid', inplace=True)
        if not keep:
            del df['国证{}行业代码'.format(u_num)]
        return df


def get_cn_industry(only_A=True):
    """获取国证四级行业分类"""
    df1 = _get_cn_industry(only_A, 1, False)
    df2 = _get_cn_industry(only_A, 2, False)
    df3 = _get_cn_industry(only_A, 3, False)
    df4 = _get_cn_industry(only_A, 4, False)
    return df1.join(
        df2.set_index('sid'), how='left', on='sid'
    ).join(
        df3.set_index('sid'), how='left', on='sid'
    ).merge(
        df4.set_index('sid'), how='left', on='sid'
    )


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


def sector_info():
    """
    部门及超级部门分类
    """
    level = 4
    u_num = NUM_MAPS[level]
    col_name = '国证{}行业代码'.format(u_num)
    df = _get_cn_industry(level=level)
    # 在此先映射部门及超级部门代码
    # 在转换为类别前，处理部门、超级部门编码
    # 为保持一致性，使用原始三位数编码
    df['sector_code'] = df[col_name].map(sector_code_map)
    df['super_sector_code'] = df.sector_code.map(supper_sector_code_map)
    res = pd.DataFrame(
        {
            'sid': df.sid.values,
            '部门编码': df.sector_code.values,
            '超级部门编码': df.super_sector_code.values,
        }
    )
    return res.sort_values('sid')


def concept_categories():
    """同花顺概念类别映射{代码:名称}"""
    with session_scope('cn') as sess:
        query = sess.query(
            THSGN.概念编码.distinct(),
            THSGN.概念,
        )
        df = pd.DataFrame.from_records(query.all())
        df.columns = ['code', 'name']
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
    id_maps = {v: 'A{}'.format(str(k+1).zfill(3)) for k, v in zip(no, key)}
    name_maps = {v: vs[k] for (k, v) in id_maps.items()}
    return id_maps, name_maps


def concept_info(only_A=True):
    """股票概念编码信息(同花顺概念)
    
    Keyword Arguments:
        only_A {bool} -- 只包含A股代码 (default: {True})
    
    Returns:
        pd.DataFrame -- 股票概念编码信息表

    Example:
    >>> concept_info().head(3)
    sid   A001   A002   A003   A004   A005  ...   A205
      1  False  False  False  False  False  ...  False
      2  False  False  False  False  False  ...  False
      4  False  False  False   True  False  ...  False
    """
    id_maps, _ = field_code_concept_maps()
    with session_scope('cn') as sess:
        query = sess.query(
            THSGN.股票代码,
            THSGN.概念编码,
        )
        if only_A:
            query = query.filter(
                ~THSGN.股票代码.startswith('2'),
                ~THSGN.股票代码.startswith('9'),
            )
        df = pd.DataFrame.from_records(query.all())
        df.columns = ['sid', '概念']
        out = pd.pivot_table(df,
                             values='概念',
                             index='sid',
                             columns='概念',
                             aggfunc=np.count_nonzero,
                             fill_value=0)
        out.rename(columns=id_maps, inplace=True)
        return out.astype('bool').reset_index()


def get_static_info_table():
    """股票静态信息合并表"""
    stocks = stock_info()
    cn_industry = get_cn_industry()
    sector = sector_info()
    concept = concept_info()
    df = stocks.join(
        cn_industry.set_index('sid'), on='sid'
    ).join(
        sector.set_index('sid'), on='sid'
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
    return _normalize_ad_ts_sid(df), maps


# >>>>>>>>>>>>>>>>>>>动态信息<<<<<<<<<<<<<<<<<<<<


def get_short_name_changes():
    """股票简称变动历史"""
    with session_scope('cn') as sess:
        query = sess.query(
            StockDaily.股票代码,
            StockDaily.日期,
            StockDaily.名称
        ).group_by(
            StockDaily.股票代码,
            StockDaily.名称
        )
        df = pd.DataFrame.from_records(query.all())
        df.columns = ['sid', 'asof_date', '股票简称']
        return _normalize_ad_ts_sid(df)


def get_dividend_data(only_A=True):
    """现金股利"""
    with session_scope('szx') as sess:
        query = sess.query(
            Dividend.股票代码,
            Dividend.分红年度,
            Dividend.董事会预案公告日期,
            Dividend.派息比例人民币,
        ).filter(
            # 数据可能为空，排除
            Dividend.董事会预案公告日期.isnot(None),
        )
        if only_A:
            query = query.filter(
                ~Dividend.股票代码.startswith('2'),
                ~Dividend.股票代码.startswith('9'),
            )
        df = pd.DataFrame.from_records(query.all())
        df.columns = ['sid', '分红年度', 'asof_date', '每股人民币派息']
        df['每股人民币派息'] = df['每股人民币派息'] / 10.0
        df = _normalize_ad_ts_sid(df)
        df.sort_values(['sid', 'asof_date'], inplace=True)
        return df


def get_margin_data():
    """融资融券数据"""
    col_names = [
        'sid', 'asof_date',
        '本日融券余量', '本日融券偿还量', '本日融券卖出量',
        '本日融资买入额', '本日融资余额', '本日融资偿还额',
        '融券余量金额', '融资融券余额'
    ]
    with session_scope('szx') as sess:
        query = sess.query(
            Quote.股票代码,
            Quote.交易日期,
            Quote.本日融券余量,
            Quote.本日融券偿还量,
            Quote.本日融券卖出量,
            Quote.本日融资买入额,
            Quote.本日融资余额,
            Quote.本日融资偿还额,
            Quote.融券余量金额,
            Quote.融资融券余额,
        ).filter(
            # 非空才是融资融券标的
            # 从此业务开始之日起
            Quote.融资融券余额.isnot(None)
        )
        df = pd.DataFrame.from_records(query.all())
        df.columns = col_names
        df = _normalize_ad_ts_sid(df)
        df.sort_values(['sid', 'asof_date'], inplace=True)
        return df


# >>>>>>>>>>>>>>>>>>>财务报告<<<<<<<<<<<<<<<<<<<<


def _fill_asof_date(df, col='报告年度', ndays=45):
    """
    如果截止日期为空，则在`col`的值基础上加`ndays`天

    如财务报告以公告日期为截止日期，当公告日期为空时，默认报告年度后45天为公告日期。
    """
    cond = df.asof_date.isna()
    df.loc[cond, 'asof_date'] = df.loc[cond, col] + pd.Timedelta(days=ndays)


def _periodly_report(only_A, table):
    to_drop = ['股票简称', '机构名称', '截止日期', '合并类型编码',
               '合并类型', '报表来源编码', '报表来源']
    engine = get_engine('szx')
    df = pd.read_sql_table(table, engine)
    if only_A:
        df = df[~df.股票代码.str.startswith('2')]
        df = df[~df.股票代码.str.startswith('9')]
    df.drop(to_drop, axis=1, inplace=True, errors='ignore')
    df.rename(columns={"股票代码": "sid",
                       "公告日期": "asof_date"},
              inplace=True)
    # 修复截止日期
    _fill_asof_date(df)
    df = _normalize_ad_ts_sid(df)
    df.sort_values(['sid', 'asof_date'], inplace=True)
    return df


def get_p_balance_data(only_A=True):
    """报告期资产负债表"""
    table = PeriodlyBalanceSheet.__tablename__
    df = _periodly_report(only_A, table)
    return df


def get_p_cash_flow_data(only_A=True):
    """报告期现金流量表"""
    table = PeriodlyCashFlowStatement.__tablename__
    df = _periodly_report(only_A, table)
    return df


def get_p_income_data(only_A=True):
    """报告期利润表"""
    table = PeriodlyIncomeStatement.__tablename__
    df = _periodly_report(only_A, table)
    return df


def _financial_report_announcement_date():
    """财务报告公告日期"""
    col_names = ['股票代码', '公告日期', '报告年度']
    with session_scope('szx') as sess:
        query = sess.query(
            PeriodlyIncomeStatement.股票代码,
            PeriodlyIncomeStatement.公告日期,
            PeriodlyIncomeStatement.报告年度
        )
        df = pd.DataFrame.from_records(query.all())
        df.columns = col_names
        return df


def _get_report(only_A, table, columns=None, col='报告年度'):
    """
    获取财务报告数据
    
    使用资产负债表的公告日期作为截止日期
    """
    engine = get_engine('szx')
    df = pd.read_sql_table(table, engine, columns=columns)
    if only_A:
        df = df[~df.股票代码.str.startswith('2')]
        df = df[~df.股票代码.str.startswith('9')]
    # df.drop(to_drop, axis=1, inplace=True, errors='ignore')
    asof_dates = _financial_report_announcement_date()
    keys = ['股票代码', '报告年度']
    # 原始数据列名称更改为'报告年度'
    df.rename(columns={col: '报告年度'}, inplace=True)    
    df = df.join(
        asof_dates.set_index(keys), on=keys
    )
    df.rename(columns={"股票代码": "sid",
                        "公告日期": "asof_date"},
                inplace=True)
    # 修复截止日期
    _fill_asof_date(df, '报告年度')
    df = _normalize_ad_ts_sid(df)
    df.sort_values(['sid', 'asof_date'], inplace=True)
    return df


def get_ttm_cash_flow_data(only_A=True):
    """TTM现金流量表"""
    table = TtmCashFlowStatement.__tablename__
    to_drop = ['股票简称', '开始日期', '截止日期', '合并类型编码', '合并类型']
    columns = []
    for c in TtmCashFlowStatement.__table__.columns:
        if c.name not in to_drop:
            columns.append(c.name)
    df = _get_report(only_A, table, columns)
    return df


def get_ttm_income_data(only_A=True):
    """TTM财务利润表"""
    table = TtmIncomeStatement.__tablename__
    to_drop = ['股票简称', '开始日期', '截止日期', '合并类型编码', '合并类型']
    columns = []
    for c in TtmIncomeStatement.__table__.columns:
        if c.name not in to_drop:
            columns.append(c.name)
    df = _get_report(only_A, table, columns)
    return df


# >>>>>>>>>>>>>>>>>>>财务指标<<<<<<<<<<<<<<<<<<<<


def get_periodly_financial_indicator_data(only_A=True):
    """报告期指标表"""
    table = PeriodlyFinancialIndicator.__tablename__
    to_drop = ['股票简称', '机构名称', '开始日期', '截止日期', '数据来源编码', '数据来源']
    columns = []
    for c in PeriodlyFinancialIndicator.__table__.columns:
        if c.name not in to_drop:
            columns.append(c.name)
    df = _get_report(only_A, table, columns)
    return df


def get_quarterly_financial_indicator_data(only_A=True):
    """单季财务指标"""
    table = QuarterlyFinancialIndicator.__tablename__
    to_drop = ['股票简称', '开始日期', '截止日期', '合并类型编码', '合并类型']
    columns = []
    for c in QuarterlyFinancialIndicator.__table__.columns:
        if c.name not in to_drop:
            columns.append(c.name)
    df = _get_report(only_A, table, columns)
    return df


def get_financial_indicator_ranking_data(only_A=True):
    """
    财务指标行业排名

    申银万国二级行业
    """
    table = FinancialIndicatorRanking.__tablename__
    to_drop = ['股票简称', '行业ID', '行业级别', '级别说明']
    columns = []
    for c in FinancialIndicatorRanking.__table__.columns:
        if c.name not in to_drop:
            columns.append(c.name)
    df = _get_report(only_A, table, columns, col='报告期')
    return df


# >>>>>>>>>>>>>>>>>>>业绩预测<<<<<<<<<<<<<<<<<<<<


def get_performance_forecaste_data(only_A=True):
    """上市公司业绩预告"""
    to_drop = ['股票简称','业绩类型编码','业绩类型']
    columns = []
    for c in PerformanceForecaste.__table__.columns:
        if c.name not in to_drop:
            columns.append(c.name)
    table = PerformanceForecaste.__tablename__
    engine = get_engine('szx')
    df = pd.read_sql_table(table, engine, columns=columns)
    if only_A:
        df = df[~df.股票代码.str.startswith('2')]
        df = df[~df.股票代码.str.startswith('9')]
    df.rename(columns={"股票代码": "sid",
                        "公告日期": "asof_date"},
                inplace=True)
    # 修复截止日期
    _fill_asof_date(df, '报告年度')
    df = _normalize_ad_ts_sid(df)
    df.sort_values(['sid', 'asof_date'], inplace=True)
    return df


# >>>>>>>>>>>>>>>>>>>股东股本<<<<<<<<<<<<<<<<<<<<


def get_shareholding_concentration_data(only_A=True):
    """持股集中度"""
    col_names = ['sid', 'asof_date', '股东总户数', 'A股户数', '户均持股',
                 '户均持股比例', '股东持股数量', '股东持股比例', '股东持股比例比上报告期增减',
                 '前十大股东']
    with session_scope('szx') as sess:
        query = sess.query(
            ShareholdingConcentration.股票代码,
            ShareholdingConcentration.截止日期,
            ShareholdingConcentration.股东总户数,
            ShareholdingConcentration.A股户数,
            ShareholdingConcentration.户均持股,
            ShareholdingConcentration.户均持股比例,
            ShareholdingConcentration.股东持股数量,
            ShareholdingConcentration.股东持股比例,
            ShareholdingConcentration.股东持股比例比上报告期增减,
            ShareholdingConcentration.前十大股东,
        )
        if only_A:
            query = query.filter(
                ~ShareholdingConcentration.股票代码.startswith('2'),
                ~ShareholdingConcentration.股票代码.startswith('9'),
            )
        df = pd.DataFrame.from_records(query.all())
        df.columns = col_names
        df['asof_date'] = df['asof_date'] + pd.Timedelta(days=45)
        # 更改为逻辑类型
        df['前十大股东'] = df['前十大股东'] == '前十大股东'
        df = _normalize_ad_ts_sid(df)
        df.sort_values(['sid', 'asof_date'], inplace=True)
        return df

