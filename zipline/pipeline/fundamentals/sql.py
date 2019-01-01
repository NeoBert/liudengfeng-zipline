"""

查询本地数据

"""
import warnings

import numpy as np
import pandas as pd
from sqlalchemy import func


from cnswd.sql.base import get_engine, session_scope
from cnswd.sql.szsh import TCTGN, StockDaily, TradingCalendar
from cnswd.sql.szx import (Classification, ClassificationBom,
                           Dividend, FinancialIndicatorRanking,
                           PerformanceForecaste, PeriodlyBalanceSheet,
                           PeriodlyCashFlowStatement,
                           PeriodlyFinancialIndicator, PeriodlyIncomeStatement,
                           QuarterlyFinancialIndicator, Quote,
                           ShareholdingConcentration, StockInfo,
                           TtmCashFlowStatement, TtmIncomeStatement)


NUM_MAPS = {
    1: '一级',
    2: '二级',
    3: '三级',
    4: '四级',
}

# ==========================信息=========================== #


def get_stock_info(only_A=True):
    """股票基础信息"""
    with session_scope('szx') as sess:
        query = sess.query(
            StockInfo.股票代码,
            StockInfo.股票简称,
            StockInfo.上市日期,
            StockInfo.上市地点,
            StockInfo.上市状态,
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
        columns = ['sid', '股票简称', '上市日期', '市场', '状态', '省份', '城市',
                   '证监会一级行业', '证监会二级行业',
                   '申万一级行业', '申万二级行业', '申万三级行业',
                   '注册资本']
        df = pd.DataFrame.from_records(query.all(), columns=columns)
        df['asof_date'] = df['上市日期'] - pd.Timedelta(days=1)
        # 注册资本转换 -> 十分位数
        df['注册资本十分位数'] = pd.qcut(
            np.log(df.注册资本.values), 10, labels=False)
        return df.sort_values('sid')


def get_cn_bom():
    """国证行业分类编码表"""
    with session_scope('szx') as sess:
        query = sess.query(
            ClassificationBom.分类编码,
            ClassificationBom.分类名称,
        ).filter(
            ClassificationBom.平台类别 == '国证行业分类'
        )
        df = pd.DataFrame.from_records(query.all(), columns=['分类编码', '分类名称'])
        return df.sort_values('分类编码')


def _get_cn_industry(only_A, level, bom):
    """国证四级行业分类"""
    assert level in (1, 2, 3, 4), '国证行业只有四级分类'
    u_num = NUM_MAPS[level]
    col_names = ['sid', '国证{}行业'.format(u_num), '国证{}行业编码'.format(u_num)]
    with session_scope('szx') as sess:
        query = sess.query(
            Classification.股票代码,
            Classification.分类名称,
            Classification.分类编码
        ).filter(
            Classification.平台类别 == '国证行业分类'
        )
        if only_A:
            query = query.filter(
                ~Classification.股票代码.startswith('2'),
                ~Classification.股票代码.startswith('9'),
            )
        df = pd.DataFrame.from_records(query.all(), columns=col_names)
        if df.empty:
            msg = '在本地数据库中无法获取国证行业{}分类数据。\n'.format(u_num)
            msg += '这将导致股票分类数据缺失。\n'
            msg += '运行`stock szx-cf`提取网络数据并存储在本地数据库。'
            warnings.warn(msg)
            return pd.DataFrame(columns=col_names)
        # 层级对应的编码位数长度
        digit = level * 2 + 1
        col_name = '国证{}行业编码'.format(u_num)
        df[col_name] = df[col_name].map(lambda x: x[:digit])
        df['国证{}行业'.format(u_num)] = df[col_name].map(
            lambda x: bom.at[x, '分类名称'])
        return df


def get_cn_industry(only_A=True):
    """获取国证四级行业分类"""
    bom = get_cn_bom()
    bom.set_index('分类编码', inplace=True)
    df1 = _get_cn_industry(only_A, 1, bom)
    df2 = _get_cn_industry(only_A, 2, bom)
    df3 = _get_cn_industry(only_A, 3, bom)
    df4 = _get_cn_industry(only_A, 4, bom)
    return df1.join(
        df2.set_index('sid'), how='left', on='sid'
    ).join(
        df3.set_index('sid'), how='left', on='sid'
    ).merge(
        df4.set_index('sid'), how='left', on='sid'
    )


def concept_categories():
    """概念类别映射{代码:名称}"""
    with session_scope('szsh') as sess:
        query = sess.query(
            TCTGN.概念id.distinct(),
            TCTGN.概念简称,
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
    with session_scope('szsh') as sess:
        query = sess.query(
            TCTGN.股票代码,
            TCTGN.概念id,
        )
        if only_A:
            query = query.filter(
                ~TCTGN.股票代码.startswith('2'),
                ~TCTGN.股票代码.startswith('9'),
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


# ==========================动态数据=========================== #

def get_short_name_changes():
    """股票简称变动历史"""
    with session_scope('szsh') as sess:
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
        return df

# TODO:废弃


def get_quote_data(only_A=True):
    """股票交易数据"""
    engine = get_engine('szx')
    table = Quote.__tablename__
    df = pd.read_sql_table(table, engine)
    if only_A:
        df = df[~df.股票代码.str.startswith('2')]
        df = df[~df.股票代码.str.startswith('9')]
    to_drop = ['股票简称', '交易所', '今日开盘价', '最高成交价', '涨跌', '涨跌幅',
               '最低成交价', '收盘价', '成交数量', '成交金额', '昨收盘价', '总笔数']
    df.drop(to_drop, axis=1, inplace=True, errors='ignore')
    df.rename(columns={"股票代码": "sid",
                       "交易日期": "asof_date"},
              inplace=True)
    return df


# TODO:考虑增加其他综合计算指标，如PEG


def get_equity_data(only_A=True):
    """公司股本数据"""
    with session_scope('szx') as sess:
        query = sess.query(
            Quote.股票代码,
            Quote.交易日期,
            Quote.发行总股本,
            Quote.流通股本,
        ).filter(
            Quote.流通股本 > 0
        )
    columns = ['sid', 'asof_date', '发行总股本', '流通股本']
    df = pd.DataFrame.from_records(query.all(), columns=columns)
    if only_A:
        df = df[~df.sid.str.startswith('2')]
        df = df[~df.sid.str.startswith('9')]
    return df


def get_margin_data(only_A=True):
    """融资融券数据"""
    with session_scope('szx') as sess:
        query = sess.query(
            Quote.股票代码,
            Quote.交易日期,
            Quote.本日融资余额,
            Quote.本日融资买入额,
            Quote.本日融资偿还额,
            Quote.本日融券余量,
            Quote.本日融券卖出量,
            Quote.本日融券偿还量,
            Quote.融券余量金额,
            Quote.融资融券余额,
        ).filter(
            Quote.融资融券余额 > 0
        )
    columns = ['sid', 'asof_date', '本日融资余额', '本日融资买入额', '本日融资偿还额',
               '本日融券余量', '本日融券卖出量', '本日融券偿还量',
               '融券余量金额', '融资融券余额'
               ]
    df = pd.DataFrame.from_records(query.all(), columns=columns)
    if only_A:
        df = df[~df.sid.str.startswith('2')]
        df = df[~df.sid.str.startswith('9')]
    return df


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
            Dividend.派息比例人民币 > 0
        )
        if only_A:
            query = query.filter(
                ~Dividend.股票代码.startswith('2'),
                ~Dividend.股票代码.startswith('9'),
            )
        df = pd.DataFrame.from_records(query.all())
        df.columns = ['sid', '分红年度', 'asof_date', '每股人民币派息']
        df['每股人民币派息'] = df['每股人民币派息'] / 10.0
        df.sort_values(['sid', 'asof_date'], inplace=True)
        return df

# ==========================财务报告=========================== #


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
    df.sort_values(['sid', 'asof_date'], inplace=True)
    return df

# ==========================TTM=========================== #


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

# ==========================财务指标=========================== #


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

# ==========================业绩预告=========================== #


def get_performance_forecaste_data(only_A=True):
    """上市公司业绩预告"""
    to_drop = ['股票简称', '业绩类型编码', '业绩类型']
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
    df.sort_values(['sid', 'asof_date'], inplace=True)
    return df

# ==========================股东股本=========================== #


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
        df.sort_values(['sid', 'asof_date'], inplace=True)
        return df
