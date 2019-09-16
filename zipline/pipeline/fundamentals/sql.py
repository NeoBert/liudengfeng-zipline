"""

查询本地数据

"""
import re
import warnings

import numpy as np
import pandas as pd
from sqlalchemy import func

from cnswd.sql.base import get_engine, session_scope
from cnswd.sql.szsh import StockDaily, TradingCalendar, THSGN
from cnswd.sql.data_browse import (Classification, ClassificationBom,
                                   CompanyShareChange, Dividend,
                                   FinancialIndicatorRanking, InvestmentRating,
                                   PerformanceForecaste, PeriodlyBalanceSheet,
                                   PeriodlyCashFlowStatement,
                                   PeriodlyFinancialIndicator, PeriodlyIncomeStatement,
                                   QuarterlyCashFlowStatement,
                                   QuarterlyFinancialIndicator,
                                   QuarterlyIncomeStatement, Quote,
                                   ShareholdingConcentration, StockInfo,
                                   TtmCashFlowStatement, TtmIncomeStatement)

NUM_MAPS = {
    1: '一级',
    2: '二级',
    3: '三级',
    4: '四级',
}

TO_DORP_PAT = re.compile('^_|^[1-9]|^[一二三四五六七八九]')


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
    m = re.match(TO_DORP_PAT, x)
    if m:
        x = re.sub(TO_DORP_PAT, '', x)
        x = _normalized_col_name(x)
    return x

# ==========================信息=========================== #


def get_stock_info(only_A=True):
    """股票基础信息"""
    with session_scope('dataBrowse') as sess:
        query = sess.query(
            StockInfo.证券代码,
            StockInfo.证券简称,
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
                ~StockInfo.证券代码.startswith('2'),
                ~StockInfo.证券代码.startswith('9'),
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
    with session_scope('dataBrowse') as sess:
        query = sess.query(
            ClassificationBom.分类编码,
            ClassificationBom.分类名称,
        ).filter(
            ClassificationBom.分类编码.startswith('Z')
        )
        df = pd.DataFrame.from_records(query.all(), columns=['分类编码', '分类名称'])
        return df.sort_values('分类编码')


def _get_cn_industry(only_A, level, bom):
    """国证四级行业分类"""
    assert level in (1, 2, 3, 4), '国证行业只有四级分类'
    u_num = NUM_MAPS[level]
    col_names = ['sid', '国证{}行业'.format(u_num), '国证{}行业编码'.format(u_num)]
    with session_scope('dataBrowse') as sess:
        query = sess.query(
            Classification.证券代码,
            Classification.分类名称,
            Classification.分类编码
        ).filter(
            Classification.平台类别 == '国证行业分类'
        )
        if only_A:
            query = query.filter(
                ~Classification.证券代码.startswith('2'),
                ~Classification.证券代码.startswith('9'),
            )
        df = pd.DataFrame.from_records(query.all(), columns=col_names)
        if df.empty:
            msg = '在本地数据库中无法获取国证行业{}分类数据。\n'.format(u_num)
            msg += '这将导致股票分类数据缺失。\n'
            msg += '运行`stock db-classify`提取网络数据并存储在本地数据库。'
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
            THSGN.概念编码.distinct(),
            THSGN.概念,
        )
        df = pd.DataFrame.from_records(query.all())
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


# ==========================动态数据=========================== #
def get_tdata(only_A=True):
    """股票交易其他数据"""
    with session_scope('szsh') as sess:
        query = sess.query(
            StockDaily.股票代码,
            StockDaily.日期,
            StockDaily.成交金额,
            StockDaily.换手率,
            StockDaily.流通市值,
            StockDaily.总市值
        ).filter(
            StockDaily.流通市值 > 0.0
        )
        df = pd.DataFrame.from_records(query.all())
        df.columns = ['sid', 'asof_date', '成交金额', '换手率', '流通市值', '总市值']
        if only_A:
            df = df[~df.sid.str.startswith('2')]
            df = df[~df.sid.str.startswith('9')]
        df.sort_values(['sid', 'asof_date'], inplace=True)
        return df


def get_short_name_changes(only_A=True):
    """股票简称变动历史"""
    def f(g):
        return g[g['股票简称'] != g['股票简称'].shift(1)]
    with session_scope('szsh') as sess:
        query = sess.query(
            StockDaily.股票代码,
            StockDaily.日期,
            StockDaily.名称
        )
        df = pd.DataFrame.from_records(query.all())
        df.columns = ['sid', 'asof_date', '股票简称']
        if only_A:
            df = df[~df.sid.str.startswith('2')]
            df = df[~df.sid.str.startswith('9')]
        df.sort_values(['sid', 'asof_date'], inplace=True)
        return df.groupby('sid').apply(f).reset_index(drop=True)


def get_equity_data(only_A=True):
    """公司股本数据"""
    with session_scope('dataBrowse') as sess:
        query = sess.query(
            CompanyShareChange.证券代码,
            CompanyShareChange.变动日期,
            CompanyShareChange.总股本,
            CompanyShareChange.已流通股份,
        ).filter(
            CompanyShareChange.已流通股份 > 0
        )
    columns = ['sid', 'asof_date', '总股本', '流通股本']
    df = pd.DataFrame.from_records(query.all(), columns=columns)
    if only_A:
        df = df[~df.sid.str.startswith('2')]
        df = df[~df.sid.str.startswith('9')]
    return df


def get_margin_data(only_A=True):
    """融资融券数据"""
    with session_scope('dataBrowse') as sess:
        query = sess.query(
            Quote.证券代码,
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
    with session_scope('dataBrowse') as sess:
        query = sess.query(
            Dividend.证券代码,
            Dividend.分红年度,
            Dividend.董事会预案公告日期,
            Dividend.派息比例_人民币,
        ).filter(
            # 数据可能为空，排除
            Dividend.董事会预案公告日期.isnot(None),
            Dividend.派息比例_人民币 > 0
        )
        if only_A:
            query = query.filter(
                ~Dividend.证券代码.startswith('2'),
                ~Dividend.证券代码.startswith('9'),
            )
        df = pd.DataFrame.from_records(query.all())
        df.columns = ['sid', '分红年度', 'asof_date', '每股人民币派息']
        df['每股人民币派息'] = df['每股人民币派息'] / 10.0
        df.sort_values(['sid', 'asof_date'], inplace=True)
        return df

# ==========================财务报告=========================== #


def _fill_ad_and_ts(df, col='报告年度', ndays=45):
    """
    修复截止日期、公告日期。
    如果`asof_date`为空，则使用`col`的值
        `timestamp`在`col`的值基础上加`ndays`天"""
    cond = df.asof_date.isna()
    df.loc[cond, 'asof_date'] = df.loc[cond, col]
    df.loc[cond, 'timestamp'] = df.loc[cond, col] + pd.Timedelta(days=ndays)
    # 由于存在数据不完整的情形，当timestamp为空，在asof_date基础上加ndays
    cond1 = df.timestamp.isna()
    df.loc[cond1, 'timestamp'] = df.loc[cond1,
                                        'asof_date'] + pd.Timedelta(days=ndays)
    # 1991-12-31 时段数据需要特别修正
    cond2 = df.timestamp.map(lambda x: x.is_quarter_end)
    cond3 = df.asof_date == df.timestamp
    df.loc[cond2 & cond3, 'timestamp'] = df.loc[cond2 &
                                                cond3, 'asof_date'] + pd.Timedelta(days=ndays)


def _periodly_report(only_A, table):
    # 一般而言，定期财务报告截止日期与报告年度相同
    # 但不排除数据更正等情形下，报告年度与截止日期不一致
    to_drop = ['证券简称', '机构名称', '合并类型编码',
               '合并类型', '报表来源编码', '报表来源', 'last_refresh_time', '备注']
    engine = get_engine('dataBrowse')
    df = pd.read_sql_table(table, engine)
    if only_A:
        df = df[~df.证券代码.str.startswith('2')]
        df = df[~df.证券代码.str.startswith('9')]
    df.drop(to_drop, axis=1, inplace=True, errors='ignore')
    df.rename(columns={"证券代码": "sid",
                       "截止日期": "asof_date",
                       "公告日期": "timestamp"},
              inplace=True)
    # 修复截止日期
    _fill_ad_and_ts(df)
    # 规范列名称
    df.columns = df.columns.map(_normalized_col_name)
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
    """
    获取财报公告日期，供其他计算类型的表使用(使用资产负债表公告日期)

    注：
        季度报告、财务指标是依据定期报告计算得来，并没有实际的公告日期。
        以其利润表定期报告的公告日期作为`asof_date`
    """
    col_names = ['证券代码', '公告日期', '截止日期']
    with session_scope('dataBrowse') as sess:
        query = sess.query(
            PeriodlyBalanceSheet.证券代码,
            PeriodlyBalanceSheet.公告日期,
            PeriodlyBalanceSheet.截止日期
        )
        df = pd.DataFrame.from_records(query.all())
        df.columns = col_names
        return df


def _get_report(only_A, table, columns=None, col='截止日期'):
    """
    获取财务报告数据

    使用利润表的公告日期
    """
    engine = get_engine('dataBrowse')
    df = pd.read_sql_table(table, engine, columns=columns)
    if only_A:
        df = df[~df.证券代码.str.startswith('2')]
        df = df[~df.证券代码.str.startswith('9')]
    # df.drop(to_drop, axis=1, inplace=True, errors='ignore')
    asof_dates = _financial_report_announcement_date()
    keys = ['证券代码', '截止日期']
    if col != '截止日期':
        # 处理行业排名
        df['报告年度'] = df[col]
        # 原始数据列名称更改为'截止日期'
        df.rename(columns={col: '截止日期'}, inplace=True)
    df = df.join(
        asof_dates.set_index(keys), on=keys
    )
    df.rename(columns={"证券代码": "sid",
                       "截止日期": "asof_date",
                       "公告日期": "timestamp"},
              inplace=True)
    # 修复截止日期
    _fill_ad_and_ts(df)
    # 规范列名称
    df.columns = df.columns.map(_normalized_col_name)
    df.sort_values(['sid', 'asof_date'], inplace=True)
    return df


# ========================季度报告========================= #


def get_q_income_data(only_A=True):
    """季度利润表"""
    table = QuarterlyIncomeStatement.__tablename__
    to_drop = ['证券简称', '开始日期', '合并类型编码', '合并类型', 'last_refresh_time', '备注']
    columns = []
    for c in QuarterlyIncomeStatement.__table__.columns:
        if c.name not in to_drop:
            columns.append(c.name)
    df = _get_report(only_A, table, columns)
    return df


def get_q_cash_flow_data(only_A=True):
    """季度现金流量表"""
    table = QuarterlyCashFlowStatement.__tablename__
    to_drop = ['证券简称', '开始日期', '合并类型编码', '合并类型', 'last_refresh_time', '备注']
    columns = []
    for c in QuarterlyCashFlowStatement.__table__.columns:
        if c.name not in to_drop:
            columns.append(c.name)
    df = _get_report(only_A, table, columns)
    return df


# ==========================TTM=========================== #


def get_ttm_cash_flow_data(only_A=True):
    """TTM现金流量表"""
    table = TtmCashFlowStatement.__tablename__
    to_drop = ['证券简称', '开始日期', '合并类型编码', '合并类型', 'last_refresh_time', '备注']
    columns = []
    for c in TtmCashFlowStatement.__table__.columns:
        if c.name not in to_drop:
            columns.append(c.name)
    df = _get_report(only_A, table, columns)
    return df


def get_ttm_income_data(only_A=True):
    """TTM财务利润表"""
    table = TtmIncomeStatement.__tablename__
    to_drop = ['证券简称', '开始日期', '合并类型编码', '合并类型', 'last_refresh_time', '备注']
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
    to_drop = ['证券简称', '机构名称', '开始日期', '数据来源编码',
               '数据来源', 'last_refresh_time', '备注']
    columns = []
    for c in PeriodlyFinancialIndicator.__table__.columns:
        if c.name not in to_drop:
            columns.append(c.name)
    df = _get_report(only_A, table, columns)
    return df


def get_quarterly_financial_indicator_data(only_A=True):
    """单季财务指标"""
    table = QuarterlyFinancialIndicator.__tablename__
    to_drop = ['证券简称', '开始日期', '合并类型编码', '合并类型', 'last_refresh_time', '备注']
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
    to_drop = ['证券简称', '行业ID', '行业级别', '级别说明', 'last_refresh_time', '备注']
    columns = []
    for c in FinancialIndicatorRanking.__table__.columns:
        if c.name not in to_drop:
            columns.append(c.name)
    df = _get_report(only_A, table, columns, col='报告期')
    return df

# ==========================业绩预告=========================== #


def get_performance_forecaste_data(only_A=True):
    """上市公司业绩预告"""
    to_drop = ['证券简称', '业绩类型编码', '业绩类型', 'last_refresh_time', '备注']
    columns = []
    for c in PerformanceForecaste.__table__.columns:
        if c.name not in to_drop:
            columns.append(c.name)
    table = PerformanceForecaste.__tablename__
    engine = get_engine('dataBrowse')
    df = pd.read_sql_table(table, engine, columns=columns)
    if only_A:
        df = df[~df.证券代码.str.startswith('2')]
        df = df[~df.证券代码.str.startswith('9')]
    df.rename(columns={"证券代码": "sid",
                       "公告日期": "asof_date"},
              inplace=True)
    # 修复截止日期
    _fill_ad_and_ts(df, '报告年度')
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


# ==========================投资评级=========================== #


def get_investment_rating_data(only_A=True):
    """投资评级"""
    to_drop = ['序号', '证券简称', 'last_refresh_time', '备注']
    columns = []
    for c in InvestmentRating.__table__.columns:
        if c.name not in to_drop:
            columns.append(c.name)
    table = InvestmentRating.__tablename__
    engine = get_engine('dataBrowse')
    df = pd.read_sql_table(table, engine, columns=columns)
    if only_A:
        df = df[~df.证券代码.str.startswith('2')]
        df = df[~df.证券代码.str.startswith('9')]
    df.rename(columns={"证券代码": "sid",
                       "发布日期": "asof_date"},
              inplace=True)
    df.sort_values(['sid', 'asof_date'], inplace=True)
    return df
