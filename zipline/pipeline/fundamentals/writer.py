"""
如表行数超大，bcolz写入str数据会异常缓慢，此时应尽量避免写入字符串类型数据，而是转换为类型进行处理。

替代方案：
    以附加属性写入信息
    或者更改为类别

性能：
    写入1千万行3列长度为6的随机数DataFrame，耗时不到1秒
"""
import os
import sys
import warnings
from shutil import rmtree

import bcolz
import logbook
from logbook import Logger

from cnswd.sql.szx import (FinancialIndicatorRanking, PerformanceForecaste,
                           PeriodlyBalanceSheet, PeriodlyCashFlowStatement,
                           PeriodlyFinancialIndicator, PeriodlyIncomeStatement,
                           QuarterlyCashFlowStatement,
                           QuarterlyFinancialIndicator,
                           QuarterlyIncomeStatement, TtmCashFlowStatement,
                           TtmIncomeStatement)

from ..common import AD_FIELD_NAME, TS_FIELD_NAME, SID_FIELD_NAME
from .base import bcolz_table_path
from .preprocess import (_normalize_ad_ts_sid, get_investment_rating,
                         get_static_info_table)
from .sql import (get_dividend_data, get_equity_data,
                  get_financial_indicator_ranking_data, get_margin_data,
                  get_p_balance_data, get_p_cash_flow_data, get_p_income_data,
                  get_performance_forecaste_data,
                  get_periodly_financial_indicator_data, get_q_cash_flow_data,
                  get_q_income_data, get_quarterly_financial_indicator_data,
                  get_short_name_changes, get_tdata, get_ttm_cash_flow_data,
                  get_ttm_income_data)

# 设置显示日志
logbook.set_datetime_format('local')
logbook.StreamHandler(sys.stdout).push_application()

TAB_MAPS = {
    # 定期财务报告
    PeriodlyBalanceSheet.__tablename__: get_p_balance_data,
    PeriodlyIncomeStatement.__tablename__: get_p_income_data,
    PeriodlyCashFlowStatement.__tablename__: get_p_cash_flow_data,
    # TTM财务报告
    TtmIncomeStatement.__tablename__: get_ttm_income_data,
    TtmCashFlowStatement.__tablename__: get_ttm_cash_flow_data,
    # 报告期财务指标
    PeriodlyFinancialIndicator.__tablename__: get_periodly_financial_indicator_data,
    # 季度财务指标
    QuarterlyFinancialIndicator.__tablename__: get_quarterly_financial_indicator_data,
    # 财务指标行业排名
    FinancialIndicatorRanking.__tablename__: get_financial_indicator_ranking_data,
    # 上市公司业绩预告
    PerformanceForecaste.__tablename__: get_performance_forecaste_data,
    # 季度利润表
    QuarterlyIncomeStatement.__tablename__: get_q_income_data,
    # 季度现金流量表
    QuarterlyCashFlowStatement.__tablename__: get_q_cash_flow_data,
}


def write_dataframe(df, table_name, attr_dict=None):
    """以bcolz格式写入数据框"""
    log = Logger(table_name)
    # 转换为bcolz格式并存储
    rootdir = bcolz_table_path(table_name)
    if os.path.exists(rootdir):
        rmtree(rootdir)
    df = _normalize_ad_ts_sid(df)
    for c in (AD_FIELD_NAME, TS_FIELD_NAME, SID_FIELD_NAME):
        if df[c].hasnans:
            warnings.warn(f'{c}列含有空值，已移除')
            df = df.loc[~df[c].isnan(), :]
    ct = bcolz.ctable.fromdataframe(df, rootdir=rootdir)
    log.info('写入数据至：{}'.format(rootdir))
    if attr_dict:
        # 设置属性
        for k, v in attr_dict.items():
            ct.attrs[k] = v


def write_static_info_to_bcolz():
    """写入股票分类等静态数据"""
    table_name = 'infoes'
    df, attr_dict = get_static_info_table()
    write_dataframe(df, table_name, attr_dict)


def write_dynamic_data_to_bcolz():
    """
    将每日变动数据以bcolz格式存储，提高数据集加载速度

    项目：
        1. 交易数据(含融资融券)
        2. 现金股利
        3. 股票简称变动历史
    """
    df_t = get_tdata()
    write_dataframe(df_t, 'tdata')
    df_e = get_equity_data()
    write_dataframe(df_e, 'equity')
    df_m = get_margin_data()
    write_dataframe(df_m, 'margin')
    df_dd = get_dividend_data()
    write_dataframe(df_dd, 'dividend')
    df_sn = get_short_name_changes()
    write_dataframe(df_sn, 'shortname')
    df_ir, attr_dic = get_investment_rating()
    write_dataframe(df_ir, 'investment_rating', attr_dic)


def write_financial_data_to_bcolz():
    """写入财务报告数据

    项目：
        1. 定期资产负债表
        2. 定期利润表
        3. 定期现金流量表
        4. TTM利润表
        5. TTM现金流量表
        6. 报告期财务指标
        7. 季度财务指标
        8. 财务指标行业排名
        9. 上市公司业绩预告
        10. 季度利润表
        11. 季度现金流量表
    """
    for table, func in TAB_MAPS.items():
        write_dataframe(func(), table)


def write_sql_data_to_bcolz():
    """写入Fundamentals数据"""
    write_static_info_to_bcolz()
    write_dynamic_data_to_bcolz()
    write_financial_data_to_bcolz()