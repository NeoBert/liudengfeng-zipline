"""
定期财务报告

自巨潮提取上市公司预约与实际公布财务报告日期后，对已经有明确的公告日期的，时间戳更改为公告日期，
截止日期为公告日期前一天。对无法获取公告日期的，以报告日期后45天作为时间戳，前偏移一天为截止日期。

timestamp = 公告日期 如果公告日期存在
timestamp = 报告截止日期 + 45天 如果不存在公告日期

"""
from cnswd.sql.szx import (TtmIncomeStatement,
                           TtmCashFlowStatement,
                           PeriodlyBalanceSheet,
                           PeriodlyIncomeStatement,
                           PeriodlyCashFlowStatement,
                           QuarterlyFinancialIndicator,
                           PeriodlyFinancialIndicator,
                           PerformanceForecaste,
                           FinancialIndicatorRanking,
                           )
from .preprocess import (get_p_balance_data,
                         get_p_cash_flow_data,
                         get_p_income_data,
                         get_ttm_income_data,
                         get_ttm_cash_flow_data,
                         get_periodly_financial_indicator_data,
                         get_quarterly_financial_indicator_data,
                         get_financial_indicator_ranking_data,
                         get_performance_forecaste_data,
                         )
from .writer import write_dataframe


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
}


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
    """
    for table, func in TAB_MAPS.items():
        write_dataframe(func(), table)
