"""
如表行数超大，bcolz写入str数据会异常缓慢，此时应尽量避免写入字符串类型数据，而是转换为类型进行处理。

使用默认输入缺失值
    bool_value   False
    dt_value     NaT
    float_value  NaN
    int_value    0
    str_value    None

替代方案：
    以附加属性写入信息
    或者更改为类别

性能：
    写入1千万行3列长度为6的随机数DataFrame，耗时不到1秒
"""
import os
import sys
import time
import warnings
from shutil import rmtree

import bcolz
import logbook
import pandas as pd

from cnswd.utils import make_logger

from ..common import AD_FIELD_NAME, SID_FIELD_NAME, TS_FIELD_NAME
from .base import bcolz_table_path
from .localdata import (get_dividend_data,
                        get_financial_indicator_ranking_data, get_margin_data,
                        get_p_balance_data, get_p_cash_flow_data,
                        get_p_income_data, get_performance_forecaste_data,
                        get_periodly_financial_indicator_data,
                        get_q_cash_flow_data, get_q_income_data,
                        get_quarterly_financial_indicator_data,
                        get_ttm_cash_flow_data, get_ttm_income_data)
from .preprocess import (get_investment_rating,
                         get_short_name_history, get_static_info_table)

# from .yahoo import YAHOO_ITEMS, read_item_data

# 设置显示日志
logbook.set_datetime_format('local')
logbook.StreamHandler(sys.stdout).push_application()
logger = make_logger('深证信数据包')

TAB_MAPS = {
    # 定期财务报告
    'periodly_balance_sheets': get_p_balance_data,
    'periodly_income_statements': get_p_income_data,
    'periodly_cash_flow_statements': get_p_cash_flow_data,
    # TTM财务报告
    'ttm_income_statements': get_ttm_income_data,
    'ttm_cash_flow_statements': get_ttm_cash_flow_data,
    # 报告期财务指标
    'periodly_financial_indicators': get_periodly_financial_indicator_data,
    # 季度财务指标
    'quarterly_financial_indicators': get_quarterly_financial_indicator_data,
    # 财务指标行业排名
    'financial_indicator_rankings': get_financial_indicator_ranking_data,
    # 上市公司业绩预告
    'performance_forecastes': get_performance_forecaste_data,
    # 季度利润表
    'quarterly_income_statements': get_q_income_data,
    # 季度现金流量表
    'quarterly_cash_flow_statements': get_q_cash_flow_data,
}


def _fix_mixed_type(df):
    # 1. 修复str类型中存在混合类型的列 如 ldf np.NaN lyy
    # 2. bool 类型含空值
    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col]):
            # 注意，必须输入字符None，否则会出错
            df.fillna(value={col: 'None'}, inplace=True)
        if pd.api.types.is_bool_dtype(df[col]):
            df.fillna(value={col: False}, inplace=True)
        if pd.api.types.is_integer_dtype(df[col]):
            df.fillna(value={col: -1}, inplace=True)
        if pd.api.types.is_datetime64tz_dtype(df[col]):
            raise ValueError('时间列不得带时区信息')


def write_dataframe(df, table_name, attr_dict=None):
    """以bcolz格式写入数据框"""
    # 转换为bcolz格式并存储
    rootdir = bcolz_table_path(table_name)
    if os.path.exists(rootdir):
        rmtree(rootdir)
    for c in (AD_FIELD_NAME, TS_FIELD_NAME, SID_FIELD_NAME):
        if c in df.columns and df[c].hasnans:
            warnings.warn(f'{c}列含有空值，已移除')
            df = df.loc[~df[c].isnull(), :]
    # 修复`asof_date newer than timestamp`
    # 至少相差一小时
    if AD_FIELD_NAME in df.columns and TS_FIELD_NAME in df.columns:
        cond = df[AD_FIELD_NAME] == df[TS_FIELD_NAME]
        df.loc[cond, AD_FIELD_NAME] = df.loc[cond,
                                             TS_FIELD_NAME] - pd.Timedelta(hours=1)
    # 修复混合类型，填充默认值，否则bcolz.ctable.fromdataframe会出错
    _fix_mixed_type(df)
    # 丢失tz信息
    ct = bcolz.ctable.fromdataframe(df, rootdir=rootdir)
    if attr_dict:
        # 设置属性
        for k, v in attr_dict.items():
            ct.attrs[k] = v
    ct.flush()
    logger.info(f'{len(df)} 行 写入：{rootdir}')


def write_static_info_to_bcolz():
    """写入股票分类等静态数据"""
    logger.info('读取股票分类数据')
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
        4. 投资评级
    """
    logger.info('读取融资融券')
    df_m = get_margin_data()
    write_dataframe(df_m, 'margin')
    logger.info('读取现金股利')
    df_dd = get_dividend_data()
    write_dataframe(df_dd, 'dividend')
    logger.info('读取股票简称变动历史')
    df_sn = get_short_name_history()
    write_dataframe(df_sn, 'shortname', {})
    logger.info('读取股票投资评级')
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
        logger.info(f'读取{table}')
        write_dataframe(func(), table)


# def write_yahoo():
#     for item in YAHOO_ITEMS:
#         df = read_item_data(item)
#         write_dataframe(df, item)


def write_data_to_bcolz():
    """写入Fundamentals数据"""
    print('准备写入Fundamentals数据......')
    s = time.time()
    write_static_info_to_bcolz()
    write_dynamic_data_to_bcolz()
    write_financial_data_to_bcolz()
    # write_yahoo()
    print(f"用时{time.time() - s:.2f}秒")
