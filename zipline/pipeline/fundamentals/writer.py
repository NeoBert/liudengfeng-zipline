"""
bcolz写入str数据非常缓慢，尽量避免写入字符串类型数据。

替代方案：
    以附加属性写入信息
    或者更改为类别

性能：
    写入1千万行3列长度为6的随机数DataFrame，耗时不到1秒
"""
import os
import sys
from shutil import rmtree

import bcolz
import logbook
from logbook import Logger

from ..common import AD_FIELD_NAME, TS_FIELD_NAME
from .base import bcolz_table_path
from .preprocess import _normalize_ad_ts_sid, get_static_info_table
from .sql import get_dividend_data, get_short_name_changes,get_equity_data,get_margin_data

# 设置显示日志
logbook.set_datetime_format('local')
logbook.StreamHandler(sys.stdout).push_application()


def write_dataframe(df, table_name, attr_dict=None):
    """以bcolz格式写入数据框"""
    log = Logger(table_name)
    # 转换为bcolz格式并存储
    rootdir = bcolz_table_path(table_name)
    if os.path.exists(rootdir):
        rmtree(rootdir)
    df = _normalize_ad_ts_sid(df)
    # df[AD_FIELD_NAME] = df[AD_FIELD_NAME].astype('int64')
    # df[TS_FIELD_NAME] = df[TS_FIELD_NAME].astype('int64')
    # odo(df, rootdir)
    ct = bcolz.ctable.fromdataframe(df, rootdir=rootdir)
    log.info('写入数据至：{}'.format(rootdir))
    if attr_dict:
        # 设置属性
        # ct = bcolz.open(rootdir)
        for k, v in attr_dict.items():
            ct.attrs[k] = v


def write_static_info_to_bcolz():
    """写入股票分类等静态数据"""
    table_name = 'infoes'
    logger = Logger(table_name)
    logger.info('准备数据......')
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
    # df_e = get_equity_data()
    # write_dataframe(df_e, 'equity')
    df_m = get_margin_data()
    write_dataframe(df_m, 'margin')
    # df_dd = get_dividend_data()
    # write_dataframe(df_dd, 'dividend')
    # df_sn = get_short_name_changes()
    # write_dataframe(df_sn, 'shortname')


def write_sql_data_to_bcolz():
    """写入Fundamentals数据"""
    write_static_info_to_bcolz()
    write_dynamic_data_to_bcolz()
    # write_financial_data_to_bcolz()
