"""
动态信息

包括：
    1. 股票简称变动历史
    2. 融资融券
    3. 现金股利
"""
from .writer import write_dataframe
from .preprocess import (get_dividend_data, get_margin_data,
                         get_short_name_changes)



def write_dynamic_data_to_bcolz():
    """
    将每日变动数据以bcolz格式存储，提高数据集加载速度

    项目：
        1. 融资融券
        2. 现金股利
        3. 股票简称变动历史
    """
    df_md = get_margin_data()
    write_dataframe(df_md, 'margin')
    df_dd = get_dividend_data()
    write_dataframe(df_dd, 'dividend')
    df_sn = get_short_name_changes()
    write_dataframe(df_sn, 'shortname')
