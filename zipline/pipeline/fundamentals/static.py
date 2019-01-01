"""
静态分类数据

股票地域、行业、概念、交易所等信息本不属于静态数据，但极少变动，视同静态数据。简化处理

基础分类信息
asof_date    timestamp    sid      地域  证监会行业    同花顺行业  部门编码  超级部门   A001
2014-01-06   2014-01-07   1        湖南         A         A2       311       3       0
2014-01-07   2014-01-08   333      广东         B         B2       307       3       1
2014-01-08   2014-01-09   2024     广西         C         C2       207       2       0
2014-01-09   2014-01-10   300001   北京         D         D2       102       1       1

注意：
    将文字转换为类别，将映射保存为属性，提高读写速度。

TODO:废弃
"""
from logbook import Logger

from .preprocess import get_static_info_table
from .writer import write_dataframe


def write_static_info_to_bcolz():
    """写入股票分类等静态数据"""
    table_name = 'infoes'
    logger = Logger(table_name)
    logger.info('准备数据......')
    df, attr_dict = get_static_info_table()
    write_dataframe(df, table_name, attr_dict)
