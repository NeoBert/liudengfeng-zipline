from .writer_cninfo import write_data_to_bcolz as cninfo
from .writer_wy import write_data_to_bcolz as wy


def write_data_to_bcolz(bundle):
    """写入Fundamentals数据"""
    if 'wy' in bundle:
        wy()
    else:
        cninfo()
