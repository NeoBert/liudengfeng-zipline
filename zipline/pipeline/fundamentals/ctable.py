from .base import bcolz_table_path
import bcolz


def get_ctable(table_name, bundle_name):
    """获取bcolz表数据"""
    rootdir = bcolz_table_path(table_name, bundle_name)
    ct = bcolz.ctable(rootdir=rootdir, mode='r')
    df = ct.todataframe()
    return df
