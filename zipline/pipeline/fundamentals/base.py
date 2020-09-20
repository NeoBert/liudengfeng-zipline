import os

import blaze as bz

from zipline.utils.paths import zipline_path


def bcolz_table_path(table_name, bundle='cninfo'):
    """bcolz文件路径"""
    root_dir = zipline_path(['bcolz', bundle])
    if not os.path.exists(root_dir):
        os.makedirs(root_dir)
    path_ = os.path.join(root_dir, '{}.bcolz'.format(table_name))
    return path_
