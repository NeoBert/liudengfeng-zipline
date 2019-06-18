import blaze as bz
import os
# from cnswd.utils import data_root
from zipline.utils.paths import data_root
from cnswd.sql.base import db_path


VALID_DB_NAMES = ('szsh', 'dataBrowse', 'info', 'thematicStatistics')


def bcolz_table_path(table_name):
    """bcolz文件路径"""
    root_dir = data_root('bcolz')
    if not os.path.exists(root_dir):
        os.makedirs(root_dir)
    path_ = os.path.join(root_dir, '{}.bcolz'.format(table_name))
    return path_


def sqlite_db_path(db_name):
    assert db_name in VALID_DB_NAMES, '有效数据库名称为:{}'.format(VALID_DB_NAMES)
    r_dir = db_path(db_name)
    return "sqlite:///{}".format(r_dir)


def sqlite_table_path(db_name, table_name):
    return '{}::{}'.format(sqlite_db_path(db_name), table_name)
