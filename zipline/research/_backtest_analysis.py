"""回测分析模块"""
import errno
import os

import pandas as pd
import pyfolio as pf

from zipline.utils.paths import zipline_path


def create_full_tear_sheet(self):
    """创建完整工作底稿(DataFrame扩展方法)"""
    returns, positions, transactions = pf.utils.extract_rets_pos_txn_from_zipline(
        self)
    loc = -int(len(self) / 4) if int(len(self) / 4) else -1
    live_start_date = self.index[loc]
    pf.create_full_tear_sheet(
        returns,
        positions=positions,
        transactions=transactions,
        live_start_date=live_start_date,
        round_trips=True)


def get_latest_backtest_info(dir_name=zipline_path(['backtest'])):
    """最新回测结果文件路径及更新时间"""
    assert os.path.isdir(dir_name)
    try:
        candidates = [os.path.join(dir_name, x) for x in os.listdir(dir_name)]
        most_recent = max(candidates, key=os.path.getmtime)
        return most_recent, pd.Timestamp(
            int(os.path.getmtime(most_recent)), unit='s', tz='Asia/Shanghai')
    except (ValueError, OSError) as e:
        if getattr(e, 'errno', errno.ENOENT) != errno.ENOENT:
            raise
        raise ValueError('在目录{}下，没有发现回测结果'.format(dir_name))


def get_backtest(dir_name=zipline_path(['backtest']), file_name=None):
    """获取最近的回测结果(数据框)"""
    if file_name is None:
        pref_file, _ = get_latest_backtest_info(dir_name)
    else:
        assert isinstance(file_name, str)
        assert file_name.endswith('.pkl'), '文件名必须带".pkl"扩展'
        pref_file = os.path.join(dir_name, file_name)
    return pd.read_pickle(pref_file)


pd.DataFrame.create_full_tear_sheet = create_full_tear_sheet
