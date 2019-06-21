import os
import pandas as pd
import numpy as np
from zipline.api import attach_pipeline, pipeline_output, get_datetime
from zipline.pipeline import Pipeline
from zipline.pipeline import CustomFactor
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.factors import Returns
from zipline.pipeline.fundamentals import Fundamentals

from zipline.utils.paths import zipline_path
from zipline.data.treasuries_cn import earliest_possible_date, get_treasury_data
from zipline.data.benchmarks_cn import get_cn_benchmark_returns


START_DATE = pd.Timestamp('2002-1-7',tz='utc') # 基准收益率与国债同步数据开始日期


# 以国债三个月利率为rf。可选`1month`
df_rf = get_treasury_data(None, None)['3month']
df_rm = get_cn_benchmark_returns('000300')


# 目标输出
rm_rf = df_rm - df_rf
result = pd.DataFrame({'Mkt-RF':rm_rf.values,'SMB':0.0,'HML':0.0, 'RMW':0.0, 'CMA':0.0}, index=rm_rf.index)
result.loc[df_rf.index, 'RF'] = df_rf

# 结果存放路径
file_name = "ff5"
root_dir = zipline_path(['factors'])
if not os.path.exists(root_dir):
    os.makedirs(root_dir)
result_path_ = os.path.join(root_dir, f'{file_name}.pkl')


def groups(s, n):
    """将`Series`划分为`n`组"""
    assert isinstance(s, pd.Series)
    assert 2 <= n <= 3
    s = s.sort_values(ascending=True)
    if n == 2:
        half = int(len(s)*0.5)
        small = s[:half]
        big = s[half:]
        return big, small
    else:
        thirty = int(len(s)*0.3)
        seventy = int(len(s)*0.7)
        high = s[:thirty]
        middle = s[thirty:seventy]
        low = s[seventy:]
        return high, middle, low


def _sorts2t3(s2_groups, s3_groups):
    """
    将s2与s3划分为6类

    返回相交部分`index`
    """
    res = []
    for s2 in s2_groups:
        for s3 in s3_groups:
            res.append(s2.index.intersection(s3.index))
    return res


def smb_6(s2_groups, s3_groups, returns):
    """根据2个序列划分6组的平均收益率"""
    indexes = _sorts2t3(s2_groups, s3_groups)
    # 前部分为small，而后部分为big
    res = [returns[indexes[i]].mean() for i in range(3)] + [-returns[indexes[i]].mean() for i in range(3,6)]
    return 1/3 * sum(res)


class MarketEquity(CustomFactor):
    """
    this factor outputs the market cap of every security on the day.
    """
    window_length = 1
    inputs = [USEquityPricing.market_cap]

    def compute(self, today, assets, out, mcap):
        out[:] = mcap[-1]


class BookEquity(CustomFactor):
    """
    this factor outputs the book value of every security on the day.
    """
    window_length = 1
    # 原文使用tangible_book_value，即有形账面价值
    # 使用所有者权益来代替
    inputs = [Fundamentals.balance_sheet.所有者权益或股东权益合计]

    def compute(self, today, assets, out, book):
        out[:] = book[-1]


class OP(CustomFactor):
    """
    运营盈利能力因子

    备注：
        1. 以扣非净资产收益率代替
    """
    window_length = 1
    inputs = [Fundamentals.financial_indicators.净资产收益率_扣除非经常性损益]

    def compute(self, today, assets, out, book):
        out[:] = book[-1]


class INV(CustomFactor):
    """
    投资因子

    备注：
        1. 资产总计_t / 资产总计_t-1
    """
    window_length = 255   # 确保获取二年的资产总计数据
    inputs = [Fundamentals.balance_sheet.资产总计]

    def compute(self, today, assets, out, total):
        out[:] = total[-1] / total[0]


def initialize(context):
    """
    use our factors to add our pipes and screens.
    """
    pipe = Pipeline()
    attach_pipeline(pipe, 'ff_example')

    mkt_cap = MarketEquity()
    pipe.add(mkt_cap, 'market_cap')

    book_equity = BookEquity()
    # book equity over market equity
    bm = book_equity/mkt_cap
    pipe.add(bm, 'bm')

    # 营运能力
    op = OP()
    pipe.add(op, 'op')

    # 投资因子
    inv = INV()
    pipe.add(inv, 'inv')

    returns = Returns(window_length=2)
    pipe.add(returns, 'returns')
    
    dt = get_datetime().normalize()
    start_ = dt if dt > START_DATE else START_DATE
    context.result = result.loc[start_: , :]


def before_trading_start(context, data):
    """
    every trading day, we use our pipes to construct the Fama-French
    portfolios, and then calculate the Fama-French factors appropriately.
    """
    factors = pipeline_output('ff_example')

    # get the data we're going to use
    returns = factors['returns']

    # 流通市值分二组
    big, small = groups(factors['market_cap'], 2)
    
    # 计算 SMB(B/M)
    bm_growth, bm_neutral, bm_value = groups(factors['bm'], 3)
    # small在前
    smb_bm = smb_6((small, big), (bm_growth, bm_neutral, bm_value), returns)

    # 计算 SMB(OP)
    op_robust, op_neutral, op_weak = groups(factors['op'], 3)
    # small在前
    smb_op = smb_6((small, big), (op_robust, op_neutral, op_weak), returns)

    # 计算 SMB(INV)
    inv_aggressive, inv_neutral, inv_conservative = groups(factors['inv'], 3)
    # small在前
    smb_inv = smb_6((small, big), (inv_aggressive, inv_neutral, inv_conservative), returns)   
    
    # 计算 SMB(B/M)
    smb = (smb_bm + smb_op + smb_inv) / 3

    # 计算 HML
    small_value = returns[small.index.intersection(bm_value.index)].mean()
    big_value = returns[big.index.intersection(bm_value.index)].mean()
    small_growth = returns[small.index.intersection(bm_growth.index)].mean()
    big_growth = returns[big.index.intersection(bm_growth.index)].mean()
    hml = 1/2 * (small_value + big_value - small_growth - big_growth)

    # 计算 RMW
    small_robust = returns[small.index.intersection(op_robust.index)].mean()
    big_robust = returns[big.index.intersection(op_robust.index)].mean()
    small_weak = returns[small.index.intersection(op_weak.index)].mean()
    big_weak = returns[big.index.intersection(op_weak.index)].mean()
    rmw = 1/2 * (small_robust + big_robust - small_weak - big_weak)
    
    # 计算 CMA
    small_conservative = returns[small.index.intersection(inv_conservative.index)].mean()
    big_conservative = returns[big.index.intersection(inv_conservative.index)].mean()
    small_aggressive  = returns[small.index.intersection(inv_aggressive.index)].mean()
    big_aggressive = returns[big.index.intersection(inv_aggressive.index)].mean()
    cma = 1/2 * (small_conservative + big_conservative - small_aggressive - big_aggressive)

    # 保留数据
    context.smb = smb
    context.hml = hml
    context.rmw = rmw
    context.cma = cma    

def handle_data(context, data):
    # 去除时间，保留日期
    dt = get_datetime().normalize()
    context.result.loc[dt, 'SMB'] = context.smb
    context.result.loc[dt, 'HML'] = context.hml
    context.result.loc[dt, 'RMW'] = context.rmw
    context.result.loc[dt, 'CMA'] = context.cma

# 使用分析函数保存因子计算结果   
def analyze(context, perf):
    dt = get_datetime().normalize()
    result = context.result.sort_index()
    result = result.loc[:dt, :].dropna()
    # 覆盖式存储因子数据
    result.to_pickle(result_path_)