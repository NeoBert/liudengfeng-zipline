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
result = pd.DataFrame({'Mkt-RF':rm_rf.values,'SMB':0.0,'HML':0.0}, index=rm_rf.index)
result.loc[df_rf.index, 'RF'] = df_rf

# 结果存放路径
file_name = "ff3"
root_dir = zipline_path(['factors'])
if not os.path.exists(root_dir):
    os.makedirs(root_dir)
result_path_ = os.path.join(root_dir, f'{file_name}.pkl')


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


def initialize(context):
    """
    use our factors to add our pipes and screens.
    """
    pipe = Pipeline()
    attach_pipeline(pipe, 'ff_example')

    #     common_stock = CommonStock()
    #     # filter down to securities that are either common stock or SPY
    #     pipe.set_screen(common_stock.eq(1))
    mkt_cap = MarketEquity()
    pipe.add(mkt_cap, 'market_cap')

    book_equity = BookEquity()
    # book equity over market equity
    be_me = book_equity/mkt_cap
    pipe.add(be_me, 'be_me')

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
    mkt_cap = factors.sort_values(['market_cap'], ascending=True)
    be_me = factors.sort_values(['be_me'], ascending=True)

    # to compose the six portfolios, split our universe into portions
    half = int(len(mkt_cap)*0.5)
    small_caps = mkt_cap[:half]
    big_caps = mkt_cap[half:]
    
    thirty = int(len(be_me)*0.3)
    seventy = int(len(be_me)*0.7)
    growth = be_me[:thirty]
    neutral = be_me[thirty:seventy]
    value = be_me[seventy:]

    # now use the portions to construct the portfolios.
    # note: these portfolios are just lists (indices) of equities
    small_value = small_caps.index.intersection(value.index)
    small_neutral = small_caps.index.intersection(neutral.index)
    small_growth = small_caps.index.intersection(growth.index)
    
    big_value = big_caps.index.intersection(value.index)
    big_neutral = big_caps.index.intersection(neutral.index)
    big_growth = big_caps.index.intersection(growth.index)

    # take the mean to get the portfolio return, assuming uniform
    # allocation to its constituent equities.
    sv = returns[small_value].mean()
    sn = returns[small_neutral].mean()
    sg = returns[small_growth].mean()
       
    bv = returns[big_value].mean()
    bn = returns[big_neutral].mean()
    bg = returns[big_growth].mean()

    # computing SMB
    context.smb = (sv + sn + sg)/3 - (bv + bn + bg)/3

    # computing HML
    context.hml = (sv + bv)/2 - (sg + bg)/2


def handle_data(context, data):
    # 去除时间，保留日期
    dt = get_datetime().normalize()
    context.result.loc[dt, 'SMB'] = context.smb
    context.result.loc[dt, 'HML'] = context.hml

    
# 使用分析函数保存因子计算结果   
def analyze(context, perf):
    dt = get_datetime().normalize()
    result = context.result.sort_index()
    result = result.loc[:dt, :].dropna()
    # 覆盖式存储因子数据
    result.to_pickle(result_path_)