import numpy as np

from zipline.pipeline import CustomFactor
from zipline.pipeline.data import CNEquityPricing
from zipline.pipeline.factors import RSI
from zipline.pipeline.fundamentals import Fundamentals


class Momentum(CustomFactor):
    '''
    从12个月前到1个月前的11个月的累积收益率
    '''
    inputs = (CNEquityPricing.close,)
    params = {'t0': 21}
    window_length = 244

    def compute(self, today, assets, out, closes, t0):
        out[:] = closes[-t0] / closes[0] - 1


class Size(CustomFactor):
    inputs = (CNEquityPricing.close, Fundamentals.equity.流通股本)
    window_length = 1

    def compute(self, today, assets, out, closes, mkt_equity):
        out[:] = np.log(closes[-1] * mkt_equity[-1])


class Value(CustomFactor):
    inputs = (CNEquityPricing.close, Fundamentals.equity.流通股本,
              Fundamentals.balance_sheet.所有者权益或股东权益合计)
    window_length = 1

    def compute(self, today, assets, out, closes, mkt_equity, book_value):
        out[:] = closes[-1] * mkt_equity[-1] / book_value[-1]


def STR():
    return -RSI()


class Volatility(CustomFactor):
    """收益率波动"""
    inputs = (CNEquityPricing.close,)
    window_length = 122  # 半年

    def compute(self, today, assets, out, closes):
        rets = np.diff(closes, axis=0) / closes[:-1]
        out[:] = np.std(rets, axis=0)
