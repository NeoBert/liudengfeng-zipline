"""
Dataset representing OHLCV data.
"""
from zipline.utils.numpy_utils import float64_dtype
import numpy as np
from ..domain import CN_EQUITIES
from .dataset import Column, DataSet


class EquityPricing(DataSet):
    """
    Dataset representing daily trading prices and volumes.
    """
    open = Column(float64_dtype)
    high = Column(float64_dtype)
    low = Column(float64_dtype)
    close = Column(float64_dtype)
    volume = Column(float64_dtype)
    amount = Column(float64_dtype)
    market_cap = Column(float64_dtype)
    total_cap = Column(float64_dtype)
    shares_outstanding = Column(float64_dtype)
    total_shares = Column(float64_dtype)
    turnover = Column(float64_dtype)
    # 后复权股价，主要用于技术分析
    # 以首发价为开始，用当日涨跌幅累计计算
    b_open = Column(float64_dtype)
    b_high = Column(float64_dtype)
    b_low = Column(float64_dtype)
    b_close = Column(float64_dtype)

# # 更改为`CN_EQUITIES`
# Backwards compat alias.
CNEquityPricing = EquityPricing.specialize(CN_EQUITIES)
USEquityPricing = CNEquityPricing
