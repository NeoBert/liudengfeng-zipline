"""
Dataset representing OHLCV data.
"""
from zipline.utils.numpy_utils import float64_dtype, categorical_dtype

from ..domain import US_EQUITIES, CN_EQUITIES
from .dataset import Column, DataSet


class EquityPricing(DataSet):
    """
    :class:`~zipline.pipeline.data.DataSet` containing daily trading prices and
    volumes.
    """
    open = Column(float64_dtype, currency_aware=True)
    high = Column(float64_dtype, currency_aware=True)
    low = Column(float64_dtype, currency_aware=True)
    close = Column(float64_dtype, currency_aware=True)
    volume = Column(float64_dtype)
    currency = Column(categorical_dtype)
    amount = Column(float64_dtype, currency_aware=True)
    market_cap = Column(float64_dtype, currency_aware=True)
    total_cap = Column(float64_dtype, currency_aware=True)
    shares_outstanding = Column(float64_dtype)
    total_shares = Column(float64_dtype)
    turnover = Column(float64_dtype)
    # 后复权股价。以首次交易价格为基准，涨跌幅累计计算
    b_open = Column(float64_dtype, currency_aware=True)
    b_high = Column(float64_dtype, currency_aware=True)
    b_low = Column(float64_dtype, currency_aware=True)
    b_close = Column(float64_dtype, currency_aware=True)


# Backwards compat alias.
USEquityPricing = EquityPricing.specialize(US_EQUITIES)
CNEquityPricing = EquityPricing.specialize(CN_EQUITIES)
