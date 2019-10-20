"""
Dataset representing OHLCV data.
"""
from zipline.utils.numpy_utils import float64_dtype

from ..domain import CN_EQUITIES
from .dataset import Column, DataSet


class EquityPricing(DataSet):
    """
    :class:`~zipline.pipeline.data.DataSet` containing daily trading prices and
    volumes.
    """
    open = Column(float64_dtype)
    high = Column(float64_dtype)
    low = Column(float64_dtype)
    close = Column(float64_dtype)
    volume = Column(float64_dtype)


# # 更改为`CN_EQUITIES`
# Backwards compat alias.
CNEquityPricing = EquityPricing.specialize(CN_EQUITIES)
USEquityPricing = CNEquityPricing
