import numpy as np

from zipline.pipeline.data import CNEquityPricing
from zipline.pipeline.factors import CustomFactor
from zipline.pipeline.factors.statistical import vectorized_beta
from zipline.pipeline.fundamentals import Fundamentals
from zipline.utils.math_utils import nanmean

PPY = 244  # 每年交易天数

# TODO：-1 行业是否作为单独行业，还是使用补充股票方法?


class BaseExposure(CustomFactor):

    window_length = PPY * 2  # 2年
    sector_code = None

    def compute(self, today, assets, out, closes, sectors):
        res = np.zeros(closes.shape[1])
        rets = np.diff(closes, axis=0) / closes[:-1]
        sectors = sectors[-1]
        window_length = self.window_length

        match_col = np.where(sectors == self.sector_code)[0]
        match_rets = rets[:, match_col]
        target_rets = nanmean(match_rets, axis=1).reshape(-1, 1)

        allowed_missing = int(window_length * 0.25)
        # 行业内股票收益率基于行业收益率回归得到各股票的β值，即敞口
        beta = vectorized_beta(
            dependents=match_rets,
            independent=target_rets,
            allowed_missing=allowed_missing,
        )
        # 更新β值，其余部分为0
        res[match_col] = beta
        out[:] = res


class SectorExposure(BaseExposure):
    inputs = (CNEquityPricing.close,
              Fundamentals.info.sector_code)


class SWSectorExposure(BaseExposure):
    inputs = (CNEquityPricing.close,
              Fundamentals.info.sw_sector)
