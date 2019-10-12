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
        change_ratio = np.diff(closes, axis=0) / closes[:-1]
        sectors = sectors[-1]
        window_length = self.window_length

        match_col = np.where(sectors == self.sector_code)[0]
        change_ratio_in_sector = change_ratio[:, match_col]
        # 行业收益率
        sector_returns = nanmean(change_ratio_in_sector, axis=1).reshape(-1, 1)

        allowed_missing = int(window_length * 0.25)
        # 行业内股票收益率基于行业收益率回归得到各股票的β值，即敞口
        beta = vectorized_beta(
            dependents=change_ratio_in_sector,
            independent=sector_returns,
            allowed_missing=allowed_missing,
        )
        # 更新β值，其余部分为0
        res[match_col] = beta
        out[:] = res


class SectorExposure(BaseExposure):
    # 使用复权价确保正确计算收益率
    inputs = (CNEquityPricing.b_close,
              Fundamentals.info.sector_code)


class SWSectorExposure(BaseExposure):
    # 使用复权价确保正确计算收益率
    inputs = (CNEquityPricing.b_close,
              Fundamentals.info.sw_sector)
