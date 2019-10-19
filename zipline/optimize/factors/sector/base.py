import numpy as np

from zipline.pipeline.data import CNEquityPricing
from zipline.pipeline.factors import CustomFactor
from zipline.pipeline.factors.statistical import vectorized_beta
from zipline.pipeline.fundamentals import Fundamentals
from zipline.utils.math_utils import nanmean


PPY = 244  # 每年交易天数


class BaseExposure(CustomFactor):

    window_length = PPY * 2  # 2年
    sector_code = None

    def compute(self, today, assets, out, closes, sectors):
        res = np.zeros(closes.shape[1])
        change_ratio = np.diff(closes, axis=0) / closes[:-1]
        latest_sectors = sectors[-1]

        stock_in_sector = latest_sectors == self.sector_code
        change_ratio_in_sector = change_ratio[:, stock_in_sector]

        # epsilon = 0.000001
        # nan_locs = np.where(np.isnan(change_ratio_in_sector))[1]  # 列
        # print(assets[np.unique(nan_locs)])

        # change_ratio_in_sector = np.where(np.isnan(change_ratio_in_sector), epsilon, change_ratio_in_sector)
        # 行业收益率
        sector_returns = nanmean(change_ratio_in_sector, axis=1).reshape(-1, 1)

        allowed_missing = int(self.window_length * 0.25)
        # 行业内各股票收益率基于行业平均收益率回归得到各股票的β值，即敞口
        beta = vectorized_beta(
            dependents=change_ratio_in_sector,
            independent=sector_returns,
            allowed_missing=allowed_missing,
        )
        # 更新β值，其余部分为0
        res[stock_in_sector] = beta
        out[:] = res


class CNSectorExposure(BaseExposure):
    # 使用复权价确保正确计算收益率
    # 暂时缺失 b_close
    inputs = (CNEquityPricing.close,
              Fundamentals.info.sector_code)


class SWSectorExposure(BaseExposure):
    # 使用复权价确保正确计算收益率
    # 暂时缺失 b_close
    inputs = (CNEquityPricing.close,
              Fundamentals.info.sw_sector)
