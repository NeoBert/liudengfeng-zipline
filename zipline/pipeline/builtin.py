import numpy as np
import pandas as pd

from ..utils.numpy_utils import changed_locations
from ..utils.math_utils import nanmean, nansum
from .factors import CustomFactor
from .classifiers import Classifier
from .fundamentals import Fundamentals


class TTM(CustomFactor):
    """
    尾部12个月项目因子季度加权平均值。

    **Default Inputs:** None

    **Default Window Length:** 255

    params:
        **is_cum：** 原始数据是否为累积值。默认为真。
            非累积营业收入
                2019 Q1 1000
                2019 Q2  800
                2019 Q3  300
                2019 Q4  900
            累积营业收入
                2019 Q1 1000
                2019 Q2 1800
                2019 Q3 2100
                2019 Q4 3000
            如原始数据为累积值，则使用季度因子调整加权平均计算TTM。
    """
    params = {
        'is_cum': True   # 如果原始数据为累计数，则标记为True
    }
    window_length = 255  # 确保超过一年

    def _validate(self):
        super(TTM, self)._validate()
        if self.window_length < 250:
            raise ValueError("有效窗口长度至少应大于等于250")
        if len(self.inputs) != 2:
            raise ValueError("输入项目共二项，第一项为要计算的指标，第二项为相应指标的`asof_date`")
        if getattr(self.inputs[1], 'name') != 'asof_date':
            raise ValueError("第二项输入指标名称指标必须为`asof_date`")

    def _locs_and_quarterly_multiplier(self, x):
        dts = pd.DatetimeIndex(x)
        # 取最后四个季度
        locs = changed_locations(dts, True)[-4:]
        ms = 4 / np.array(list(map(lambda x: x.quarter, dts[locs])))
        return locs, ms

    def compute(self, today, assets, out, values, dates, is_cum):
        locs = map(self._locs_and_quarterly_multiplier, dates.T)
        if is_cum:
            # 使用季度因子加权平均
            ttm = np.array([nanmean(value[loc] * ms)
                            for value, (loc, ms) in zip(values.T, locs)]).T
        else:
            # 非累计的原始数据，只需要简单相加，不需要季度因子调整
            ttm = np.array([nansum(value[loc])
                            for value, (loc, _) in zip(values.T, locs)]).T
        out[:] = ttm
