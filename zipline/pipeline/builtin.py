"""其他自定义因子、分类器、过滤器

由于存在大量依赖，整合在一个模块中
"""
import numpy as np
import pandas as pd

from ..utils.math_utils import nanmean, nansum
from ..utils.numpy_utils import changed_locations, int64_dtype
from .classifiers import CustomClassifier
from .data.equity_pricing import CNEquityPricing
from .factors import (AverageDollarVolume, CustomFactor, DailyReturns,
                      SimpleMovingAverage)
from .filters import CustomFilter, StaticSids
from .fundamentals import Fundamentals

##############################################################################
#                                  自定义因子                                 #
##############################################################################


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
    
    用法：
    >>>from zipline.pipeline.builtin import TTM
    >>>from zipline.pipeline.fundamentals import Fundamentals
    >>>ttm = TTM(inputs=(Fundamentals.profit_statement.管理费用, Fundamentals.profit_statement.asof_date), window_length=255, is_cum=True)
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
        # 必须为同一数据集
        a = repr(self.inputs[0]).split('.')[0]
        b = repr(self.inputs[1]).split('.')[0]
        if a != b:
            raise ValueError(f'项目及其日期必须在同一数据集！项目数据集：{a} 日期数据集：{b}')

    def _locs_and_quarterly_multiplier(self, x):
        """计算单列的位置及季度乘子"""
        locs = changed_locations(x, True)[-4:]
        # 使用numpy计算季度乘子速度提升30%
        # 如为NaT，其季度结果为1
        months = x.astype('datetime64[M]').astype(int) % 12 + 1
        quarters = np.ceil(months / 3)
        factors = 4 / quarters
        return locs, factors[locs]

    def compute(self, today, assets, out, values, dates, is_cum):
        filled = np.where(np.isnat(dates), np.datetime64('1970-03-31'), dates)
        # 按股票计算位置变化及季度乘子
        locs = map(self._locs_and_quarterly_multiplier, filled.T)
        if is_cum:
            # 使用季度因子加权平均
            ttm = np.array([nanmean(value[loc] * ms)
                            for value, (loc, ms) in zip(values.T, locs)]).T
        else:
            # 非累计的原始数据，只需要简单相加，不需要季度因子调整
            ttm = np.array([nansum(value[loc])
                            for value, (loc, _) in zip(values.T, locs)]).T
        out[:] = ttm


class TradingDays(CustomFactor):
    """
    窗口期内所有有效交易天数

    Parameters
    ----------
    window_length : 整数
        统计窗口数量

    Notes
    -----
    如成交量大于0,表示当天交易
    """
    inputs = [CNEquityPricing.volume]

    def compute(self, today, assets, out, vs):
        out[:] = np.count_nonzero(vs, 0)


def QTradableStocksUS():
    """
    可交易股票(过滤器)

    条件
    ----
        1. 该股票在过去200天内必须有180天的有效收盘价
        2. 并且在最近20天的每一天都正常交易(非停牌状态)
        以上均使用成交量来判定，成交量为0，代表当天停牌
    """
    v20 = TradingDays(window_length=20)
    v200 = TradingDays(window_length=200)
    return (v20 >= 20) & (v200 >= 180)

##############################################################################
#                                 自定义分类器                                #
##############################################################################


class Sector(CustomClassifier):
    """
    股票行业分类代码(Industry groups are consolidated into 11 sectors)

    """
    SECTOR_NAMES = {
        101: '基本材料',  # 'BASIC_MATERIALS',
        102: '主要消费',  # 'CONSUMER_CYCLICAL',
        103: '金融服务',  # 'FINANCIAL_SERVICES',
        104: '房地产',    # 'REAL_ESTATE'),
        205: '可选消费',  # 'CONSUMER_DEFENSIVE',
        206: '医疗保健',  # 'HEALTHCARE',
        207: '公用事业',  # 'UTILITIES',
        308: '通讯服务',  # 'COMMUNICATION_SERVICES',
        309: '能源',      # 'ENERGY',
        310: '工业领域',  # 'INDUSTRIALS',
        311: '工程技术',  # 'TECHNOLOGY',
        -1:  '未知',
    }

    BASIC_MATERIALS = 101
    CONSUMER_CYCLICAL = 102
    FINANCIAL_SERVICES = 103
    REAL_ESTATE = 104
    CONSUMER_DEFENSIVE = 205
    HEALTHCARE = 206
    UTILITIES = 207
    COMMUNICATION_SERVICES = 308
    ENERGY = 309
    INDUSTRIALS = 310
    TECHNOLOGY = 311

    inputs = [Fundamentals.info.sector_code]
    window_length = 1
    dtype = int64_dtype
    missing_value = -1

    def compute(self, today, assets, out, sc):
        out[:] = sc[-1]


class SWSector(CustomClassifier):
    """申万行业"""
    AGRICULTURE = 801010
    MINING = 801020
    CHEMICALS = 801030
    STEEL = 801040
    METALS = 801050
    ELECTRONICS = 801080
    APPLIANCES = 801110
    FOOD = 801120
    TEXTILES = 801130
    LIGHT_MANUFACTURING = 801140
    PHARMACEUTICALS = 801150
    UTILITIES = 801160
    TRANSPORTATION = 801170
    REAL_ESTATE = 801180
    COMMERCE = 801200
    SERVICES = 801210
    CONGLOMERATE = 801230
    BUILDING_MATERIALS = 801710
    BUILDING_DECORATIONS = 801720
    ELECTRICALS = 801730
    DEFENSE_MILITARY = 801740
    IT = 801750
    MEDIA = 801760
    COMMUNICATION_SERVICES = 801770
    BANKS = 801780
    NONBANK_FINANCIALS = 801790
    AUTO = 801880
    MACHINERY = 801890

    SECTOR_NAMES = {
        801010: 'AGRICULTURE',  # 农林牧渔
        801020: 'MINING',  # 采掘
        801030: 'CHEMICALS',  # 化工
        801040: 'STEEL',  # 钢铁
        801050: 'METALS',  # 有色金属
        801080: 'ELECTRONICS',  # 电子
        801110: 'APPLIANCES',  # 家用电器
        801120: 'FOOD',  # 食品饮料
        801130: 'TEXTILES',  # 纺织服装
        801140: 'LIGHT_MANUFACTURING',  # 轻工制造
        801150: 'PHARMACEUTICALS',  # 医药生物
        801160: 'UTILITIES',  # 公用事业
        801170: 'TRANSPORTATION',  # 交通运输
        801180: 'REAL_ESTATE',  # 房地产
        801200: 'COMMERCE',  # 商业贸易
        801210: 'SERVICES',  # 休闲服务
        801230: 'CONGLOMERATE',  # 综合
        801710: 'BUILDING_MATERIALS',  # 建筑材料
        801720: 'BUILDING_DECORATIONS',  # 建筑装饰
        801730: 'ELECTRICALS',  # 电气设备
        801740: 'DEFENSE_MILITARY',  # 国防军工
        801750: 'IT',  # 计算机
        801760: 'MEDIA',  # 传媒
        801770: 'COMMUNICATION_SERVICES',  # 通信
        801780: 'BANKS',  # 银行
        801790: 'NONBANK_FINANCIALS',  # 非银金融
        801880: 'AUTO',  # 汽车
        801890: 'MACHINERY',  # 机械设备
    }

    window_length = 1
    inputs = (Fundamentals.info.sw_sector,)
    dtype = int64_dtype
    missing_value = -1

    def compute(self, today, assets, out, cats):
        out[:] = cats[0]


# 别名
QTradableStocksCN = QTradableStocksUS
