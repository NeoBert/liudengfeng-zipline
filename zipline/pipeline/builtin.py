"""其他自定义因子、分类器、过滤器

由于存在大量依赖，整合在一个模块中

说明：
    1. 雅虎数据基础单位：千
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


# region 自定义因子

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
    >>>ttm = TTM(inputs=[Fundamentals.profit_statement.管理费用,
                         Fundamentals.profit_statement.asof_date],
                 window_length=255, is_cum=True)
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

# region 过滤器

# 交易总体仅保护股票、指数，不包含ETF、债券

class IsIndex(CustomFilter):
    inputs = []
    window_length = 1

    def compute(self, today, assets, out):
        out[:] = [len(str(x)) == 7 for x in assets]


class IsShares(CustomFilter):
    inputs = []
    window_length = 1

    def compute(self, today, assets, out):
        out[:] = [len(str(x)) != 7 for x in assets]


def QTradableStocksUS():
    """
    可交易股票(过滤器)

    条件
    ----
        1. 该股票在过去200天内必须有180天的有效收盘价
        2. 并且在最近20天的每一天都正常交易(非停牌状态)
        以上均使用成交量来判定，成交量为0，代表当天停牌
    """
    is_stock = IsShares()
    v20 = TradingDays(window_length=20)
    v200 = TradingDays(window_length=200)
    return is_stock & (v20 >= 20) & (v200 >= 180)

# endregion


class PE(CustomFactor):
    """股价与每股收益比率(市盈率)"""
    window_length = 1
    window_safe = True
    inputs = (CNEquityPricing.close, Fundamentals.profit_statement.基本每股收益)

    def compute(self, today, assets, out, n, d):
        out[:] = n[-1] / d[-1]


class PB(CustomFactor):
    """市值与账面资产净值比率(市净率)"""
    window_length = 1
    window_safe = True

    inputs = (CNEquityPricing.close, CNEquityPricing.total_shares,
              Fundamentals.balance_sheet.所有者权益或股东权益合计)

    def compute(self, today, assets, out, c, n, d):
        out[:] = c[-1] * n[-1] / d[-1] * 10000.0


class PS(CustomFactor):
    """市值与销售总额比率(市销率)"""
    window_length = 1
    window_safe = True
    inputs = (CNEquityPricing.close, CNEquityPricing.total_shares,
              Fundamentals.profit_statement.其中营业收入)

    def compute(self, today, assets, out, c, n, d):
        out[:] = c[-1] * n[-1] / d[-1] * 10000.0


def ttm_sales():
    return TTM(inputs=[Fundamentals.profit_statement.其中营业收入,
                       Fundamentals.profit_statement.asof_date],
               is_cum=True)

# endregion

# region 行业分类器


class Sector(CustomClassifier):
    """
    股票行业分类代码(Industry groups are consolidated into 11 sectors)

    """
    SECTOR_NAMES = {
        101: '基本材料',  # 'BASIC_MATERIALS',
        102: '主要消费',  # 'CONSUMER_CYCLICAL',
        103: '金融服务',  # 'FINANCIAL_SERVICES',
        104: '房地产',    # 'REAL_ESTATE',
        205: '可选消费',  # 'CONSUMER_DEFENSIVE',
        206: '医疗保健',  # 'HEALTHCARE',
        207: '公用事业',  # 'UTILITIES',
        308: '通讯服务',  # 'COMMUNICATION_SERVICES',
        309: '能源',      # 'ENERGY',
        310: '工业领域',  # 'INDUSTRIALS',
        311: '工程技术',  # 'TECHNOLOGY',
        # -1:  '未知',
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

    inputs = (Fundamentals.info.sector_code,)
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
        801010: '农林牧渔',  # 'AGRICULTURE'
        801020: '采掘',  # MINING
        801030: '化工',  # CHEMICALS
        801040: '钢铁',  # STEEL
        801050: '有色金属',  # METALS
        801080: '电子',  # ELECTRONICS
        801110: '家用电器',  # APPLIANCES
        801120: '食品饮料',  # FOOD
        801130: '纺织服装',  # TEXTILES
        801140: '轻工制造',  # LIGHT_MANUFACTURING
        801150: '医药生物',  # PHARMACEUTICALS
        801160: '公用事业',  # UTILITIES
        801170: '交通运输',  # TRANSPORTATION
        801180: '房地产',  # REAL_ESTATE
        801200: '商业贸易',  # COMMERCE
        801210: '休闲服务',  # SERVICES
        801230: '综合',  # CONGLOMERATE
        801710: '建筑材料',  # BUILDING_MATERIALS
        801720: '建筑装饰',  # BUILDING_DECORATIONS
        801730: '电气设备',  # ELECTRICALS
        801740: '国防军工',  # DEFENSE_MILITARY
        801750: '计算机',  # IT
        801760: '传媒',  # MEDIA
        801770: '通信',  # COMMUNICATION_SERVICES
        801780: '银行',  # BANKS
        801790: '非银金融',  # NONBANK_FINANCIALS
        801880: '汽车',  # AUTO
        801890: '机械设备',  # MACHINERY
        # -1:  '未知',
    }

    window_length = 1
    inputs = (Fundamentals.info.sw_sector,)
    dtype = int64_dtype
    missing_value = -1

    def compute(self, today, assets, out, cats):
        out[:] = cats[0]
# endregion


# region 别名
QTradableStocksCN = QTradableStocksUS
QTradableStocks = QTradableStocksUS
# endregion

# region Valuation


def enterprise_value():
    """This number tells you what cash return you would get if you bought the entire company,
    including its debt. Enterprise Value = Market Cap + Preferred stock + Long-Term Debt And
    Capital Lease + Short Term Debt And Capital Lease + Securities Sold But Not Yet Repurchased
     - Cash, Cash Equivalent And Market Securities - Securities Purchased with Agreement to Resell
     - Securities Borrowed.
    """
    raise NotImplementedError()


def market_cap():
    """Price * Total SharesOutstanding. The most current market cap for example, would be
    the most recent closing price x the most recent reported shares outstanding. For ADR
    share classes, market cap is price * (ordinary shares outstanding / adr ratio).
    """
    return shares_outstanding() * CNEquityPricing.close.latest


def share_class_level_shares_outstanding():
    """The latest shares outstanding reported by the company of a particular share class;
    most common source of this information is from the cover of the 10K, 10Q, or 20F filing.
    This figure is an aggregated shares outstanding number for a particular share class of
    the company. This field is updated quarterly and it is not adjusted for corporate action
    events including splits.
    """
    raise NotImplementedError()


def shares_outstanding():
    """The latest total shares outstanding reported by the company; most common source of
    this information is from the cover of the 10K, 10Q, or 20F filing. This figure is an
    aggregated shares outstanding number for a company in terms of a particular share class.
    It can be used to calculate market cap, based on each individual share’s trading price
    and the total aggregated shares outstanding figure. This field is updated quarterly and
    it is not adjusted for corporate action events including splits.
    """
    return CNEquityPricing.total_shares.latest

# endregion

# region 辅助类


class ClipRatio(CustomFactor):
    """
    计算二个因子的比率。当数值低于给定下限时，以Nan替代

    **Default Inputs:** None
    **Default window_length:** 1

    params:
        **lower:** 数值下限
    """
    window_length = 1
    params = {'lower': 0.0}   # 数值下限

    def _validate(self):
        super(ClipRatio, self)._validate()
        if len(self.inputs) != 2:
            raise ValueError("计算因子之间的比率，输入项目长度只能为2")

    def compute(self, today, assets, out, n, d, lower):
        res = n[-1] / d[-1]
        out[:] = np.where(res < lower, np.nan, res)

# endregion

# region Valuation Ratios


def book_value_per_share():
    """Common Shareholder’s Equity / Diluted Shares Outstanding.
    """
    return Fundamentals.financial_indicators.每股净资产.latest


def book_value_yield():
    """BookValuePerShare / Price
    """
    return book_value_per_share() / CNEquityPricing.close.latest


def cash_return():
    """指自由现金流量与企业价值之比。 
    Morningstar通过使用公司文件或报告中报告的基础数据来计算比率：FCF / 企业价值.
    """
    return NotImplementedError()


def cf_yield():
    """CFOPerShare / Price.
    """
    return cfo_per_share() / CNEquityPricing.close.latest


def cfo_per_share():
    """Cash Flow from Operations / Average Diluted Shares Outstanding
    """
    n = TTM(inputs=[Fundamentals.quarterly_free_cash_flow.quarterlyOperatingCashFlow,
                    Fundamentals.quarterly_free_cash_flow.asof_date],
            is_cum=True)
    d = SimpleMovingAverage(
        inputs=[CNEquityPricing.total_shares],
        window_length=244) * 10000.0
    return n / d


def trailing_dividend_yield():
    """
    Dividends Per Share over the trailing 12 months / Price
    """
    ttm = TTM(inputs=[Fundamentals.dividend.每股人民币派息,
                      Fundamentals.dividend.asof_date], is_cum=False)
    return ttm / CNEquityPricing.close.latest


def earning_yield():
    """
    Diluted EPS / Price
    """
    n = profit_statement.稀释每股收益.latest
    return n / CNEquityPricing.close.latest


def ev_to_ebitda():
    """
    This reflects the fair market value of a company, and allows comparability 
    to other companies as this is capital structure-neutral.
    """
    n = market_cap()
    d = Fundamentals.quarterly_ebitda.quarterlyEbitda.latest * 1000.0
    return n / d


def fcf_per_share():
    """
    Free Cash Flow / Average Diluted Shares Outstanding
    """
    n = Fundamentals.quarterly_free_cash_flow.quarterlyFreeCashFlow.latest * 1000.0
    # 暂时以总股本代替
    d = shares_outstanding()
    return n / d


def trailing_pb_ratio():
    """Adjusted close price / Book Value Per Share.
    If the result is negative or zero, then null.
    """
    b = SimpleMovingAverage(
        inputs=[Fundamentals.financial_indicators.每股净资产],
        window_length=244
    )
    return ClipRatio(inputs=[b, CNEquityPricing.close])


def trailing_ps_ratio():
    ttm_sales = TTM(inputs=[Fundamentals.profit_statement.其中营业收入,
                            Fundamentals.profit_statement.asof_date],
                    is_cum=True)

    return ttm_sales / CNEquityPricing.close.latest
# endregion
