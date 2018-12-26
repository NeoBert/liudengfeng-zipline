import blaze
from datashape import isscalar, isrecord
from zipline.pipeline.loaders.blaze import from_blaze
from zipline.pipeline.loaders.blaze.core import datashape_type_to_numpy
from zipline.utils.memoize import classlazyval
from zipline.pipeline.domain import CN_EQUITIES
from zipline.utils.numpy_utils import (
    int64_dtype,
    categorical_dtype,
    _FILLVALUE_DEFAULTS,
)
from secdb.model.engine import SZX_ENGINE


def fillvalue_for_expr(expr):
    fillmissing = _FILLVALUE_DEFAULTS.copy()
    fillmissing.update({
        int64_dtype: -9999,
        categorical_dtype: 'NA',
    })
    
    ret = {}
    for name, type_ in expr.dshape.measure.fields:
        if isscalar(type_):
            n_type = datashape_type_to_numpy(type_)
            ret[name] = fillmissing[n_type]
    return ret


class Fundamentals(object):
    global_expr = blaze.data(SZX_ENGINE)
    
    ##################
    # Column
    ##################
    
    @classlazyval
    def sw_sector_name(cls):
        return cls.StockInfo.申万行业一级名称
    
    @classlazyval
    def total_assets(cls):
        return cls.BalanceSheet.资产总计
    
    @classlazyval
    def total_equity(cls):
        return cls.BalanceSheet.所有者权益或股东权益合计
    
    @classlazyval
    def total_debt(cls):
        return cls.BalanceSheet.负债合计
    
    @classlazyval
    def operating_cash_flow(cls):
        return cls.CashFlowStmt.经营活动产生的现金流量净额
    
    @classlazyval
    def investing_cash_flow(cls):
        return cls.CashFlowStmt.投资活动产生的现金流量净额
    
    @classlazyval
    def financing_cash_flow(cls):
        return cls.CashFlowStmt.筹资活动产生的现金流量净额
    
    @classlazyval
    def total_revenue(cls):
        return cls.IncomeStmt.一_营业总收入
    
    @classlazyval
    def gross_profit(cls):
        return cls.IncomeStmt.三_营业利润
    
    @classlazyval
    def net_profit(cls):
        return cls.IncomeStmt.五_净利润
    
    @classlazyval
    def shares_outstanding(cls):
        return cls.ShareChange.总股本
    
    @classlazyval
    def shares_outstanding(cls):
        return cls.ShareChange.人民币普通股
    
    @classlazyval
    def pe_ratio(cls):
        return cls.Pricing.市盈率
    
    ##################
    # Dataset
    ##################
    
    @classlazyval
    def StockInfo(cls):
        expr = cls.global_expr['StockInfo']
        expr = expr.relabel(
            股票代码='sid',
            上市日期='asof_date', 
        )
        return from_blaze(
            expr, 
            no_deltas_rule='ignore',
            no_checkpoints_rule='ignore',
            domain=CN_EQUITIES,
            missing_values=fillvalue_for_expr(expr)
        )
    
    @classlazyval
    def BalanceSheet(cls):
        expr = cls.global_expr['BalanceSheet']
        expr = expr.relabel(
            股票代码='sid',
            截止日期='asof_date',
            公告日期='timestamp', 
        )
        return from_blaze(
            expr, 
            no_deltas_rule='ignore',
            no_checkpoints_rule='ignore',
            domain=CN_EQUITIES,
            missing_values=fillvalue_for_expr(expr)
        )
    
    @classlazyval
    def CashFlowStmt(cls):
        expr = cls.global_expr['CashFlowStmt']
        expr = expr.relabel(
            股票代码='sid',
            截止日期='asof_date',
            公告日期='timestamp', 
        )
        return from_blaze(
            expr, 
            no_deltas_rule='ignore',
            no_checkpoints_rule='ignore',
            domain=CN_EQUITIES,
            missing_values=fillvalue_for_expr(expr)
        )
    
    @classlazyval
    def IncomeStmt(cls):
        expr = cls.global_expr['IncomeStmt']
        expr = expr.relabel(
            股票代码='sid',
            截止日期='asof_date',
            公告日期='timestamp', 
        )
        return from_blaze(
            expr, 
            no_deltas_rule='ignore',
            no_checkpoints_rule='ignore',
            domain=CN_EQUITIES,
            missing_values=fillvalue_for_expr(expr)
        )
    
    @classlazyval
    def ShareChange(cls):
        expr = cls.global_expr['ShareChange']
        expr = expr.relabel(
            股票代码='sid',
            变动日期='asof_date',
            公告日期='timestamp', 
        )
        return from_blaze(
            expr, 
            no_deltas_rule='ignore',
            no_checkpoints_rule='ignore',
            domain=CN_EQUITIES,
            missing_values=fillvalue_for_expr(expr)
        )   
    
    @classlazyval
    def Rating(cls):
        expr = cls.global_expr['Rating']
        expr = expr.relabel(
            股票代码='sid',
            发布日期='asof_date', 
        )
        return from_blaze(
            expr, 
            no_deltas_rule='ignore',
            no_checkpoints_rule='ignore',
            domain=CN_EQUITIES,
            missing_values=fillvalue_for_expr(expr)
        )
    
    @classlazyval
    def Pricing(cls):
        expr = cls.global_expr['Quote']
        expr = expr.relabel(
            股票代码='sid',
            交易日期='asof_date', 
        )
        return from_blaze(
            expr, 
            no_deltas_rule='ignore',
            no_checkpoints_rule='ignore',
            domain=CN_EQUITIES,
            missing_values=fillvalue_for_expr(expr)
        )      