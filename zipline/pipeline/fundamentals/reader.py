import blaze
from cnswd.sql.base import get_engine
from cnswd.sql.szx import StockInfo
from zipline.utils.memoize import classlazyval

from ..domain import CN_EQUITIES
from ..loaders.blaze import from_blaze
from .normalize import fillvalue_for_expr, gen_odo_kwargs

SZX_ENGINE = get_engine('szx')

class Fundamentals(object):
    szx_expr = blaze.data(SZX_ENGINE)

    ##################
    # Dataset
    ##################
    
    @classlazyval
    def StockInfo(cls):
        table = StockInfo.__tablename__
        expr = cls.szx_expr[table]
        expr = expr.relabel(
            股票代码='sid',
            上市日期='asof_date', 
        )
        return from_blaze(
            expr, 
            no_deltas_rule='ignore',
            no_checkpoints_rule='ignore',
            odo_kwargs=gen_odo_kwargs(expr),
            domain=CN_EQUITIES,
            missing_values=fillvalue_for_expr(expr)
        )
