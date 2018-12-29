from cnswd.sql.base import get_engine
import blaze
from zipline.pipeline.loaders.blaze import from_blaze
from zipline.pipeline.domain import CN_EQUITIES
from zipline.pipeline.loaders.blaze.core import datashape_type_to_numpy
from datashape import isscalar, isrecord
from zipline.utils.numpy_utils import (
    int64_dtype,
    categorical_dtype,
    _FILLVALUE_DEFAULTS,
)


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


from datashape import (Date, DateTime, Option, Record, String, boolean,
                       integral, isrecord, isscalar, var)
from collections import OrderedDict

def _normalized_dshape(input_dshape, utc=True):
    """
    关闭`dshape`中可选，保留原始数据类型
    option[int] -> int
    如`utc==True`, 则修正ctable中丢失时区信息
    """
    fields = OrderedDict(input_dshape.measure.fields)
    out_dshape = []
    for name, type_ in fields.items():
        if name in (AD_FIELD_NAME, TS_FIELD_NAME):
            if utc:
                out_dshape.append([name, DateTime(tz='UTC')])
            else:
                out_dshape.append([name, DateTime()])
        else:
            if isinstance(type_, Option):
                type_ = type_.ty
            out_dshape.append([name, type_])
    return var * Record(out_dshape)

SZX_ENGINE = get_engine('szx')
global_expr = blaze.data(SZX_ENGINE)
expr = global_expr['stock_infos']

expr.dshape.measure

expr = expr.relabel(
    股票代码='sid',
    上市日期='asof_date', 
)
ds = from_blaze(
    expr, 
    no_deltas_rule='ignore',
    no_checkpoints_rule='ignore',
    domain=CN_EQUITIES,
    missing_values=fillvalue_for_expr(expr)
)

dataset_expr = expr
measure = dataset_expr.dshape.measure
measure['asof_date']