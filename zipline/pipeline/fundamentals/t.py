from collections import OrderedDict

import pandas as pd

import blaze
from cnswd.sql.base import get_engine
from datashape import (Date, DateTime, Option, Record, String, boolean, dshape,
                       integral, isrecord, isscalar, var)
from odo import discover, odo, resource
from zipline.pipeline.common import AD_FIELD_NAME, TS_FIELD_NAME
from zipline.pipeline.domain import CN_EQUITIES
from zipline.pipeline.loaders.blaze import from_blaze
from zipline.pipeline.loaders.blaze.core import datashape_type_to_numpy
from zipline.utils.numpy_utils import (_FILLVALUE_DEFAULTS, categorical_dtype,
                                       int64_dtype)


from zipline.pipeline.fundamentals.normalize import gen_odo_kwargs, fillvalue_for_expr, _relabel

SZX_ENGINE = get_engine('szx')
global_expr = blaze.data(SZX_ENGINE)
expr = global_expr['stock_infos']

def f(expr):
    expr = _relabel(expr)
    ds = from_blaze(
        expr,
        no_deltas_rule='ignore',
        no_checkpoints_rule='ignore',
        odo_kwargs=gen_odo_kwargs(expr),
        domain=CN_EQUITIES,
        missing_values=fillvalue_for_expr(expr)
    )
    return ds

ds = f(expr)


measure = expr.dshape.measure
measure['上市日期']
