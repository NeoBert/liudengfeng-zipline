"""

规范sqlite数据表达式

"""

from collections import OrderedDict

import blaze
from cnswd.sql.base import get_engine
from datashape import (Date, DateTime, Option, Record, String, boolean, dshape,
                       integral, isrecord, isscalar, var)
from odo import odo
from zipline.pipeline.loaders.blaze.core import datashape_type_to_numpy
from zipline.utils.numpy_utils import (_FILLVALUE_DEFAULTS, categorical_dtype,
                                       int64_dtype)

from ..common import AD_FIELD_NAME, TS_FIELD_NAME

  


def fillvalue_for_expr(expr):
    """为表达式填充空白值
    
    Arguments:
        expr {Expr} -- 要使用的blaze表达式
    """
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


def gen_odo_kwargs(expr, utc=True):
    """生成odo转换参数
    
    Arguments:
        expr {Expr} -- 要使用的blaze表达式
    
    Keyword Arguments:
        utc {bool} -- [是否将日期转换为utc] (default: {True})

    TODO：检查utc转换后的结果！！！
    """

    fields = OrderedDict(expr.dshape.measure.fields)
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
    return {'dshape': var * Record(out_dshape)}
