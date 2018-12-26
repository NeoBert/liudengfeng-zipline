from collections import OrderedDict

import numpy as np
import pandas as pd
from datashape import (Date, DateTime, Option, Record, String, boolean,
                       integral, isrecord, isscalar, var)

from zipline.pipeline.loaders.blaze.core import datashape_type_to_numpy
from zipline.utils.numpy_utils import (_FILLVALUE_DEFAULTS, bool_dtype,
                                       categorical_dtype, datetime64ns_dtype,
                                       default_missing_value_for_dtype,
                                       float64_dtype, int64_dtype,
                                       object_dtype)

from ..common import AD_FIELD_NAME, SID_FIELD_NAME, TS_FIELD_NAME
from ..loaders.blaze.core import datashape_type_to_numpy


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


def make_default_missing_values_for_expr(expr):
    """数据集输出时的字段缺省默认值"""
    missing_values = {}
    for name, type_ in expr.dshape.measure.fields:
        # 无需设置缺损值的列，直接跳过
        if name in (AD_FIELD_NAME, SID_FIELD_NAME, TS_FIELD_NAME):
            continue
        n_type = datashape_type_to_numpy(type_)
        if n_type is object_dtype:
            missing_values[name] = '未定义'
        elif n_type is bool_dtype:
            missing_values[name] = False
        elif n_type is int64_dtype:
            missing_values[name] = -1
        else:
            missing_values[name] = default_missing_value_for_dtype(n_type)
    return missing_values


def make_default_missing_values_for_df(dtypes):
    """DataFrame对象各字段生成缺省默认值"""
    missing_values = {}
    # 此处name为字段名称
    for f_name, type_ in dtypes.items():
        name = type_.name
        if name.startswith('int'):
            # # 应为0
            missing_values[f_name] = 0
        elif name.startswith('object'):
            missing_values[f_name] = '未定义'
        else:
            missing_values[f_name] = default_missing_value_for_dtype(type_)
    return missing_values


def fillvalue_for_expr(expr):
    """表达式默认值"""
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
