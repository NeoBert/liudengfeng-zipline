from collections import OrderedDict
import re
import numpy as np
import pandas as pd
from datashape import (Date, DateTime, Option, Record, String, boolean,
                       integral, isrecord, isscalar, var)
from pypinyin import lazy_pinyin, Style

from zipline.utils.numpy_utils import (_FILLVALUE_DEFAULTS, bool_dtype,
                                       categorical_dtype, datetime64ns_dtype,
                                       default_missing_value_for_dtype,
                                       float32_dtype, float64_dtype,
                                       int64_dtype, object_dtype)

from ..common import AD_FIELD_NAME, SID_FIELD_NAME, TS_FIELD_NAME
from ..loaders.blaze.core import datashape_type_to_numpy

DOT_PAT = re.compile(r"\.")
IN_TABLE = "1234567890"
OUT_TABLE = "一二三四五六七八九零"


def _normalized_dshape(input_dshape, utc=False):
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
            # if isinstance(type_, Option):
            #     type_ = type_.ty
            out_dshape.append([name, type_])
        # out_dshape.append([name, datashape_type_to_numpy(type_)])
    return var * Record(out_dshape)


def fillvalue_for_expr(expr):
    """表达式默认值"""
    fillmissing = _FILLVALUE_DEFAULTS.copy()
    fillmissing.update({int64_dtype: -1})
    ret = {}
    # 传入类型 对象、浮点、整数、日期、逻辑
    for name, type_ in expr.dshape.measure.fields:
        if name in (AD_FIELD_NAME, SID_FIELD_NAME, TS_FIELD_NAME):
            continue
        if isscalar(type_):
            n_type = datashape_type_to_numpy(type_)
            if pd.core.dtypes.common.is_float_dtype(n_type):
                ret[name] = fillmissing[float32_dtype]
            elif pd.core.dtypes.common.is_datetime64_any_dtype(n_type):
                ret[name] = fillmissing[datetime64ns_dtype]
            elif pd.core.dtypes.common.is_integer_dtype(n_type):
                ret[name] = fillmissing[int64_dtype]
            elif pd.core.dtypes.common.is_object_dtype(n_type):
                ret[name] = ''  # fillmissing[object_dtype]
            elif pd.core.dtypes.common.is_bool_dtype(n_type):
                ret[name] = fillmissing[bool_dtype]
    return ret


def gen_odo_kwargs(expr, utc=False):
    """生成odo转换参数

    Arguments:
        expr {Expr} -- 要使用的blaze表达式

    Keyword Arguments:
        utc {bool} -- [是否将日期转换为utc] (default: {True})
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


def get_acronym(phrase):
    """
    获取字符串的首字母
    :param phrase: 字符串
    :return: 字符串
    """
    return ''.join(lazy_pinyin(phrase, style=Style.FIRST_LETTER)).upper()


def regular_name(col_name, trantab):
    """规范列名称

    规则：
    1. 首字符为数字，以中文数字拼音首字符替代
    2. '.'以 'd'替代
    3. 其余不变

    案例：
    ----
    >>> regular_name("5G", trantab)
    WG
    >>> regular_name("3代半导体", trantab)
    S代半导体
    >>> regular_name("工业4.0", trantab)
    工业4D0
    >>> regular_name("阿里概念", trantab)
    阿里概念
    """
    first_letter = col_name[0]
    if first_letter in IN_TABLE:
        col_name = f"{get_acronym(first_letter.translate(trantab))}{col_name[1:]}"
    col_name = DOT_PAT.sub('D', col_name)
    return col_name


def get_bcolz_col_names(cols):
    """整理适应于bcolz表中列名称规范，返回OrderedDict对象"""
    trantab = str.maketrans(IN_TABLE, OUT_TABLE)   # 制作翻译表
    # col_names = OrderedDict(
    #     {col: get_acronym(col.translate(trantab)) for col in cols})
    col_names = OrderedDict()
    for col in cols:
        if col in (AD_FIELD_NAME, SID_FIELD_NAME, TS_FIELD_NAME):
            col_names[col] = col
        else:
            col_names[col] = regular_name(col, trantab)
    if len(col_names.values()) != len(set(col_names.values())):
        raise ValueError("整理后得列名称包含重复值")
    return col_names
