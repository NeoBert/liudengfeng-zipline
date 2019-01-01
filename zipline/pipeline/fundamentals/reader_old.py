"""

基础数据容器包含
1. 基础信息
    + 市场状态 在市、暂停、退市
    + 上市日期
    + 地区分类

2. 财务数据
3. 投资评级
4. 业绩预告
5. 概念编码

"""

import re
import warnings

import bcolz
import blaze
import pandas as pd

from odo import discover

from zipline.pipeline.domain import CN_EQUITIES
from zipline.pipeline.loaders.blaze import from_blaze
from zipline.utils.memoize import classlazyval

from ..data.dataset import BoundColumn
from .base import bcolz_table_path
from .constants import (MARKET_MAPS, QUARTERLY_TABLES, SECTOR_NAMES,
                        SUPER_SECTOR_NAMES)
from .utils import _normalized_dshape, fillvalue_for_expr,gen_odo_kwargs

ITEM_CODE_PATTERN = re.compile(r'A\d{3}')
warnings.filterwarnings("ignore")


def verify_code(code):
    if not re.match(ITEM_CODE_PATTERN, code):
        raise ValueError('编码格式应为A+三位整数。输入{}'.format(code))


def _gen_expr(table_name):
    """生成表所对应的表达式"""
    rootdir = bcolz_table_path(table_name)
    # # 此时读取的datetime丢失了时区信息
    ctable = bcolz.ctable(rootdir=rootdir, mode='r')
    df = ctable.todataframe() # 转换为DataFrame对象
    raw_dshape = discover(ctable)
    dshape = _normalized_dshape(raw_dshape, True)
    expr = blaze.data(df, name=table_name, dshape=dshape)
    return expr


def gen_data_set(table_name):
    """读取bcolz格式的数据，生成DataSet"""
    expr = _gen_expr(table_name)
    return from_blaze(
        expr,
        # loader=global_loader,
        no_deltas_rule='ignore',
        no_checkpoints_rule='ignore',
        odo_kwargs=gen_odo_kwargs(expr),
        missing_values=fillvalue_for_expr(expr),
        domain=CN_EQUITIES,
    )


def query_maps(table_name, attr_name, key_to_int=False):
    """查询bcolz表中属性值"""
    rootdir = bcolz_table_path(table_name)
    ct = bcolz.open(rootdir)
    d = ct.attrs[attr_name]
    if key_to_int:
        return {int(k): v for k, v in d.items()}
    else:
        return d


class Fundamentals(object):
    """股票基础数据集容器类"""

    @staticmethod
    def has_column(column):
        """简单判定列是否存在于`Fundamentals`各数据集中"""
        return type(column) == BoundColumn

    #=======================根据数据集列编码查询列名称=================#
    # 数据集列使用四位编码，首位为A，其余按原始序列连续编号
    # 要查询列所代表的具体含义，使用下面方法
    # 输入A001之类的编码

    @classlazyval
    def concept_maps(self):
        return query_maps('infoes', 'concept')

    @staticmethod
    def concept_col_name(code):
        """股票概念中文名称（输入编码）"""
        table_name = 'infoes'
        attr_name = 'concept'
        maps = query_maps(table_name, attr_name)
        return maps[code]

    #===================根据数据集列名称关键字模糊查询列编码===============#
    # 有时需要根据列关键字查找列编码，使用下面方法
    # 输入关键字查询列编码

    @staticmethod
    def concept_col_code(key):
        """模糊查询概念编码（输入概念关键词）"""
        table_name = 'infoes'
        attr_name = 'concept'
        maps = query_maps(table_name, attr_name)
        out = {k: v for k, v in maps.items() if key in v}
        return out

    #========================编码中文含义========================#
    # 为提高读写速度，文本及类别更改为整数编码，查找整数所代表的含义，使用
    # 下列方法。数字自0开始，长度为len(类别)
    # 输入数字（如触发Keyerror，请减少数值再次尝试

    @classlazyval
    def supper_sector_maps(self):
        return query_maps('infoes', 'super_sector_code', True)

    @staticmethod
    def supper_sector_cname(code):
        """超级部门编码含义"""
        code = int(code)
        table_name = 'infoes'
        attr_name = 'super_sector_code'
        maps = query_maps(table_name, attr_name, True)
        return maps[code]

    @classlazyval
    def sector_maps(self):
        return query_maps('infoes', 'sector_code', True)

    @staticmethod
    def sector_cname(code):
        """部门编码含义"""
        code = int(code)
        table_name = 'infoes'
        attr_name = 'sector_code'
        maps = query_maps(table_name, attr_name, True)
        return maps[code]

    @staticmethod
    def sector_code(key):
        """关键词查询部门编码"""
        table_name = 'infoes'
        attr_name = 'sector_code'
        maps = query_maps(table_name, attr_name)
        out = {k: v for k, v in maps.items() if key in v}
        return out

    @staticmethod
    def market_cname(code):
        """市场版块编码含义"""
        code = str(code)
        table_name = 'infoes'
        attr_name = 'market'
        maps = query_maps(table_name, attr_name)
        return maps[code]

    @classlazyval
    def region_maps(self):
        return query_maps('infoes', 'region')

    @staticmethod
    def region_cname(code):
        """地域版块编码含义"""
        code = str(code)
        table_name = 'infoes'
        attr_name = 'region'
        maps = query_maps(table_name, attr_name)
        return maps[code]

    @staticmethod
    def region_code(key):
        """关键词查询地域编码"""
        table_name = 'infoes'
        attr_name = 'region'
        maps = query_maps(table_name, attr_name)
        out = {k: v for k, v in maps.items() if key in v}
        return out

    @classlazyval
    def csrc_industry_maps(self):
        return query_maps('infoes', 'csrc_industry')

    @staticmethod
    def csrc_industry_cname(code):
        """证监会行业编码含义"""
        code = str(code)
        table_name = 'infoes'
        attr_name = 'csrc_industry'
        maps = query_maps(table_name, attr_name)
        return maps[code]

    @staticmethod
    def csrc_industry_code(key):
        """关键词模糊查询证监会行业编码"""
        table_name = 'infoes'
        attr_name = 'csrc_industry'
        maps = query_maps(table_name, attr_name)
        out = {k: v for k, v in maps.items() if key in v}
        return out

    @classlazyval
    def cn_industry_maps(self):
        return query_maps('infoes', 'cn_industry')

    @staticmethod
    def cn_industry_cname(code):
        """国证行业编码含义"""
        code = str(code)
        table_name = 'infoes'
        attr_name = 'cn_industry'
        maps = query_maps(table_name, attr_name)
        return maps[code]

    @staticmethod
    def cn_industry_code(key):
        """关键词模糊查询国证行业编码"""
        table_name = 'infoes'
        attr_name = 'cn_industry'
        maps = query_maps(table_name, attr_name)
        out = {k: v for k, v in maps.items() if key in v}
        return out

    #========================数据集========================#

    @classlazyval
    def info(self):
        """股票静态信息数据集"""
        return gen_data_set(table_name='infoes')

    @classlazyval
    def balance_sheet(self):
        """资产负债数据集"""
        return gen_data_set(table_name='balance_sheets')

    @classlazyval
    def balance_sheet_yearly(self):
        """资产负债数据集(仅包含年度报告)"""
        return gen_data_set('balance_sheets', True)

    @classlazyval
    def profit_statement(self):
        """利润表数据集"""
        return gen_data_set(table_name='profit_statements')

    @classlazyval
    def profit_statement_yearly(self):
        """年度利润表数据集(仅包含年度报告)"""
        return gen_data_set('profit_statements', True)

    @classlazyval
    def cash_flow(self):
        """现金流量表数据集"""
        return gen_data_set(table_name='cashflow_statements')

    @classlazyval
    def cash_flow_yearly(self):
        """现金流量表数据集(仅包含年度报告)"""
        return gen_data_set('cashflow_statements', True)

    @classlazyval
    def key_financial_indicator(self):
        """主要财务指标数据集"""
        return gen_data_set(table_name='zyzbs')

    @classlazyval
    def key_financial_indicator_yearly(self):
        """主要财务指标数据集(仅包含年度报告)"""
        return gen_data_set('zyzbs', True)

    @classlazyval
    def margin(self):
        """融资融券数据集"""
        return gen_data_set(table_name='margin')

    @classlazyval
    def dividend(self):
        """每股股利数据集"""
        return gen_data_set(table_name='dividend')

    #========================单列========================#
    @classlazyval
    def rating(self):
        """股票评级（单列）"""
        return gen_data_set(table_name='rating').rating

    @classlazyval
    def short_name(self):
        """股票简称（单列）"""
        return gen_data_set(table_name='short_names').short_name

    @classlazyval
    def treatment(self):
        """股票特别处理（单列）"""
        return gen_data_set(table_name='special_treatments').treatment
