"""

基础数据容器包含
1. 基础信息
    + 上市日期
    + 地区分类
    + 行业分类

2. 财务数据
3. 投资评级
4. 业绩预告
5. 概念编码
6. 雅虎财经 自由现金流、息税前利润
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

# from zipline.pipeline.loaders.blaze import global_loader
from ..data.dataset import BoundColumn
from .base import bcolz_table_path
from .constants import SECTOR_NAMES, SUPER_SECTOR_NAMES
from .utils import _normalized_dshape, fillvalue_for_expr, gen_odo_kwargs

ITEM_CODE_PATTERN = re.compile(r'A\d{3}')
warnings.filterwarnings("ignore")


def _gen_expr(table_name):
    """生成表所对应的表达式"""
    rootdir = bcolz_table_path(table_name, 'wy')
    ct = bcolz.ctable(rootdir=rootdir, mode='r')
    # 必须转换为DataFrame对象
    df = ct.todataframe()
    raw_dshape = discover(df)
    # 此处不得使用UTC，即 asof_date: datetime
    # 而不是 asof_date: datetime[tz='UTC']
    dshape_ = _normalized_dshape(raw_dshape, False)
    expr = blaze.data(df, name=table_name, dshape=dshape_)
    return expr


def gen_data_set(table_name):
    """读取bcolz格式的数据，生成DataSet"""
    expr = _gen_expr(table_name)
    return from_blaze(
        expr,
        # loader=None,
        no_deltas_rule='ignore',
        no_checkpoints_rule='ignore',
        # odo_kwargs=gen_odo_kwargs(expr, utc=False),
        missing_values=fillvalue_for_expr(expr),
        domain=CN_EQUITIES,
    )


def attr_maps(table_name, attr_name):
    """查询原始名称对应的bcolz表中列名称"""
    rootdir = bcolz_table_path(table_name, 'wy')
    ct = bcolz.open(rootdir)
    return ct.attrs[attr_name]


def _code(key, table_name, attr_name):
    maps = attr_maps(table_name, attr_name)
    out = {k: v for k, v in maps.items() if key in v}
    return out


def _cname(code, table_name, attr_name):
    code = int(code)
    maps = attr_maps(table_name, attr_name)
    return maps[code]


class Fundamentals(object):
    """股票基础数据集容器类"""

    @staticmethod
    def has_column(column):
        """简单判定列是否存在于`Fundamentals`各数据集中"""
        return type(column) == BoundColumn

    #=======================根据数据集列编码查询列名称=================#
    # 股票概念列名称处理原则：
    # 首字为数字 以中文数字拼音首字符替代
    # 如概念名称中含 '.', 以大写 'D'替代

    @staticmethod
    def query_concept_col_name(concept_name):
        """股票概念中文名称所对应的列名称"""
        table_name = '股票概念'
        return attr_maps(table_name, concept_name)

    #===================根据数据集列名称关键字模糊查询列编码===============#
    # 有时需要根据列关键字查找列编码，使用下面方法
    # 输入关键字查询列编码

    @staticmethod
    def ir_iarc_code(key):
        """模糊查询研究机构编码（输入研究机构关键词）"""
        table_name = '投资评级'
        attr_name = '评级机构'
        return _code(key, table_name, attr_name)

    @staticmethod
    def ir_researcher_code(key):
        """模糊查询研究员编码（输入研究员关键词）"""
        table_name = '投资评级'
        attr_name = '分析师'
        return _code(key, table_name, attr_name)

    #========================编码中文含义========================#
    # 为提高读写速度，文本及类别更改为整数编码，查找整数所代表的含义，使用
    # 下列方法。数字自0开始，长度为len(类别)
    # 输入数字（如触发Keyerror，请减少数值再次尝试

    @staticmethod
    def ir_iarc_cname(code):
        """研究机构编码对应名称"""
        table_name = ''
        attr_name = '评级机构'
        return _cname(code, table_name, attr_name)

    @staticmethod
    def ir_researcher_cname(code):
        """研究员编码对应名称"""
        table_name = '投资评级'
        attr_name = '分析师'
        return _cname(code, table_name, attr_name)

    #=========================单列=========================#

    @classlazyval
    def ipo_date(self):
        """股票上市日期"""
        return gen_data_set(table_name='基础信息').上市日期

    @classlazyval
    def region(self):
        """地域分类"""
        return gen_data_set(table_name='基础信息').地域

    @classlazyval
    def sw_industry(self):
        """申万一级行业"""
        return gen_data_set(table_name="行业分类").申万一级行业

    @classlazyval
    def sw_sector(self):
        """申万一级部门编码"""
        return gen_data_set(table_name="行业分类").sw_sector

    @classlazyval
    def ths_industry(self):
        """同花顺一级行业"""
        return gen_data_set(table_name="行业分类").同花顺一级行业

    @classlazyval
    def zjh_industry(self):
        """证监会一级行业"""
        return gen_data_set(table_name="行业分类").证监会一级行业

    @classlazyval
    def cn_industry(self):
        """国证一级行业"""
        return gen_data_set(table_name="行业分类").国证一级行业

    @classlazyval
    def sector_code(self):
        """部门行业分类"""
        return gen_data_set(table_name="行业分类").sector_code

    @classlazyval
    def super_sector_code(self):
        """超级部门行业分类"""
        return gen_data_set(table_name="行业分类").super_sector_code

    @classlazyval
    def short_name(self):
        """股票简称（单列）"""
        return gen_data_set(table_name='股票简称').股票简称

    #========================数据集========================#

    @classlazyval
    def concept(self):
        """股票同花顺概念【过滤器】"""
        return gen_data_set('股票概念')

    @classlazyval
    def margin(self):
        """融资融券数据集"""
        return gen_data_set(table_name='融资融券')

    @classlazyval
    def dividend(self):
        """每股股利数据集"""
        return gen_data_set(table_name='现金股利')

    @classlazyval
    def rating(self):
        """投资评级"""
        return gen_data_set(table_name='投资评级')

    @classlazyval
    def balance_sheet(self):
        """资产负债数据集"""
        return gen_data_set(table_name='资产负债表')

    @classlazyval
    def profit_statement(self):
        """利润表数据集"""
        return gen_data_set(table_name='利润表')

    @classlazyval
    def cash_flow(self):
        """现金流量表数据集"""
        return gen_data_set(table_name='现金流量表')

    # @classlazyval
    # def q_profit_statement(self):
    #     """季度利润表数据集"""
    #     return gen_data_set(table_name='quarterly_income_statements')

    # @classlazyval
    # def q_cash_flow(self):
    #     """季度现金流量表数据集"""
    #     return gen_data_set(table_name='quarterly_cash_flow_statements')

    # @classlazyval
    # def ttm_profit_statement(self):
    #     """TTM利润表数据集"""
    #     return gen_data_set(table_name='ttm_income_statements')

    # @classlazyval
    # def ttm_cash_flow(self):
    #     """TTM现金流量表数据集"""
    #     return gen_data_set(table_name='ttm_cash_flow_statements')

    # @classlazyval
    # def financial_indicators(self):
    #     """定期财务指标数据集"""
    #     return gen_data_set(table_name='periodly_financial_indicators')

    # @classlazyval
    # def q_financial_indicators(self):
    #     """季度财务指标数据集"""
    #     return gen_data_set(table_name='quarterly_financial_indicators')

    # @classlazyval
    # def financial_indicator_rankings(self):
    #     """财务指标排名数据集"""
    #     return gen_data_set(table_name='financial_indicator_rankings')

    # @classlazyval
    # def performance_forecastes(self):
    #     """业绩预告数据集"""
    #     return gen_data_set(table_name='performance_forecastes')

# # region 雅虎财经
#     @classlazyval
#     def annual_ebitda(self):
#         """年度税息折旧及摊销前利润"""
#         return gen_data_set(table_name='annual_ebitda')

#     @classlazyval
#     def annual_free_cash_flow(self):
#         """年度自由现金流"""
#         return gen_data_set(table_name='annual_free_cash_flow')

#     @classlazyval
#     def annual_total_assets(self):
#         """年度总资产"""
#         return gen_data_set(table_name='annual_total_assets')

#     @classlazyval
#     def quarterly_ebitda(self):
#         """季度税息折旧及摊销前利润"""
#         return gen_data_set(table_name='quarterly_ebitda')

#     @classlazyval
#     def quarterly_free_cash_flow(self):
#         """季度自由现金流"""
#         return gen_data_set(table_name='quarterly_free_cash_flow')

#     @classlazyval
#     def quarterly_total_assets(self):
#         """季度总资产"""
#         return gen_data_set(table_name='quarterly_total_assets')
# # endregion
