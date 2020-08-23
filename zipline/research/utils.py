"""
辅助函数
"""
import warnings

from cnswd.utils import ensure_list, sanitize_dates

from .core import symbols, trading_sessions


def select_output_by(output, start=None, end=None, assets=None, reduce_format=True):
    """
    按时间及代码选择`pipeline`输出数据框

    专用于研究环境下的run_pipeline输出结果分析

    参数
    ----
    output : MultiIndex DataFrame
        pipeline输出结果
    start ： str
        开始时间
    end ： str
        结束时间    
    assets ： 可迭代对象或str
        股票代码

    案例
    ----  
    >>> # result 为运行`pipeline`输出结果 
    >>> select_output_by(result,'2018-04-23','2018-04-24',assets=['000585','600871'])

                                                  mean_10
    2018-04-23 00:00:00+00:00 	*ST东电(000585) 	2.7900
                                *ST油服(600871) 	2.0316
    2018-04-24 00:00:00+00:00 	*ST东电(000585) 	2.7620
                                *ST油服(600871) 	2.0316    
    """
    nlevels = output.index.nlevels
    if nlevels != 2:
        raise ValueError('输入数据框只能是run_pipeline输出结果，MultiIndex DataFrame')
    start, end = sanitize_dates(start, end)
    sessions = trading_sessions(start, end)
    start, end = sessions[0], sessions[-1]
    if assets is not None:
        assets = symbols(assets)
    else:
        assets = []

    ret = _select_output_by(output, start, end, assets)

    if reduce_format:
        cond1 = start == end
        cond2 = len(assets) == 1
        if cond1 & cond2:
            ret = ret.xs((start, assets[0]))
        elif cond1:
            ret = ret.xs(start, level=0)
        elif cond2:
            ret = ret.xs(assets[0], level=1)

    return ret


def _select_output_by(output, start, end, assets=[]):
    ret = output.loc[start:end]
    if len(assets):
        ret = ret.loc[(slice(None), assets), :]
    return ret
