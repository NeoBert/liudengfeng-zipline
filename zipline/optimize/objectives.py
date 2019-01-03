"""
目标模块

假设前提：
    1. 无论是目标权重或当前权重，在特定时刻，单个资产不得同时存在多头与空头权重；
"""

import abc
import logbook

import numpy as np
import pandas as pd
import cvxpy as cvx

from six import with_metaclass

from .utils import ensure_series


logger = logbook.Logger('投资组合优化目标')


class BaseObjective(with_metaclass(abc.ABCMeta, object)):
    """This is a base class and should not be used directly.
    """
    def __init__(self):
        self._make_variable()

    def _make_variable(self, current_weights=None):
        '''current_weights：pd.Series[Asset -> float] or dict[Asset -> float]
        '''
        if current_weights is None:
            self._new_index = self._old_index
        else:
            current_weights = ensure_series(current_weights)
            self._new_index = self._old_index.union(current_weights.index).unique()
        
        self._nvar = len(self._new_index)
        assert self._nvar > 0, 'Obejctive should have included >= 1 asset!'
        
        self._new_weights = cvx.Variable(self._nvar, name='new_weights')

    def __repr__(self):  
        return "%s(#var=%s)" % (self.__class__.__name__, self._nvar)

    @property
    def new_weights(self):
        """
        权重表达式
        每调用self._make_variable后自动修改
        """
        return self._new_weights

    @property
    def new_weights_series(self):
        """
        权重表达式序列
        每调用self._make_variable后，调用则修改
        """
        return pd.Series([self.new_weights[i] for i in range(self._nvar)], 
                        index=self._new_index)

    @property
    def new_weights_value(self, d=6):
        """
        权重值
        未进行优化求解返回空值
        """
        ret = None
    
        after_optimization = self.new_weights[0].value
        if after_optimization:
            ret = self.new_weights_series.apply(lambda x: round(x.value, d))
        
        return ret
  
    
    @abc.abstractmethod
    def to_cvxpy(self, current_weights):
        raise NotImplementedError()


class TargetWeights(BaseObjective):
    """
    与投资组合目标权重最小距离的BaseObjective

    Parameters
    ----------
    weights：pd.Series[Asset -> float] or dict[Asset -> float]
        资产目标权重(或资产目标权重映射)

    Notes
    -----
    一个目标值为1.0表示投资组合的当前净清算价值的100％在相应资产中应保持多头。

    一个目标值为-1.0表示投资组合的当前净清算价值的100％在相应资产中应保持空头。

    目标值为0.0的资产将被忽略，除非算法在给定资产中已有头寸。

    如果算法在资产中已有头寸，且没有提供目标权重，那么目标权重被假设为0.
    """

    def __init__(self, weights):
        self._target_weights = ensure_series(weights)
        self._target_weights.fillna(0.0, inplace=True) # 因子值不得为Nan
        
        self._old_index = self._target_weights.index
        super(TargetWeights, self).__init__()

    def to_cvxpy(self, current_weights=None):
        '''current_weights：pd.Series[Asset -> float] or dict[Asset -> float]
        '''             
        self._make_variable(current_weights)
        self._target_weights = self._target_weights.reindex(self._new_index).fillna(0)
        err = cvx.sum_squares(self.new_weights - self._target_weights.values)
        return cvx.Minimize(err)


class MaximizeAlpha(BaseObjective):
    """
    对于一个alpha向量最大化weights.dot(alphas)的目标对象

    理想情况下，资产alpha系数应使得期望值与目标投资组合持有的时间成正比。
    
    特殊情况下，alphas只是每项资产的期望收益估计值，这个目标只是最大化整个投资
    组合的预期收益。

    Parameters
    ----------
    alphas ： pd.Series[Asset -> float] 或 dict[Asset -> float]
        资产与这些资产的α系数之间的映射

    Notes
    ------

    这个目标总是与`MaxGrossExposure`约束一起使用，并且通常应该与`PositionConcentration`
    约束一起使用。

    如果没有对总风险的限制，这个目标会产生一个错误，试图为每个非零的alphas系数的资产分配无限的资本。

    如果没有对单个头寸大小的限制，这个目标将分配所有的头寸到预期收益最大的单项资产中。
    """

    def __init__(self, alphas):
        self._alphas = ensure_series(alphas)
        self._alphas.fillna(0.0, inplace=True) # 因子值不得为Nan
        
        self._old_index = self._alphas.index
        super(MaximizeAlpha, self).__init__()

    def to_cvxpy(self, current_weights=None):
        '''current_weights：pd.Series[Asset -> float] or dict[Asset -> float]
        '''        
        self._make_variable(current_weights)
        self._alphas = self._alphas.reindex(self._new_index).fillna(0)
        profit = self._alphas.values.T * self.new_weights  # 加权收益
        return cvx.Maximize(cvx.sum(profit))
