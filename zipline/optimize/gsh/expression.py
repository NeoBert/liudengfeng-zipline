import logbook
import cvxpy as cvx
import numpy as np
import pandas as pd

from ..expression import Expression
from ..utils import get_ix


logger = logbook.Logger('组合优化风险模型')


class RiskModel(Expression):
    pass


class StaticCovar(RiskModel):
    def __init__(self, cov=None, corr=None, stdev=None):
        if cov is None:
            assert isinstance(corr, pd.DataFrame)
            assert isinstance(stdev, pd.Series)
            assert all(corr.index.values == corr.columns.values)
            assert all(stdev.index.values == corr.columns.values)
            
            stocks = stdev.index
            stdev = stdev.values
            corr = corr.values
            cov = np.diag(stdev) @ corr @ np.diag(stdev)
            cov = pd.DataFrame(cov, index=stocks, columns=stocks)

        self.cov = cov
        assert all(cov.index.values == cov.columns.values)
        assert (cov.notna().all(None))
            
        super(StaticCovar, self).__init__()

    @property
    def labels(self):
        return self.cov.columns
    
    def weight_expr(self, w_plus, z, l):
        ix1, ix2 = get_ix(l, self.labels)
        
        return cvx.quad_form(
            w_plus[ix1], self.cov.iloc[ix2, ix2].values
        )


class EmpyricalCovar(StaticCovar):
    """Empirical Sigma matrix, built looking at *lookback* past returns.
    """
    def __init__(self, returns, lookback=None):
        """returns is dataframe, lookback is int"""
        if lookback is not None:
            assert lookback <= len(returns), 'lookback windows is larger than the returns series'
            returns = returns.sort_index().iloc[-lookback:]
        cov = returns.cov()
        super(EmpyricalCovar, self).__init__(cov)


class FactorModelCovar(StaticCovar):
    def __init__(self, exposures, factor_covar, idiosync):
        """
        exposures: N x K, (index->stock, columns->factor)
        factor_covar: K x K, (index, columns->factor)
        idiosync: N x N, (index, columns->stock)
        """
        assert (exposures.notna().all(None))
        assert (factor_covar.notna().all(None))
        assert (idiosync.notna().all(None))
        
        assert all(exposures.index.values == idiosync.columns.values)
        assert all(factor_covar.index.values == exposures.columns.values)
        assert all(idiosync.index.values == idiosync.columns.values)
        assert all(factor_covar.index.values == factor_covar.columns.values)
        
        self.exposures = exposures
        self.factor_covar = factor_covar
        self.idiosync = idiosync
        
        cov = np.dot(np.dot(exposures.values, factor_covar.values),
                       exposures.T.values) + idiosync.values
        
        cov = pd.DataFrame(data=cov, 
                           index=self.exposures.index,
                           columns=self.exposures.index)
        
        super(FactorModelCovar, self).__init__(cov)