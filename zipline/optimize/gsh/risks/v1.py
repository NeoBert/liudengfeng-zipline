from zipline.utils.memoize import remember_last
from zipline.pipeline.risks.v1 import (
    SECTORS,
    STYLES,    
    returns_pipeline,        
    risk_loading_pipeline,
)
from zipline.research.pipebench import run_pipeline
from zipline.research.utils import select_output_by

from zipline.pipeline.factors.sector.v1 import gen_index_return

import numpy as np
import pandas as pd


def get_factor_loadings(start, end, assets=None):
    ret = _get_factor_loadings(start, end)
    
    if assets:
        ret = select_output_by(ret, assets=assets)
    
    return ret


def get_factor_returns(start, end):
    # Sector returns
    sector_returns = gen_index_return(start, end, 1)
    sector_returns.columns = SECTORS
    
    # Stock returns
    returns = get_returns(start, end)
    
    factor_loadings = _get_factor_loadings(start, end)
    
    # Residual returns
    residual_returns = pd.DataFrame(columns=returns.columns)
    for asset in returns:
        loadings = select_output_by(factor_loadings[SECTORS], assets=asset)
        residual_returns[asset] = (
            returns[asset] - (loadings * sector_returns).sum(axis=1)
        )
        
    # Style factor returns
    factor_returns = pd.DataFrame(columns=STYLES)
    for t, s in residual_returns.iterrows():
        s = pd.DataFrame(s.dropna())
        loadings = select_output_by(factor_loadings[STYLES], t, t).dropna()
        
        df = pd.merge(s, loadings, left_index=True, right_index=True)
        
        y = df.iloc[:, 0].values
        X = df.iloc[:, 1:].values
        
        f = np.linalg.inv(X.T @ X) @ X.T @ y
        
        factor_returns.loc[t] = f

    return pd.merge(sector_returns, factor_returns, left_index=True, right_index=True)


def get_returns(start, end):
    ret = _get_returns(start, end)
    n = int(len(ret) * 0.75)
    
    return ret.dropna(axis=1, thresh=n)


@remember_last    
def _get_factor_loadings(start, end):
    pipeline = risk_loading_pipeline()
    return run_pipeline(pipeline, start, end)


@remember_last    
def _get_returns(start, end):
    pipeline = returns_pipeline()
    ret = run_pipeline(pipeline, start, end)
    return ret.iloc[:, 0].unstack(1)