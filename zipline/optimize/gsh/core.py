import numpy as np
import pandas as pd
import cvxpy as cvx

from .result import OptimizationResult

import logbook
log = logbook.Logger('投资组合优化')


def calculate_optimal_portfolio(objective, constraints, current_portfolio=None):
    """
    计算给定目标及限制的投资组合最优权重

    Parameters
    ----------
    objective :Objective
        将要最大化或最小化目标
    constraints ：list[Constraint]
        新投资组合必须满足的约束列表  
    current_portfolio：pd.Series, 可选
        包含当前投资组合权重的系列，以投资组合的清算价值的百分比表示。
        当从交易算法调用时，current_portfolio的默认值是算法的当前投资组合；
        当交互调用时，current_portfolio的默认值是一个空组合。

    Returns
    -------
    optimal_portfolio (pd.Series)
        当前返回的是pd.DataFrame(多头、空头、合计权重)

        包含最大化（或最小化）目标而不违反任何约束条件的投资组合权重的系列。权重应该与
        `current_portfolio`同样的方式来表达。

    Raises
    ------
    InfeasibleConstraints
        Raised when there is no possible portfolio that satisfies the received 
        constraints.
    UnboundedObjective
        Raised when the received constraints are not sufficient to put an upper 
        (or lower) bound on the calculated portfolio weights.

    Notes

    This function is a shorthand for calling run_optimization, checking for an error, 
    and extracting the result’s new_weights attribute.

    If an optimization problem is feasible, the following are equivalent:

    >>> # Using calculate_optimal_portfolio.
    >>> weights = calculate_optimal_portfolio(objective, constraints, portfolio)
    >>> # Using run_optimization.
    >>> result = run_optimization(objective, constraints, portfolio)
    >>> result.raise_for_status()  # Raises if the optimization failed.
    >>> weights = result.new_weights

    See also
    ---------
    zipline.optimize.run_optimization()
    """
    result = run_optimization(objective, constraints, current_portfolio)
    result.raise_for_status()
    return result.new_weights


def run_optimization(objective, constraints, current_portfolio=None):
    """
    运行投资组合优化

    Parameters
    ----------
    objective :Objective
        将要最大化或最小化目标
    constraints ：list[Constraint])
        新投资组合必须满足的约束列表      
    current_portfolio：pd.Series, 可选
        包含当前投资组合权重的系列，以投资组合的清算价值的百分比表示。
        当从交易算法调用时，current_portfolio的默认值是算法的当前投资组合；
        当交互调用时，current_portfolio的默认值是一个空组合。

    Returns
    -------
    result：zipline.optimize.OptimizationResult
        包含有关优化结果信息的对象

    See also
    --------
    zipline.optimize.OptimizationResult
    zipline.optimize.calculate_optimal_portfolio()   
    """
    assert isinstance(constraints, list), 'constraints应该为列表类型'
    
    prob, w_plus, labels = _run(
        objective, 
        constraints, 
        current_portfolio,
        solver=cvx.ECOS
    )
    return OptimizationResult(
        prob, 
        w_plus,
        labels,
        current_portfolio,
    )


def _run(objective, constraints, current_weights, **solver_opts):
    labels = objective.labels
    
    if isinstance(current_weights, pd.Series):
        holding_labels = current_weights.index
        labels = labels.union(holding_labels)
    
        w = current_weights.reindex(labels, fill_value=0).values
        z = cvx.Variable(len(labels))
        w_plus = w + z
    elif current_weights is None:
        w_plus = z = cvx.Variable(len(labels))
    else:
        raise Exception('current_weights must be pandas.Series or None')
        
    cvx_obj = cvx.Maximize(objective.weight_expr(w_plus, z, labels))

    cvx_con = []
    for constraint in constraints:
        for item in constraint.weight_expr(w_plus, z, labels):
            cvx_con.append(item)
            assert item.is_dcp(), '{} does not follow DCP rules'.format(constraint)

    cvx_prob = cvx.Problem(cvx_obj, cvx_con)
    
    cvx_prob.solve(**solver_opts)

    return cvx_prob, w_plus, labels