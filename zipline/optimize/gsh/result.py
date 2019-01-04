# coding=utf8
from .exceptions import (
    UnboundedObjective,
    InfeasibleConstraints,
    OptimizationFailed,
)
import pandas as pd


class OptimizationResult(object):
    def __init__(self, problem, w_plus, labels, current_portfolio):
        self.problem = problem
        
        self.status = self.problem.status
        self.success = self.status == 'optimal'
        
        if not self.success:
            self.new_weights = None
        else:
            self.new_weights = pd.Series(w_plus.value, index=labels)

        self.old_weights = current_portfolio
    
    @property
    def trade_weights(self):
        if self.new_weights is None:
            return None
        elif self.old_weights is None:
            return self.new_weights
        else:
            return (self.new_weights - self.old_weights).dropna()
        
    def raise_for_status(self):
        if self.status == 'unbounded':
            info = 'The problem is unbounded. Defaulting to no trades'
            raise UnboundedObjective(info)
        elif self.status == 'infeasible':
            info = 'The problem is infeasible. Defaulting to no trades'
            raise InfeasibleConstraints(info)