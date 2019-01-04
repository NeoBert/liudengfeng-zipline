# coding=utf8
import logbook
import cvxpy as cvx

from .expression import Expression
from .utils import ensure_series, get_ix
from .risks import RiskModel

logger = logbook.Logger('组合优化目标')


class Objective(Expression):
    pass
        

class TargetWeights(Objective):
    '''
    Objective that minimizes the distance from an already-computed portfolio.

    Parameters:
    weights (pd.Series[Asset -> float] or dict[Asset -> float]) 
    – Map from asset to target percentage of holdings.

    Notes
    A target value of 1.0 indicates that 100% of the portfolio’s 
    current net liquidation value should be held in a long position in the corresponding asset.

    A target value of -1.0 indicates that -100% of the portfolio’s 
    current net liquidation value should be held in a short position in the corresponding asset.

    Assets with target values of exactly 0.0 are ignored 
    unless an algorithm has an existing position in the given asset.

    If an algorithm has an existing position in an asset 
    and no target weight is provided, the target weight is assumed to be zero.
    '''
    def __init__(self, weights):
        self.weights = ensure_series(weights).dropna()
        
        super(TargetWeights, self).__init__()

    @property
    def labels(self):
        return self.weights.index
    
    def weight_expr(self, w_plus, z, l):
        # Default is for maximization of a problem
        ix1, ix2 = get_ix(l, self.labels)
        
        return -cvx.norm(
            self.weights.values[ix2] - w_plus[ix1], 1
        )
    
    
class MaximizeAlpha(Objective):
    '''
    Objective that maximizes weights.dot(alphas) for an alpha vector.
    Ideally, alphas should contain coefficients such that alphas[asset] 
    is proportional to the expected return of asset for the time horizon 
    over which the target portfolio will be held.
    In the special case that alphas is an estimate of expected returns for each asset, 
    this objective simply maximizes the expected return of the total portfolio.
    
    Parameters:	
    alphas (pd.Series[Asset -> float] or dict[Asset -> float]) 
    – Map from assets to alpha coefficients for those assets.
    
    Notes:
    This objective should almost always be used with a MaxGrossExposure constraint, 
    and should usually be used with a PositionConcentration constraint.
    
    Without a constraint on gross exposure, 
    this objective will raise an error attempting to allocate an unbounded
    amount of capital to every asset with a nonzero alpha.
    
    Without a constraint on individual position size, 
    this objective will allocate all of its capital in the single
    asset with the largest expected return.
    '''
    def __init__(self, alphas):        
        self.alphas = ensure_series(alphas).dropna()
        
        super(MaximizeAlpha, self).__init__()
    
    @property
    def labels(self):
        return self.alphas.index
        
    def weight_expr(self, w_plus, z, l):
        # Default is for maximization of a problem     
        ix1, ix2 = get_ix(l, self.labels)
        
        return cvx.sum(cvx.multiply(
            self.alphas.values[ix2], w_plus[ix1]
        ))


class QuadraticUtility(Objective):
    def __init__(self, alpha_model=None, risk_model=None, lambda_=0.01):
        if alpha_model is None and risk_model is None:
            assert False
        if alpha_model is not None:
            assert isinstance(alpha_model, MaximizeAlpha)
        if risk_model is not None:
            assert isinstance(risk_model, RiskModel)
            assert lambda_ > 0

        self.alpha_model = alpha_model
        self.risk_model = risk_model
        self.lambda_ = lambda_
    
    @property
    def labels(self):
        if self.alpha_model is None:
            return self.risk_model.labels
        elif self.risk_model is None:
            return self.alpha_model.labels
        else:       
            return self.alpha_model.labels.intersection(
                self.risk_model.labels)      
       
    def weight_expr(self, w_plus, z, l):
        # Default is for maximization of a problem
        if self.alpha_model is None:
            return -self.risk_model.weight_expr(w_plus, z, l)
        elif self.risk_model is None:
            return self.alpha_model.weight_expr(w_plus, z, l)
        else:
            return self.alpha_model.weight_expr(w_plus, z, l) - \
               self.lambda_ * self.risk_model.weight_expr(w_plus, z, l)