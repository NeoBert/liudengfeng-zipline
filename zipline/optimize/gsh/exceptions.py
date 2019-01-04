# coding=utf8


class InfeasibleConstraints(Exception):
    """

    Raised when an optimization fails because there are no valid portfolios.

    This most commonly happens when the weight in some asset is simultaneously 
    constrained to be above and below some threshold.
    """
    def __init__(self, info):
        self.info = info
    
    def __str__(self):
        return self.info


class UnboundedObjective(Exception):
    """

    Raised when an optimization fails because at least one weight in the ‘optimal’ 
    portfolio is ‘infinity’.

    More formally, raised when an optimization fails because the value of an 
    objective function improves as a value being optimized grows toward infinity, 
    and no constraint puts a bound on the magnitude of that value.
    """
    def __init__(self, info):
        self.info = info
    
    def __str__(self):
        return self.info


class OptimizationFailed(Exception):
    """
    Generic exception raised when an optimization fails a reason with no special 
    metadata.
    """
    def __init__(self, info):
        self.info = info
    
    def __str__(self):
        return self.info