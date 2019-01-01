try:
    from .reader import Fundamentals
except:
    msg = '请运行`zipline sql-to-bcolz`，完成数据整理'
    raise NotImplementedError(msg)