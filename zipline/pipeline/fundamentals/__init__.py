import warnings
# try:
#     from .reader_cninfo import Fundamentals as CNINFO
# except:
#     msg = '请运行`zipline fm -b cninfo`，完成数据整理'
#     warnings.warn(msg)

try:
    from .reader_wy import Fundamentals
except:
    msg = '请运行`zipline fm -b wy`，完成数据整理'
    warnings.warn(msg)
