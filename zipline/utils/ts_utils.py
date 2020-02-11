"""

Timestamp辅助模块

"""
import pandas as pd
from datetime import datetime


TZ = 'Asia/Shanghai'


def ensure_utc(dt):
    """确保Timestamp转换为'UTC'时区"""
    if isinstance(dt, str):
        return pd.Timestamp(dt, tz='UTC')
    elif isinstance(dt, pd.Timestamp):
        tz = dt.tzname()
        if tz == 'UTC':
            return dt
        elif tz == 'CST':
            return dt.tz_convert('UTC')
        elif tz is None:
            return dt.tz_localize(TZ).tz_convert('UTC')
    raise TypeError(f"希望类型为str或Timestamp，输入类型{type(dt)}")
