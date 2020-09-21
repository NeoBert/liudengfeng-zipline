import pandas as pd


def _exchanges():
    # 通过 `股票.exchange = exchanges.exchange`来关联
    # 深证信 股票信息 上市地点
    return pd.DataFrame({
        'exchange': ['深交所主板', '上交所', '深交所中小板', '深交所创业板', '上交所科创板', '深证B股', '上海B股', '指数'],
        'canonical_name': ['XSHE', 'XSHG', 'XSHE', 'XSHE', 'XSHG', 'XSHE', 'XSHG', 'XSHG'],
        'country_code': ['CN'] * 8
    })
