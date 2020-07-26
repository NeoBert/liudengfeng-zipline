"""

雅虎财经

TODO:待完成
"""
from cnswd.mongodb import get_db
import pandas as pd


def get_ttm_valuation_measures():
    """TTM估值指标"""
    db = get_db('yahoo')
    collection = db['valuation_measures']
    pipeline = [
        {
            '$match': {
                '期间类型': 'TTM',
            }
        },
        {
            '$project': {'_id': 0, '期间类型': 0}
        }
    ]
    ds = collection.aggregate(pipeline)
    df = pd.DataFrame.from_records(ds)
    # df.drop(['期间类型'], axis=1, inplace=True)
    df.rename(columns={'符号': 'sid', '截至日期': 'asof_date'}, inplace=True)
    df['sid'] = df['sid'].map(lambda x: int(x))
    return df
