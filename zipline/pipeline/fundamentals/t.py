from zipline.pipeline.fundamentals.wy_class import get_sw_industry
import pandas as pd

df = get_sw_industry()


from zipline.pipeline.fundamentals.ctable import get_ctable
df = get_ctable('sw_industry','wy')
df['asof_date']


import numpy as np
a = np.array(['a','',None])
a != None


s = pd.Series(['a','',None])
s.astype('cate')

s = pd.Series(["a", "b", None], dtype="category")
s.cat.codes

a = ["a", "b", None]
c = filter(lambda x: x is not None, a)
pd.Series(c, dtype="category")