# from cnswd.sql.base import session_scope
# import pandas as pd
# from cnswd.sql.base import get_engine
# e = get_engine('szx')
# f = r'C:\\Users\\ldf\\quotes.csv'
# reader = pd.read_csv(f, chunksize=1024)
# for chunk in reader:
#     df = chunk.copy()
#     df['股票代码'] = df['股票代码'].map(lambda x:str(x).zfill(6))
#     df.to_sql('quotes',e,if_exists='append', index=False)
# print()
