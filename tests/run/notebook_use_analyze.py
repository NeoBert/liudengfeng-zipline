# %%
%load_ext zipline
import matplotlib.pyplot as plt
from zipline.api import order_target, record, symbol

# %%
# %%zipline --start 2016-1-1 --end 2018-1-1
# from zipline.api import symbol, order, record

# def initialize(context):
#     pass

# def handle_data(context, data):
#     order(symbol('000333'), 10)
#     record(MDJT=data[symbol('000333')].price)

# %%
# 默认输出到标准输出
# _.head()

# %% 分钟级别
% % zipline - -start 2020-1-2 - -end 2020-1-8 - -data-frequency minute - b cnminutely - o dma.pickle


def initialize(context):
    context.i = 0
    context.asset = symbol('000333')


def handle_data(context, data):
    # Skip first 60 mins to get full windows
    context.i += 1
    if context.i < 60:
        return

    # Compute averages
    # data.history() has to be called with the same params
    # from above and returns a pandas dataframe.
    short_mavg = data.history(context.asset, 'price',
                              bar_count=30, frequency="1m").mean()
    long_mavg = data.history(context.asset, 'price',
                             bar_count=60, frequency="1m").mean()

    # Trading logic
    if short_mavg > long_mavg:
        # order_target orders as many shares as needed to
        # achieve the desired number of shares.
        order_target(context.asset, 100)
    elif short_mavg < long_mavg:
        order_target(context.asset, 0)

    # Save values for later inspection
    record(MDJT=data.current(context.asset, 'price'),
           short_mavg=short_mavg,
           long_mavg=long_mavg)


def analyze(context, perf):
    print('总计数', context.i)  # 应为 1210 = 5 * 242 五个交易日 242 bar / per day
    fig = plt.figure()
    ax1 = fig.add_subplot(211)
    perf.portfolio_value.plot(ax=ax1)
    ax1.set_ylabel('portfolio value in $')

    ax2 = fig.add_subplot(212)
    perf['MDJT'].plot(ax=ax2)
    perf[['short_mavg', 'long_mavg']].plot(ax=ax2)

    perf_trans = perf.loc[[t != [] for t in perf.transactions], :]
    buys = perf_trans.loc[[t[0]['amount'] >
                           0 for t in perf_trans.transactions], :]
    sells = perf_trans.loc[
        [t[0]['amount'] < 0 for t in perf_trans.transactions], :]
    ax2.plot(buys.index, perf.short_mavg.loc[buys.index, :],
             '^', markersize=10, color='m')
    ax2.plot(sells.index, perf.short_mavg.loc[sells.index, :],
             'v', markersize=10, color='k')
    ax2.set_ylabel('price in $')
    plt.legend(loc=0)
    plt.show()
