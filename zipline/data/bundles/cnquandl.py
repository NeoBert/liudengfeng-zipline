"""
构造股票日线数据集

备注：
    1. 如使用用int(stock_code)代表sid，必须在写入资产元数据时，提供sid列
    2. 默认只写入A股，且在市的股票数据
    3. 保持一致性，只需要OHKCV列
    4. 由于数据期间不一致，如601607分红派息自2000年开始，而日线数据自2010年开始，导致无法计算调整系数，
       属正常。
    5. 成交量数值可能超出int32，写入时除100，读取时乘以100，部分损失精度。
"""

import pandas as pd
import time
from cnswd.utils import make_logger, HotDataCache
from ..localdata import (fetch_single_equity, fetch_single_quity_adjustments,
                         fetch_single_minutely_equity, gen_asset_metadata)
from . import core as bundles
from .adjusts import ADJUST_FACTOR
from .refresh import CALENDAR_START

TODAY = pd.Timestamp('today').normalize()
log = make_logger('cnquandl', collection='zipline')


OHLCV_COLS = ['open', 'high', 'low', 'close', 'volume']


def _exchanges():
    # 通过 `股票.exchange = exchanges.exchange`来关联
    # 深证信 股票信息 上市地点
    return pd.DataFrame({
        'exchange': ['深交所主板', '上交所', '深交所中小板', '深交所创业板', '上交所科创板', '指数'],
        'canonical_name': ['XSHE', 'XSHG', 'XSHE', 'XSHE', 'XSHG', 'XSHG'],
        'country_code': ['CN'] * 6
    })


def _to_sid(x):
    """符号转换为sid"""
    return int(x)


def _update_splits(splits, asset_id, origin_data, start, end):
    if origin_data.empty:
        # 如为空表，直接返回，不进行任何处理
        return
    ratio = origin_data['s_ratio'] + origin_data['z_ratio']
    # 调整适应于zipline算法
    # date -> datetime64[ns]
    df = pd.DataFrame({
        'ratio': 1 / (1 + ratio),
        'effective_date': pd.to_datetime(origin_data.ex_date),
        'sid': asset_id
    })
    cond = (start <= df['effective_date']) & (df['effective_date'] <= end)
    # df['ratio'] = df.ratio.astype('float')
    splits.append(df.loc[cond, :])


def _update_dividends(dividends, asset_id, origin_data, start, end):
    if origin_data.empty:
        return
    # date -> datetime64[ns]
    df = pd.DataFrame({
        'record_date': pd.NaT,
        # pd.to_datetime(origin_data['record_date']),
        'ex_date':
        pd.to_datetime(origin_data['ex_date']),
        'declared_date': pd.NaT,
        # pd.to_datetime(origin_data['declared_date']),
        'pay_date': pd.NaT,
        # pd.to_datetime(origin_data['pay_date']),
        'amount':
        origin_data['amount'],
        'sid':
        asset_id
    })
    cond = (start <= df['pay_date']) & (df['pay_date'] <= end)
    dividends.append(df.loc[cond, :])


def gen_symbol_data(symbol_map, sessions, splits, dividends, is_minutely):
    if not is_minutely:
        cols = OHLCV_COLS + list(ADJUST_FACTOR.keys())
    else:
        cols = OHLCV_COLS
    start, end = sessions[0], sessions[-1]
    start, end = start.tz_localize(None), end.tz_localize(None)
    for _, symbol in symbol_map.iteritems():
        asset_id = _to_sid(symbol)
        if not is_minutely:
            raw_data = fetch_single_equity(
                symbol,
                start=sessions[0],
                end=sessions[-1],
            )
            # 新股可能存在日线延迟，会触发异常
            if not raw_data.empty:
                # 🆗 除去调整
                # raw_data['volume'] = raw_data['volume'] / 100.0

                # 以日期、符号为索引
                raw_data.set_index(['date', 'symbol'], inplace=True)
                raw_data = raw_data.loc[:, cols]

                # 时区调整，以0.0填充na
                # 转换为以日期为索引的表(与sessions保持一致)
                asset_data = raw_data.xs(symbol, level=1).reindex(
                    sessions.tz_localize(None)).fillna(0.0)
            else:
                asset_data = raw_data
        else:
            # 处理分钟级别数据
            asset_data = fetch_single_minutely_equity(
                symbol,
                start=sessions[0],
                end=sessions[-1],
            ).tz_localize('Asia/Shanghai').tz_convert('utc')

        # 顺带处理分红派息
        # 获取原始调整数据
        raw_adjustment = fetch_single_quity_adjustments(symbol,
                                                        start=sessions[0],
                                                        end=sessions[-1])
        # 当非空时才执行
        if not raw_adjustment.empty:
            # 剔除未来事件
            raw_adjustment = raw_adjustment[raw_adjustment.ex_date <= TODAY]
            # 更新送转
            # 送转比率大于0才有意义
            ratio = raw_adjustment.s_ratio + raw_adjustment.z_ratio
            raw_splits = raw_adjustment.loc[ratio > 0.0, :]
            _update_splits(splits, asset_id, raw_splits, start, end)

            # 更新股利
            raw_dividends = raw_adjustment.loc[raw_adjustment.amount > 0.0, :]
            _update_dividends(dividends, asset_id, raw_dividends, start, end)
        yield asset_id, asset_data


@bundles.register(
    'cndaily',
    calendar_name='XSHG',
    minutes_per_day=240)
def cndaily_bundle(environ, asset_db_writer, minute_bar_writer,
                   daily_bar_writer, adjustment_writer, calendar,
                   start_session, end_session, cache, show_progress,
                   output_dir):
    """Build a zipline data bundle from the cnstock dataset.
    """
    t = time.time()
    log.info('读取股票元数据......')
    # metadata = gen_asset_metadata(False)
    hc = HotDataCache(gen_asset_metadata, hour=9, minute=30, only_in=False)
    metadata = hc.data
    # 资产元数据写法要求添加`sid`列
    metadata['sid'] = metadata.symbol.map(_to_sid)
    symbol_map = metadata.symbol
    sessions = calendar.sessions_in_range(start_session, end_session)

    log.info('日线数据集（股票数量：{}）'.format(len(symbol_map)))

    # 写入股票元数据
    if show_progress:
        log.info('写入资产元数据')
    asset_db_writer.write(metadata, exchanges=_exchanges())

    splits = []
    dividends = []
    daily_bar_writer.write(
        gen_symbol_data(symbol_map,
                        sessions,
                        splits,
                        dividends,
                        is_minutely=False),
        show_progress=show_progress,
        has_additional_cols=True,
    )

    adjustment_writer.write(
        splits=None if len(splits) == 0 else pd.concat(splits,
                                                       ignore_index=True),
        dividends=None
        if len(dividends) == 0 else pd.concat(dividends, ignore_index=True),
    )
    log.info(f'完成用时：{time.time() - t:.2f}秒')


@bundles.register(
    'cnminutely',
    calendar_name='XSHG',
    start_session=CALENDAR_START,
    minutes_per_day=240)
def cnminutely_bundle(environ, asset_db_writer, minute_bar_writer,
                      daily_bar_writer, adjustment_writer, calendar,
                      start_session, end_session, cache, show_progress,
                      output_dir):
    """Build a zipline data bundle from the cnstock dataset.
    """
    t = time.time()
    log.info('读取股票元数据......')
    # 只保留000002A股指数，且日内设定为常数
    hc = HotDataCache(gen_asset_metadata, hour=9, minute=30, only_in=False)
    metadata = hc.data
    log.info("分钟级别数据，固定使用`000002【A股指数】`日线数据作为基准收益率")
    cond = metadata.symbol.str.len() == 6
    metadata = pd.concat(
        [metadata[metadata.symbol == '1000002'], metadata[cond]])
    # 测试切片
    # metadata = metadata.iloc[:20, :]

    metadata['sid'] = metadata.symbol.map(_to_sid)
    symbol_map = metadata.symbol

    sessions = calendar.sessions_in_range(start_session, end_session)

    log.info('分钟级别数据集（股票数量：{}）'.format(len(symbol_map)))

    # 写入股票元数据
    if show_progress:
        log.info('写入资产元数据')
    asset_db_writer.write(metadata, exchanges=_exchanges())

    splits = []
    dividends = []
    minute_bar_writer.write(
        gen_symbol_data(symbol_map,
                        sessions,
                        splits,
                        dividends,
                        is_minutely=True),
        show_progress=show_progress,
    )

    adjustment_writer.write(
        splits=None if len(splits) == 0 else pd.concat(splits,
                                                       ignore_index=True),
        dividends=None
        if len(dividends) == 0 else pd.concat(dividends, ignore_index=True),
    )
    log.info(f'完成用时：{time.time() - t:.2f}秒')
