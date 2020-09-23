"""
TEST数据包
"""

import pandas as pd
import time
from cnswd.utils import make_logger, HotDataCache
from .wy_data import (fetch_single_equity, fetch_single_quity_adjustments,
                      fetch_single_minutely_equity, gen_asset_metadata)
from . import core as bundles
from .adjusts import NON_ADJUSTED_COLUMN_FACTOR
from .utils import _exchanges

TODAY = pd.Timestamp('today').normalize()
log = make_logger('wydb', collection='zipline')


OHLCV_COLS = ['open', 'high', 'low', 'close', 'volume']
# 截取测试股票、指数代码
TEST_SIDS = [1, 2, 333, 2335, 1000002]


def _to_sid(x):
    """符号转换为sid"""
    return int(x)


def _update_splits(splits, asset_id, origin_data, start, end):
    if origin_data.empty:
        # 如为空表，直接返回，不进行任何处理
        return
    ratio = origin_data['s_ratio'] + origin_data['z_ratio']
    # 调整适应于zipline算法
    df = pd.DataFrame({
        'ratio': 1 / (1 + ratio),
        'effective_date': pd.to_datetime(origin_data['ex_date']),
        'sid': asset_id
    })
    cond = (start <= df['effective_date']) & (df['effective_date'] <= end)
    # df['ratio'] = df.ratio.astype('float')
    df = df.loc[cond, :]
    if not df.empty:
        splits.append(df)


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
    cond = (start <= df['ex_date']) & (df['ex_date'] <= end)
    df = df.loc[cond, :]
    if not df.empty:
        dividends.append(df)


def gen_symbol_data(symbol_map, sessions, splits, dividends, is_minutely):
    if not is_minutely:
        cols = OHLCV_COLS + list(NON_ADJUSTED_COLUMN_FACTOR.keys())
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
    'DTEST',
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

    # 截取测试代码
    hc = HotDataCache(gen_asset_metadata, hour=9, minute=30, only_in=False)
    metadata = hc.data
    # 资产元数据写法要求添加`sid`列
    metadata['sid'] = metadata.symbol.map(_to_sid)
    cond = metadata['sid'].isin(TEST_SIDS)
    metadata = metadata[cond]
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
    'MTEST',
    calendar_name='XSHG',
    # TODO:测试
    # start_session=pd.Timestamp('2020-06-29', tz='UTC'),
    start_session=pd.Timestamp('2020-8-12', tz='UTC'),
    minutes_per_day=240)
def cnminutely_bundle(environ, asset_db_writer, minute_bar_writer,
                      daily_bar_writer, adjustment_writer, calendar,
                      start_session, end_session, cache, show_progress,
                      output_dir):
    """Build a zipline data bundle from the cnstock dataset.
    """
    t = time.time()
    log.info('读取股票元数据......')

    # 无股指分时数据，以日线代替分钟级别数据
    hc = HotDataCache(gen_asset_metadata, hour=9, minute=30, only_in=False)
    metadata = hc.data
    metadata['sid'] = metadata.symbol.map(_to_sid)
    cond = metadata['sid'].isin(TEST_SIDS)
    metadata = metadata[cond]

    symbol_map = metadata.symbol

    sessions = calendar.sessions_in_range(start_session, end_session)

    log.info('分钟级别数据集（股票数量：{}）'.format(len(symbol_map)))

    # 写入股票元数据
    if show_progress:
        log.info('写入资产元数据')
    asset_db_writer.write(metadata, exchanges=_exchanges())

    splits = []
    dividends = []
    # 必须先写入日线数据
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

    minute_bar_writer.write(
        gen_symbol_data(symbol_map,
                        sessions,
                        splits,
                        dividends,
                        is_minutely=True),
        show_progress=show_progress,
    )

    log.info(f'完成用时：{time.time() - t:.2f}秒')
