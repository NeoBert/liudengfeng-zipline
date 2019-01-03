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
from logbook import Logger

from . import core as bundles
# from ..constants import ADJUST_FACTOR, TEST_SYMBOLS, OHLC
from ..sqldata import (fetch_single_equity, fetch_single_quity_adjustments,
                       gen_asset_metadata, fetch_single_minutely_equity, OHLCV_COLS)

TODAY = pd.Timestamp('today').normalize()
log = Logger('数据集')


def _exchanges():
    # 通过 `股票.exchange = exchanges.exchange`来关联
    return pd.DataFrame(
        {
            'exchange': ['深交所主板', '上交所', '深交所中小板', '深交所创业板'],
            'canonical_name': ['XSHG', 'XSHG', 'XSHG', 'XSHG'],
            'country_code': ['CN'] * 4
        }
    )


def _to_sid(x):
    """符号转换为sid"""
    return int(x)


def _to_symbol(x):
    """sid转换为符号"""
    return str(x).zfill(6)


def _adjusted_raw_data(raw_df):
    """调整原始数据单位，转换适用于unit32类型的数值"""
    data = raw_df.copy()
    # 增强兼容性
    for col in data.columns:
        if col in ADJUST_FACTOR.keys():
            adj = ADJUST_FACTOR.get(col, 1)
            data.loc[:, col] = data[col] * adj
    return data


def _update_splits(splits, asset_id, origin_data):
    if origin_data.empty:
        # 如为空表，直接返回，不进行任何处理
        return
    ratio = origin_data['s_ratio'] + origin_data['z_ratio']
    # 调整适应于zipline算法
    # date -> datetime64[ns]
    df = pd.DataFrame({'ratio': 1 / (1 + ratio),
                       'effective_date': pd.to_datetime(origin_data.ex_date),
                       'sid': asset_id})
    # df['ratio'] = df.ratio.astype('float')
    splits.append(df)


def _update_dividends(dividends, asset_id, origin_data):
    if origin_data.empty:
        return
    # date -> datetime64[ns]
    df = pd.DataFrame({'record_date': pd.to_datetime(origin_data['record_date']),
                       'ex_date': pd.to_datetime(origin_data['ex_date']),
                       'declared_date': pd.to_datetime(origin_data['declared_date']),
                       'pay_date': pd.to_datetime(origin_data['pay_date']),
                       'amount': origin_data['amount'],
                       'sid': asset_id})
    dividends.append(df)


def gen_symbol_data(symbol_map,
                    sessions,
                    splits,
                    dividends,
                    is_minutely):
    for _, symbol in symbol_map.iteritems():
        asset_id = _to_sid(symbol)
        if not is_minutely:
            # 日线原始数据，只需要OHLCV列
            raw_data = fetch_single_equity(
                symbol,
                start=sessions[0],
                end=sessions[-1],
            )
            # # 不得包含涨跌幅列，含有正负号
            # raw_data.drop('change_pct', axis=1, inplace=True)
            # # 百分比调整为小数
            # raw_data['turnover'] = raw_data.turnover / 100.

            # 调整数据精度
            raw_data['volume'] = raw_data['volume'] / 100.0

            # 以日期、符号为索引
            raw_data.set_index(['date', 'symbol'], inplace=True)

            # 只需要ohlcv列
            raw_data = raw_data[OHLCV_COLS]

            # 时区调整，以0.0填充na
            # 转换为以日期为索引的表(与sessions保持一致)
            asset_data = raw_data.xs(
                symbol,
                level=1
            ).reindex(
                sessions.tz_localize(None)
            ).fillna(0.0)
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
            _update_splits(splits, asset_id, raw_splits)

            # 更新股利
            raw_dividends = raw_adjustment.loc[raw_adjustment.amount > 0.0, :]
            _update_dividends(dividends, asset_id, raw_dividends)

        yield asset_id, asset_data


@bundles.register('cndaily',
                  calendar_name='XSHG',
                  #   end_session=pd.Timestamp('now', tz='Asia/Shanghai'),
                  minutes_per_day=241)
def cndaily_bundle(environ,
                   asset_db_writer,
                   minute_bar_writer,
                   daily_bar_writer,
                   adjustment_writer,
                   calendar,
                   start_session,
                   end_session,
                   cache,
                   show_progress,
                   output_dir):
    """Build a zipline data bundle from the cnstock dataset.
    """
    log.info('读取股票元数据......')
    metadata = gen_asset_metadata(False)
    # 资产元数据写法要求添加sid列！！！
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
        gen_symbol_data(
            symbol_map,
            sessions,
            splits,
            dividends,
            is_minutely=False
        ),
        show_progress=show_progress,
    )

    adjustment_writer.write(
        splits=None if len(splits) == 0 else pd.concat(
            splits, ignore_index=True),
        dividends=None if len(dividends) == 0 else pd.concat(
            dividends, ignore_index=True),
    )


# 不可包含退市或者暂停上市的股票代码


@bundles.register('cnminutely',
                  calendar_name='XSHG',
                  start_session=pd.Timestamp(
                      'now', tz='Asia/Shanghai') - pd.Timedelta(days=30),
                  #   end_session=pd.Timestamp('now', tz='Asia/Shanghai'),
                  minutes_per_day=241)
def cnminutely_bundle(environ,
                      asset_db_writer,
                      minute_bar_writer,
                      daily_bar_writer,
                      adjustment_writer,
                      calendar,
                      start_session,
                      end_session,
                      cache,
                      show_progress,
                      output_dir):
    """Build a zipline data bundle from the cnstock dataset.
    """
    log.info('读取股票元数据......')
    metadata = gen_asset_metadata()
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
        gen_symbol_data(
            symbol_map,
            sessions,
            splits,
            dividends,
            is_minutely=True
        ),
        show_progress=show_progress,
    )

    adjustment_writer.write(
        splits=None if len(splits) == 0 else pd.concat(
            splits, ignore_index=True),
        dividends=None if len(dividends) == 0 else pd.concat(
            dividends, ignore_index=True),
    )


# # 测试数据集

# 不可包含退市或者暂停上市的股票代码

@bundles.register('cdtest',
                  calendar_name='XSHG',
                  #   end_session=pd.Timestamp('now', tz='Asia/Shanghai'),
                  minutes_per_day=241)
def cntdaily_bundle(environ,
                    asset_db_writer,
                    minute_bar_writer,
                    daily_bar_writer,
                    adjustment_writer,
                    calendar,
                    start_session,
                    end_session,
                    cache,
                    show_progress,
                    output_dir):
    """Build a zipline test data bundle from the cnstock dataset.
    """
    log.info('读取股票元数据(测试集)......')
    # 测试集
    metadata = gen_asset_metadata(False)
    metadata = metadata[metadata.symbol.isin(TEST_SYMBOLS)]
    metadata.reset_index(inplace=True, drop=True)
    metadata['sid'] = metadata.symbol.map(_to_sid)
    sessions = calendar.sessions_in_range(start_session, end_session)

    symbol_map = metadata.symbol
    log.info('生成测试数据集（共{}只）'.format(len(symbol_map)))
    splits = []
    dividends = []

    asset_db_writer.write(metadata, exchanges=_exchanges())

    daily_bar_writer.write(
        gen_symbol_data(
            symbol_map,
            sessions,
            splits,
            dividends,
            is_minutely=False,
        ),
        show_progress=show_progress,
    )
    adjustment_writer.write(
        splits=pd.concat(splits, ignore_index=True) if len(splits) else None,
        dividends=pd.concat(dividends, ignore_index=True) if len(
            dividends) else None,
    )


@bundles.register('cmtest',
                  calendar_name='XSHG',
                  start_session=pd.Timestamp(
                      'now', tz='Asia/Shanghai') - pd.Timedelta(days=30),
                  #   end_session=pd.Timestamp('now', tz='Asia/Shanghai'),
                  minutes_per_day=241)
def cntminutely_bundle(environ,
                       asset_db_writer,
                       minute_bar_writer,
                       daily_bar_writer,
                       adjustment_writer,
                       calendar,
                       start_session,
                       end_session,
                       cache,
                       show_progress,
                       output_dir):
    """Build a zipline test data bundle from the cnstock dataset.
    """
    log.info('读取股票元数据(测试集)......')
    # 测试集
    metadata = gen_asset_metadata()
    metadata = metadata[metadata.symbol.isin(TEST_SYMBOLS)]
    metadata.reset_index(inplace=True, drop=True)
    metadata['sid'] = metadata.symbol.map(_to_sid)
    sessions = calendar.sessions_in_range(start_session, end_session)

    symbol_map = metadata.symbol
    log.info('生成测试数据集（共{}只）'.format(len(symbol_map)))
    splits = []
    dividends = []

    asset_db_writer.write(metadata, exchanges=_exchanges())

    minute_bar_writer.write(
        gen_symbol_data(
            symbol_map,
            sessions,
            splits,
            dividends,
            is_minutely=True
        ),
        show_progress=show_progress,
    )

    adjustment_writer.write(
        splits=None if len(splits) == 0 else pd.concat(
            splits, ignore_index=True),
        dividends=None if len(dividends) == 0 else pd.concat(
            dividends, ignore_index=True),
    )
