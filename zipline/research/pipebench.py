from datetime import time

import pandas as pd
import pytz

from zipline.data import bundles
from zipline.pipeline.data import EquityPricing
from zipline.pipeline.domain import (CN_EQUITIES, GENERIC, Domain,
                                     EquitySessionDomain)
from zipline.pipeline.engine import SimplePipelineEngine
from zipline.pipeline.hooks.progress import (IPythonWidgetProgressPublisher,
                                             ProgressHooks)
from zipline.pipeline.loaders import EquityPricingLoader
from zipline.pipeline.loaders.blaze import global_loader
from zipline.utils.memoize import remember_last
from zipline.utils.ts_utils import ensure_utc

publisher = IPythonWidgetProgressPublisher()
hooks = [ProgressHooks.with_static_publisher(publisher)]


TZ = 'Asia/Shanghai'


def create_domain(sessions,
                  data_query_time=time(0, 0, tzinfo=pytz.utc),
                  data_query_date_offset=0):
    if sessions.tz is None:
        sessions = sessions.tz_localize('UTC')

    return EquitySessionDomain(
        sessions,
        country_code='CN',
        data_query_time=data_query_time,
        data_query_date_offset=data_query_date_offset,
    )


def run_pipeline(pipeline, start_date, end_date, bundle=None):
    if bundle is None:
        bundle = 'dwy'

    return run_pipeline_against_bundle(
        pipeline, start_date, end_date, bundle
    )


def _tdate(calendar, d, direction):
    if not calendar.is_session(d):
        # this is not a trading session, advance to the next session
        # return calendar.minute_to_session_label(
        #     d,
        #     direction=direction,
        # )
        # next为上一开盘分钟，previous为前收盘时间
        if direction == 'next':
            return calendar.next_open(d)
        elif direction == 'previous':
            return calendar.previous_close(d)
        else:
            raise ValueError(f'方向只能为`next或者previous`，输入：{direction}')
    return d

# TODO:解决分钟级别
def _tminute(calendar, dt, direction):
    if isinstance(dt, str):
        dt = pd.Timestamp(dt, tz=TZ).tz_convert('UTC')
    sessions = calendar.all_sessions
    loc = sessions.get_loc(dt)

    if direction == 'next':
        return calendar.all_sessions
    elif direction == 'previous':
        return calendar.previous_close(dt)
    else:
        raise ValueError(f'方向只能为`next或者previous`，输入：{direction}')


def run_pipeline_against_bundle(pipeline, start_date, end_date, bundle):
    """Run a pipeline against the data in a bundle.

    Parameters
    ----------
    pipeline : zipline.pipeline.Pipeline
        The pipeline to run.
    start_date : pd.Timestamp
        The start date of the pipeline.
    end_date : pd.Timestamp
        The end date of the pipeline.
    bundle : str
        The name of the bundle to run the pipeline against.

    Returns
    -------
    result : pd.DataFrame
        The result of the pipeline.
    """
    engine, calendar = _pipeline_engine_and_calendar_for_bundle(bundle)
    # start_date = ensure_utc(start_date)
    # end_date = ensure_utc(end_date)

    # if start_date == end_date:
    #     d1 = d2 = _tdate(calendar, end_date, 'previous')
    # else:
    #     d1 = _tdate(calendar, start_date, 'next').normalize()
    #     d2 = _tdate(calendar, end_date, 'previous').normalize()
    #     if d1 > d2:
    #         d1 = d2
    if bundle == 'dwy':
        dts = pd.date_range(start_date, end_date, tz='UTC')
        trading_sessions = calendar.schedule.index.intersection(dts)
        start, end = trading_sessions[0], trading_sessions[-1]
    else:
        start = _tminute(calendar, start_date, 'previous')
        end = _tminute(calendar, end_date, 'next')

    if pipeline._domain is GENERIC:
        pipeline._domain = CN_EQUITIES
    return engine.run_pipeline(pipeline, start, end, hooks=hooks)


@remember_last
def _pipeline_engine_and_calendar_for_bundle(bundle):
    """Create a pipeline engine for the given bundle.

    Parameters
    ----------
    bundle : str
        The name of the bundle to create a pipeline engine for.

    Returns
    -------
    engine : zipline.pipleine.engine.SimplePipelineEngine
        The pipeline engine which can run pipelines against the bundle.
    calendar : zipline.utils.calendars.TradingCalendar
        The trading calendar for the bundle.
    """
    bundle_data = bundles.load(bundle)
    if bundle == 'dwy':
        pipeline_loader = EquityPricingLoader.without_fx(
            bundle_data.equity_daily_bar_reader,
            bundle_data.adjustment_reader,
        )
    else:
        # 分钟级别数据
        pipeline_loader = EquityPricingLoader.without_fx(
            bundle_data.equity_minute_bar_reader,
            bundle_data.adjustment_reader,
        )

    def choose_loader(column):
        if column.unspecialize() in EquityPricing.columns:
            return pipeline_loader
        # elif column in global_loader:
        #     return global_loader
        else:
            return global_loader
        raise ValueError("%s is NOT registered in `PipelineLoader`." % column)

    calendar = bundle_data.equity_daily_bar_reader.trading_calendar
    return (
        SimplePipelineEngine(
            choose_loader,
            bundle_data.asset_finder,
        ),
        calendar,
    )
