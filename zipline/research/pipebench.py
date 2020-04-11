import pandas as pd

from zipline.data import bundles
from zipline.pipeline.data import EquityPricing
from zipline.pipeline.domain import CN_EQUITIES
from zipline.pipeline.engine import SimplePipelineEngine
from zipline.pipeline.loaders import EquityPricingLoader
from zipline.pipeline.loaders.blaze import global_loader
from zipline.utils.memoize import remember_last
from zipline.utils.ts_utils import ensure_utc


def run_pipeline(pipeline, start_date, end_date):
    default_bundle = 'cndaily'

    return run_pipeline_against_bundle(
        pipeline, start_date, end_date, default_bundle
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

    start_date = ensure_utc(start_date)  # pd.Timestamp(start_date, tz='utc')
    end_date = ensure_utc(end_date)  # pd.Timestamp(end_date, tz='utc')
    if start_date == end_date:
        d1 = d2 = _tdate(calendar, end_date, 'previous')
    else:
        d1 = _tdate(calendar, start_date, 'next')
        d2 = _tdate(calendar, end_date, 'previous')
        if d1 > d2:
            d1 = d2
    return engine.run_pipeline(pipeline, d1, d2)


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

    pipeline_loader = EquityPricingLoader.without_fx(
        bundle_data.equity_daily_bar_reader,
        bundle_data.adjustment_reader,
    )

    def choose_loader(column):
        if column.unspecialize() in EquityPricing.columns:
            return pipeline_loader
        elif column in global_loader:
            return global_loader
        raise ValueError("%s is NOT registered in `PipelineLoader`." % column)

    calendar = bundle_data.equity_daily_bar_reader.trading_calendar
    return (
        SimplePipelineEngine(
            choose_loader,
            bundle_data.asset_finder,
            default_domain=CN_EQUITIES,
        ),
        calendar,
    )
