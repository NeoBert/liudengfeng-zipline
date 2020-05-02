from trading_calendars import get_calendar
from zipline.research.pipebench import _pipeline_engine_and_calendar_for_bundle
from zipline.pipeline import Pipeline
bundle = 'cndaily'
engine, calendar = _pipeline_engine_and_calendar_for_bundle(bundle)
p = Pipeline()