from datetime import time
from unittest import TestCase
import pandas as pd
from trading_calendars import get_calendar
from trading_calendars.utils.pandas_utils import days_at_time

from zipline.gens.sim_engine import (
    MinuteSimulationClock,
    SESSION_START,
    BEFORE_TRADING_START_BAR,
    BAR,
    SESSION_END
)


class TestClock(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.nyse_calendar = get_calendar("XSHG")

        # 12-31 周二, 1-1 元旦休市，so there are 3 sessions in this range (31, 2, 3)
        cls.sessions = cls.nyse_calendar.sessions_in_range(
            pd.Timestamp("2019-12-31"),
            pd.Timestamp("2020-01-03")
        )

        trading_o_and_c = cls.nyse_calendar.schedule.loc[cls.sessions]
        cls.opens = trading_o_and_c['market_open']
        cls.closes = trading_o_and_c['market_close']

    def test_bts_before_session(self):
        clock = MinuteSimulationClock(
            self.sessions,
            self.opens,
            self.closes,
            days_at_time(self.sessions, time(6, 17), "Asia/Shanghai"),
            False
        )

        all_events = list(clock)

        def _check_session_bts_first(session_label, events, bts_dt):
            minutes = self.nyse_calendar.minutes_for_session(session_label)

            self.assertEqual(245, len(events))

            self.assertEqual(events[0], (session_label, SESSION_START))
            self.assertEqual(events[1], (bts_dt, BEFORE_TRADING_START_BAR))
            for i in range(2, 244):
                self.assertEqual(events[i], (minutes[i - 2], BAR))
            self.assertEqual(events[244], (minutes[-1], SESSION_END))

        _check_session_bts_first(
            self.sessions[0],
            all_events[0:245],
            pd.Timestamp("2019-12-31 6:17", tz='Asia/Shanghai')
        )

        _check_session_bts_first(
            self.sessions[1],
            all_events[245:490],
            pd.Timestamp("2020-01-02 6:17", tz='Asia/Shanghai')
        )

        _check_session_bts_first(
            self.sessions[2],
            all_events[490:],
            pd.Timestamp("2020-01-03 6:17", tz='Asia/Shanghai')
        )

    def test_bts_during_session(self):
        self.verify_bts_during_session(
            time(11, 31), [
                pd.Timestamp("2019-12-31 11:31", tz='Asia/Shanghai'),
                pd.Timestamp("2020-01-02 11:31", tz='Asia/Shanghai'),
                pd.Timestamp("2020-01-03 11:31", tz='Asia/Shanghai')
            ],
            121
        )

    def test_bts_on_first_minute(self):
        self.verify_bts_during_session(
            time(9, 31), [
                pd.Timestamp("2019-12-31 9:31", tz='Asia/Shanghai'),
                pd.Timestamp("2020-01-02 9:31", tz='Asia/Shanghai'),
                pd.Timestamp("2020-01-03 9:31", tz='Asia/Shanghai')
            ],
            1
        )

    def test_bts_on_last_minute(self):
        self.verify_bts_during_session(
            time(15, 1), [
                pd.Timestamp("2019-12-31 15:01", tz='Asia/Shanghai'),
                pd.Timestamp("2020-01-02 15:01", tz='Asia/Shanghai'),
                pd.Timestamp("2020-01-03 15:01", tz='Asia/Shanghai')
            ],
            242
        )

    def verify_bts_during_session(self, bts_time, bts_session_times, bts_idx):
        def _check_session_bts_during(session_label, events, bts_dt):
            minutes = self.nyse_calendar.minutes_for_session(session_label)

            self.assertEqual(245, len(events))

            self.assertEqual(events[0], (session_label, SESSION_START))

            for i in range(1, bts_idx):
                self.assertEqual(events[i], (minutes[i - 1], BAR))

            self.assertEqual(
                events[bts_idx],
                (bts_dt, BEFORE_TRADING_START_BAR)
            )

            for i in range(bts_idx + 1, 243):
                self.assertEqual(events[i], (minutes[i - 2], BAR))

            self.assertEqual(events[244], (minutes[-1], SESSION_END))

        clock = MinuteSimulationClock(
            self.sessions,
            self.opens,
            self.closes,
            days_at_time(self.sessions, bts_time, "Asia/Shanghai"),
            False
        )

        all_events = list(clock)

        _check_session_bts_during(
            self.sessions[0],
            all_events[0:245],
            bts_session_times[0]
        )

        _check_session_bts_during(
            self.sessions[1],
            all_events[245:490],
            bts_session_times[1]
        )

        _check_session_bts_during(
            self.sessions[2],
            all_events[490:],
            bts_session_times[2]
        )

    def test_bts_after_session(self):
        clock = MinuteSimulationClock(
            self.sessions,
            self.opens,
            self.closes,
            days_at_time(self.sessions, time(19, 5), "Asia/Shanghai"),
            False
        )

        all_events = list(clock)

        # since 19:05 Asia/Shanghai is after the XSHG is closed, we don't emit
        # BEFORE_TRADING_START.  therefore, each day has SESSION_START,
        # 242 BARs, and then SESSION_END

        def _check_session_bts_after(session_label, events):
            minutes = self.nyse_calendar.minutes_for_session(session_label)

            self.assertEqual(244, len(events))
            self.assertEqual(events[0], (session_label, SESSION_START))

            for i in range(1, 243):
                self.assertEqual(events[i], (minutes[i - 1], BAR))

            self.assertEqual(events[-1], (minutes[241], SESSION_END))

        for i in range(0, 2):
            _check_session_bts_after(
                self.sessions[i],
                all_events[(i * 244): ((i + 1) * 244)]
            )
