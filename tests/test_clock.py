# 完成测试 ✔
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
        cls.xshg_calendar = get_calendar("XSHG")

        # 12-31 周二, 1-1 元旦休市，so there are 3 sessions
        # 日期分别为 (31, 2, 3)
        cls.sessions = cls.xshg_calendar.sessions_in_range(
            pd.Timestamp("2019-12-31"),
            pd.Timestamp("2020-01-03")
        )

        trading_o_and_c = cls.xshg_calendar.schedule.loc[cls.sessions]
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
            minutes = self.xshg_calendar.minutes_for_session(session_label)

            self.assertEqual(243, len(events))

            self.assertEqual(events[0], (session_label, SESSION_START))
            self.assertEqual(events[1], (bts_dt, BEFORE_TRADING_START_BAR))
            for i in range(2, 242):
                self.assertEqual(events[i], (minutes[i - 2], BAR))
            self.assertEqual(events[242], (minutes[-1], SESSION_END))

        _check_session_bts_first(
            self.sessions[0],
            all_events[0:243],
            pd.Timestamp("2019-12-31 6:17", tz="Asia/Shanghai")
        )

        _check_session_bts_first(
            self.sessions[1],
            all_events[243:486],
            pd.Timestamp("2020-01-02 6:17", tz="Asia/Shanghai")
        )

        _check_session_bts_first(
            self.sessions[2],
            all_events[486:],
            pd.Timestamp("2020-01-03 6:17", tz="Asia/Shanghai")
        )

    def test_bts_during_session(self):
        self.verify_bts_during_session(
            time(10, 45), [
                pd.Timestamp("2019-12-31 10:45", tz="Asia/Shanghai"),
                pd.Timestamp("2020-01-02 10:45", tz="Asia/Shanghai"),
                pd.Timestamp("2020-01-03 10:45", tz="Asia/Shanghai")
            ],
            75
        )

    def test_bts_on_first_minute(self):
        self.verify_bts_during_session(
            time(9, 30), [
                pd.Timestamp("2019-12-31 9:30", tz="Asia/Shanghai"),
                pd.Timestamp("2020-01-02 9:30", tz="Asia/Shanghai"),
                pd.Timestamp("2020-01-03 9:30", tz="Asia/Shanghai")
            ],
            1
        )

    def test_bts_on_last_minute(self):
        self.verify_bts_during_session(
            time(15, 00), [
                pd.Timestamp("2019-12-31 15:00", tz="Asia/Shanghai"),
                pd.Timestamp("2020-01-02 15:00", tz="Asia/Shanghai"),
                pd.Timestamp("2020-01-03 15:00", tz="Asia/Shanghai")
            ],
            240
        )

    def verify_bts_during_session(self, bts_time, bts_session_times, bts_idx):
        def _check_session_bts_during(session_label, events, bts_dt):
            minutes = self.xshg_calendar.minutes_for_session(session_label)

            self.assertEqual(243, len(events))

            self.assertEqual(events[0], (session_label, SESSION_START))

            for i in range(1, bts_idx):
                self.assertEqual(events[i], (minutes[i - 1], BAR))

            self.assertEqual(
                events[bts_idx],
                (bts_dt, BEFORE_TRADING_START_BAR)
            )

            for i in range(bts_idx + 1, 241):
                self.assertEqual(events[i], (minutes[i - 2], BAR))

            self.assertEqual(events[242], (minutes[-1], SESSION_END))

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
            all_events[0:243],
            bts_session_times[0]
        )

        _check_session_bts_during(
            self.sessions[1],
            all_events[243:486],
            bts_session_times[1]
        )

        _check_session_bts_during(
            self.sessions[2],
            all_events[486:],
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

        # since 19:05 Eastern is after the NYSE is closed, we don't emit
        # BEFORE_TRADING_START.  therefore, each day has SESSION_START,
        # 390 BARs, and then SESSION_END

        def _check_session_bts_after(session_label, events):
            minutes = self.xshg_calendar.minutes_for_session(session_label)

            self.assertEqual(242, len(events))
            self.assertEqual(events[0], (session_label, SESSION_START))

            for i in range(1, 241):
                self.assertEqual(events[i], (minutes[i - 1], BAR))

            self.assertEqual(events[-1], (minutes[239], SESSION_END))

        for i in range(0, 2):
            _check_session_bts_after(
                self.sessions[i],
                all_events[(i * 242): ((i + 1) * 242)]
            )
