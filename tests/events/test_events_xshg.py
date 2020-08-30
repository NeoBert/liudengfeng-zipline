#
# Copyright 2016 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# TODO：考虑午休后，对于周开始、结束的处理不正确，需要修正
from datetime import timedelta
from unittest import TestCase

import pandas as pd
import pytest
from parameterized import parameterized

from zipline.testing import parameter_space
from zipline.utils.events import (AfterOpen, BeforeClose,
                                  NDaysBeforeLastTradingDayOfWeek,
                                  NthTradingDayOfWeek)

from .test_events import (StatefulRulesTests, StatelessRulesTests,
                          minutes_for_days)


class TestStatelessRulesXSHG(StatelessRulesTests, TestCase):
    CALENDAR_STRING = "XSHG"

    HALF_SESSION = pd.Timestamp("2019-07-03", tz='UTC')
    FULL_SESSION = pd.Timestamp("2019-09-24", tz='UTC')

    # 期望触发未清洗
    @pytest.mark.skip
    @parameter_space(rule_offset=(0, 1, 2, 3, 4),
                     start_offset=(0, 1, 2, 3, 4),
                     type=('week_start', 'week_end'))
    def test_edge_cases_for_TradingDayOfWeek(self, rule_offset, start_offset,
                                             type):
        """
        Test that we account for midweek holidays. Monday 01/20 is a holiday.
        Ensure that the trigger date for that week is adjusted
        appropriately, or thrown out if not enough trading days. Also, test
        that if we start the simulation on a day where we miss the trigger
        for that week, that the trigger is recalculated for next week.

        测试我们是否考虑了春节假期。
        2019-02-02 ~ 2019-02-10 春节期间
        确保适当调整了该周的触发日期，如果交易日不足，则将其丢弃。
        同样，测试一下，如果我们在错过该周触发的那天开始仿真，
        那么下周将重新计算触发。
        """
        sim_start = pd.Timestamp('2019-01-28', tz='UTC') + \
            timedelta(days=start_offset)

        delta = timedelta(days=start_offset)

        feb_minutes = self.cal.minutes_for_sessions_in_range(
            pd.Timestamp("2019-01-28", tz='UTC') + delta,
            pd.Timestamp("2019-03-01", tz='UTC'))

        if type == 'week_start':
            rule = NthTradingDayOfWeek
            # Expect to trigger on the first trading day of the week, plus the
            # offset
            trigger_periods = [
                pd.Timestamp('2019-01-28', tz='UTC'),
                pd.Timestamp('2019-02-11', tz='UTC'),
                pd.Timestamp('2019-02-19', tz='UTC'),
                pd.Timestamp('2019-02-26', tz='UTC'),
            ]
            trigger_periods = \
                [x + timedelta(days=rule_offset) for x in trigger_periods]
        else:
            rule = NDaysBeforeLastTradingDayOfWeek
            # Expect to trigger on the last trading day of the week, minus the
            # offset
            trigger_periods = [
                pd.Timestamp('2019-02-01', tz='UTC'),
                pd.Timestamp('2019-02-15', tz='UTC'),
                pd.Timestamp('2019-02-22', tz='UTC'),
                pd.Timestamp('2019-03-01', tz='UTC'),
            ]
            trigger_periods = \
                [x - timedelta(days=rule_offset) for x in trigger_periods]

        rule.cal = self.cal
        should_trigger = rule(rule_offset).should_trigger

        # If offset is 4, there is not enough trading days in the short week,
        # and so it should not trigger
        # if rule_offset == 4:
        #     del trigger_periods[2]

        # Filter out trigger dates that happen before the simulation starts
        trigger_periods = [x for x in trigger_periods if x >= sim_start]

        # Get all the minutes on the trigger dates
        trigger_minutes = self.cal.minutes_for_session(trigger_periods[0])
        for period in trigger_periods[1:]:
            # # 使用添加
            trigger_minutes = trigger_minutes.append(
                self.cal.minutes_for_session(period))

        expected_n_triggered = len(trigger_minutes)
        trigger_minutes_iter = iter(trigger_minutes)

        n_triggered = 0
        for m in feb_minutes:
            if should_trigger(m):
                print(m)
                self.assertEqual(m, next(trigger_minutes_iter))
                n_triggered += 1

        self.assertEqual(n_triggered, expected_n_triggered)

    @parameterized.expand([('week_start', ), ('week_end', )])
    def test_week_and_time_composed_rule_am(self, type):
        week_rule = NthTradingDayOfWeek(0) if type == 'week_start' else \
            NDaysBeforeLastTradingDayOfWeek(4)
        time_rule = AfterOpen(minutes=60)

        week_rule.cal = self.cal
        time_rule.cal = self.cal

        composed_rule = week_rule & time_rule

        should_trigger = composed_rule.should_trigger

        week_minutes = self.cal.minutes_for_sessions_in_range(
            pd.Timestamp("2019-02-11", tz='UTC'),
            pd.Timestamp("2019-02-22", tz='UTC'))
        # # 开盘时间
        dt = pd.Timestamp('2019-02-11 09:30:00', tz=self.cal.tz)
        dt = dt.tz_convert('UTC')
        trigger_day_offset = 0
        trigger_minute_offset = 60
        n_triggered = 0

        def day_offset(n):
            ndays = n // 242
            nweeks = ndays // 5 + 1
            offset = ndays + (nweeks - 1) * 2
            return offset

        for n, m in enumerate(week_minutes):
            trigger_day_offset = day_offset(n)
            if should_trigger(m):
                self.assertEqual(
                    m, dt + timedelta(days=trigger_day_offset) +
                    timedelta(minutes=trigger_minute_offset))
                n_triggered += 1

        self.assertEqual(n_triggered, 2)

    @parameterized.expand([('week_start', ), ('week_end', )])
    def test_week_and_time_composed_rule_pm(self, type):
        week_rule = NthTradingDayOfWeek(0) if type == 'week_start' else \
            NDaysBeforeLastTradingDayOfWeek(4)
        # # 下午开盘后一分钟
        time_rule = AfterOpen(minutes=121)

        week_rule.cal = self.cal
        time_rule.cal = self.cal

        composed_rule = week_rule & time_rule

        should_trigger = composed_rule.should_trigger

        week_minutes = self.cal.minutes_for_sessions_in_range(
            pd.Timestamp("2019-02-11", tz='UTC'),
            pd.Timestamp("2019-02-15", tz='UTC'))
        # # 开盘时间
        dt = pd.Timestamp('2019-02-11 09:30:00', tz=self.cal.tz)
        dt = dt.tz_convert('UTC')
        trigger_day_offset = 0
        trigger_minute_offset = 121
        n_triggered = 0

        for m in week_minutes:
            if should_trigger(m):
                self.assertEqual(
                    m, dt + timedelta(days=trigger_day_offset) +
                    timedelta(minutes=trigger_minute_offset))
                n_triggered += 1

        self.assertEqual(n_triggered, 1)

    def test_offset_too_far(self):
        minute_groups = minutes_for_days(self.cal, ordered_days=True)

        # Neither rule should ever fire, since they are configured to fire
        # 11+ hours after the open or before the close.  a XSHG session is
        # never longer than 5.5 hours.
        after_open_rule = AfterOpen(hours=11, minutes=11)
        after_open_rule.cal = self.cal

        before_close_rule = BeforeClose(hours=11, minutes=5)
        before_close_rule.cal = self.cal

        for session_minutes in minute_groups:
            for minute in session_minutes:
                self.assertFalse(after_open_rule.should_trigger(minute))
                self.assertFalse(before_close_rule.should_trigger(minute))


class TestStatefulRulesXSHG(StatefulRulesTests, TestCase):
    CALENDAR_STRING = "XSHG"
