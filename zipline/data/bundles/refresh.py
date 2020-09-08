"""
由于分钟级别数据写入极其耗时，使用`append`模式
首次完成`ingest`分钟级别数据后，日常使用刷新方式写入数据

写入日线数据大约800~900s

流程
1. 判断是否存在日线数据，否则执行日线`zipline ingest`
2. 判断是否存在分钟级别数据，否则执行`zipline ingest -b cnminutely`
3. 拷贝日线调整数据库文件至分钟级别数据目录
4. 添加分钟级别数据
"""
import subprocess
from os.path import join
from pathlib import Path
from shutil import copy2

import click
import pandas as pd
from cnswd.websource.tencent import get_recent_trading_stocks
from trading_calendars import get_calendar

from zipline.utils.cli import maybe_show_progress
from cnswd.utils import make_logger
from ..localdata import fetch_single_minutely_equity
from ..minute_bars import CN_EQUITIES_MINUTES_PER_DAY, BcolzMinuteBarWriter
from .core import load, most_recent_data


log = make_logger('cnquandl', collection='zipline')
calendar = get_calendar('XSHG')
# 本地分钟级别数据开始日期
CALENDAR_START = pd.Timestamp('2020-06-29', tz='UTC')
CALENDAR_STOP = calendar.actual_last_session
now = pd.Timestamp('now')
if now.hour >= 15:
    DATA_STOP = calendar.actual_last_session
else:
    DATA_STOP = calendar.actual_last_session - calendar.day


def try_run_ingest(name):
    try:
        return most_recent_data(name)
    except ValueError:
        # 注意：每一项都要分列
        cmd = ["zipline", "ingest", "-b", name]
        subprocess.run(cmd)
    finally:
        return most_recent_data(name)


def insert(dest, codes):
    """插入股票代码分钟级别数据"""
    writer = BcolzMinuteBarWriter(
        dest,
        calendar,
        CALENDAR_START,
        CALENDAR_STOP,
        CN_EQUITIES_MINUTES_PER_DAY,
    )
    ctx = maybe_show_progress(
        codes,
        show_progress=True,
        item_show_func=lambda e: e,
        label="插入股票代码分钟级别数据",
    )
    with ctx as it:
        for code in it:
            sid = int(code)
            df = fetch_single_minutely_equity(
                code, CALENDAR_START, DATA_STOP)
            # 务必转换为UTC时区
            df = df.tz_localize('Asia/Shanghai').tz_convert('UTC')
            writer.write_sid(sid, df)


def append(dest, codes):
    """添加股票代码分钟级别数据"""
    writer = BcolzMinuteBarWriter.open(dest, CALENDAR_STOP)
    ctx = maybe_show_progress(
        codes,
        show_progress=True,
        # 🆗 显示股票代码
        item_show_func=lambda e: e,
        label="添加股票代码分钟级别数据",
    )
    with ctx as it:
        for code in it:
            sid = int(code)
            last_dt = writer.last_date_in_output_for_sid(sid)
            if last_dt is pd.NaT:
                start = CALENDAR_START
            else:
                start = last_dt + calendar.day
            if start > DATA_STOP:
                continue
            df = fetch_single_minutely_equity(code, start, DATA_STOP)
            # 务必转换为UTC时区
            df = df.tz_localize('Asia/Shanghai').tz_convert('UTC')
            writer.write_sid(sid, df)


def refresh_data():
    d_path = try_run_ingest('cndaily')
    m_path = try_run_ingest('cnminutely')
    # 拷贝调整数据库文件到分钟级别目录
    sql_fs = ['adjustments.sqlite', 'assets-7.sqlite']
    for f in sql_fs:
        src = join(d_path, f)
        tgt = join(m_path, f)
        copy2(src, tgt)
    dest = join(m_path, 'minute_equities.bcolz')
    m_dir_path = Path(dest)
    log.info("使用`000002【A股指数】`日线数据作为基准收益率")
    # 代码在其子目录下 ** 代表当前目录的子目录
    db_codes = [p.stem.split('.')[0] for p in m_dir_path.glob("**/*.bcolz")]
    web_codes = get_recent_trading_stocks()
    # 全新股票代码采用插入方式
    to_insert = set(web_codes).difference(db_codes)
    insert(dest, to_insert)
    # 已经存在的股票代码使用添加方式
    to_append = set(web_codes).intersection(db_codes)
    append(dest, to_append)
