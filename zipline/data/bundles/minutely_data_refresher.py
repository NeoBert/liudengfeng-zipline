"""
由于分钟级别数据写入极其耗时，使用`append`模式
首次完成`ingest`分钟级别数据后，日常使用刷新方式写入数据

处理流程
1. 判断是否存在日线数据，否则执行日线`zipline ingest`
2. 判断是否存在分钟级别数据，否则执行`zipline ingest -b cnminutely`
3. 拷贝日线至分钟级别数据目录【含调整数据库文件】
4. 添加分钟级别数据
"""
import shutil
import subprocess
from os.path import join
from pathlib import Path

import click
import pandas as pd
from cnswd.mongodb import get_db
from cnswd.scripts.base import get_stock_status
from cnswd.utils import make_logger
from trading_calendars import get_calendar
from zipline.utils.cli import maybe_show_progress

from ..minute_bars import CN_EQUITIES_MINUTES_PER_DAY, BcolzMinuteBarWriter
from .core import load, most_recent_data
from .for_test_bundle import TEST_CODES
from .wy_minute_data import encode_index_code, fetch_single_minutely_equity


logger = make_logger('数据包', collection='zipline')


def info_func():
    calendar = get_calendar('XSHG')
    # 本地分钟级别数据开始日期
    CALENDAR_START = pd.Timestamp('2020-06-29', tz='UTC')
    now = pd.Timestamp('now')
    if now.date() == calendar.actual_last_session.date() and now.hour < 15:
        CALENDAR_STOP = calendar.actual_last_session - calendar.day
    else:
        CALENDAR_STOP = calendar.actual_last_session
    return calendar, CALENDAR_START, CALENDAR_STOP


def execute(cmd):
    subprocess.run(cmd)


def try_run_ingest(name):
    # 注意：每一项都要分列
    cmd = ["zipline", "ingest", "-b", name]
    try:
        return most_recent_data(name)
    except ValueError:
        # 不存在数据包时，执行提取数据
        execute(cmd)
    finally:
        return most_recent_data(name)


def insert(dest, codes):
    """插入股票代码分钟级别数据"""
    c, s, e = info_func()
    writer = BcolzMinuteBarWriter(
        dest,
        c,
        s,
        e,
        CN_EQUITIES_MINUTES_PER_DAY,
    )
    ctx = maybe_show_progress(
        codes,
        show_progress=True,
        item_show_func=lambda e: e,
        label="【新增】分钟级别数据",
    )

    d_fmt = r"%Y-%m-%d"
    start_str = s.strftime(d_fmt)
    end_str = e.strftime(d_fmt)
    m_index = c.minutes_for_sessions_in_range(start_str, end_str)

    with ctx as it:
        for code in it:
            sid = int(code)
            df = fetch_single_minutely_equity(code, s.date(), e.date())
            if df.empty:
                continue
            # 务必转换为UTC时区
            # 且由于指数分钟级别数据包含的是所有分钟，需要截断至交易分钟
            df = df.tz_localize(
                'Asia/Shanghai').tz_convert('UTC').reindex(m_index, method='ffill')
            writer.write_sid(sid, df)


def append(dest, codes):
    """添加股票代码分钟级别数据"""
    c, s, e = info_func()
    writer = BcolzMinuteBarWriter.open(dest, e)
    ctx = maybe_show_progress(
        codes,
        show_progress=True,
        item_show_func=lambda e: e,
        label="【更新】分钟级别数据",
    )

    d_fmt = r"%Y-%m-%d"
    start_str = s.strftime(d_fmt)
    end_str = e.strftime(d_fmt)
    m_index = c.minutes_for_sessions_in_range(start_str, end_str)

    with ctx as it:
        for code in it:
            sid = int(code)
            last_dt = writer.last_date_in_output_for_sid(sid)
            if last_dt is pd.NaT:
                start = s
            else:
                start = last_dt + c.day
            if start > e:
                continue
            # print(sid, start.date(), e.date())
            df = fetch_single_minutely_equity(code, start.date(), e.date())
            if df.empty:
                continue
            # 务必转换为UTC时区
            # 且由于指数分钟级别数据包含的是所有分钟，需要截断至交易分钟
            df = df.tz_localize(
                'Asia/Shanghai').tz_convert('UTC').reindex(m_index, method='ffill')
            writer.write_sid(sid, df)


def _get_codes(bundle, m_dir_path):
    # 代码在其子目录下 ** 代表当前目录的子目录
    db_codes = [p.stem.split('.')[0] for p in m_dir_path.glob("**/*.bcolz")]
    if 'test' in bundle:
        web_codes = TEST_CODES
    else:
        web_codes = [code for code, dt in get_stock_status().items()
                     if dt is not None]
        db = get_db('wy_index_daily')
        index_codes = db.list_collection_names()
        web_codes += [encode_index_code(x) for x in index_codes]
    to_insert = set(web_codes).difference(db_codes)
    to_append = set(web_codes).intersection(db_codes)
    return to_insert, to_append


def refresh_data(bundle):
    # 首字为m代表分钟级别数据包
    if bundle[0] != 'm':
        return
    daily_bundle_name = f"d{bundle[1:]}"
    d_path = try_run_ingest(daily_bundle_name)
    m_path = try_run_ingest(bundle)

    logger.info("拷贝调整数据库")
    # 拷贝调整数据库
    sql_fs = ['adjustments.sqlite', 'assets-7.sqlite']
    for f in sql_fs:
        src = join(d_path, f)
        dst = join(m_path, f)
        shutil.copy2(src, dst)

    logger.info("拷贝日线数据")
    # 拷贝日线数据
    name = 'daily_equities.bcolz'
    src = join(d_path, name)
    dst = join(m_path, name)

    # 首先删除现存目录
    try:
        shutil.rmtree(dst)
    except:
        pass
    shutil.copytree(src, dst)

    # 处理分钟数据刷新
    dst = join(m_path, 'minute_equities.bcolz')
    m_dir_path = Path(dst)
    # logger.info("指数分钟级别数据实际为日线数据")

    # 比较已经写入的代码与代码总体
    to_insert, to_append = _get_codes(bundle, m_dir_path)

    # 全新股票代码采用插入方式
    insert(dst, to_insert)

    # 已经存在的股票代码使用添加方式
    append(dst, to_append)


def truncate(bundle, start):
    """截断分钟级别数据包中，实际交易日前ndays在所有ctable中的数据."""
    if isinstance(start, str):
        start = pd.Timestamp(start, utc=True).floor('D')
    calendar = get_calendar('XSHG')
    sessions = calendar.sessions_in_range(
        start, pd.Timestamp('today', tz='UTC'))
    if len(sessions) < 1:
        return
    date = sessions[0]
    p = most_recent_data(bundle)
    dest = join(p, 'minute_equities.bcolz')
    logger.warning(f"从交易日：{date.strftime(r'%Y-%m-%d')} 开始截断数据包{bundle}中的数据")
    writer = BcolzMinuteBarWriter.open(dest)
    writer.truncate(date)
