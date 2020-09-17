"""
zipline ingest 适用于期初全新导入数据
本模块将每日所需处理任务集成，`zipline rfd`一条命令完成任务。

数据包括：
1. 日线数据【含附加列】
2. 分钟级别【仅含OHLCV】
3. 基础数据【fundamentals使用】

由于分钟级别数据写入极其耗时，使用`append`模式
首次完成`ingest`分钟级别数据后，日常使用刷新方式写入数据

处理流程
1. 判断是否存在日线数据，否则执行日线`zipline ingest`
2. 判断是否存在分钟级别数据，否则执行`zipline ingest -b cnminutely`
3. 拷贝日线至分钟级别数据目录【含调整数据库文件】
4. 添加分钟级别数据
5. 执行`zipline fm`
6. 清理保留最新2次的日线数据`zipline clean -k 2`
"""
import subprocess
from os.path import join
from pathlib import Path
import shutil

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


def info_func():
    calendar = get_calendar('XSHG')
    # 本地分钟级别数据开始日期
    CALENDAR_START = pd.Timestamp('2020-06-29', tz='UTC')
    now = pd.Timestamp('now')
    if now.hour >= 15:
        CALENDAR_STOP = calendar.actual_last_session
    else:
        CALENDAR_STOP = calendar.actual_last_session - calendar.day
    return calendar, CALENDAR_START, CALENDAR_STOP


def execute(cmd):
    subprocess.run(cmd)


def try_run_ingest(name, force=False):
    # 注意：每一项都要分列
    cmd = ["zipline", "ingest", "-b", name]
    # 强制执行数据提取
    if force:
        execute(cmd)
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
        label="【新增股票】分钟级别数据",
    )
    with ctx as it:
        for code in it:
            sid = int(code)
            df = fetch_single_minutely_equity(code, s, e)
            # 务必转换为UTC时区
            df = df.tz_localize('Asia/Shanghai').tz_convert('UTC')
            writer.write_sid(sid, df)


def append(dest, codes):
    """添加股票代码分钟级别数据"""
    c, s, e = info_func()
    writer = BcolzMinuteBarWriter.open(dest, e)
    ctx = maybe_show_progress(
        codes,
        show_progress=True,
        # 🆗 显示股票代码
        item_show_func=lambda e: e,
        label="【更新股票】分钟级别数据",
    )
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
            df = fetch_single_minutely_equity(code, start, e)
            # 务必转换为UTC时区
            df = df.tz_localize('Asia/Shanghai').tz_convert('UTC')
            writer.write_sid(sid, df)


def refresh_data():
    d_path = try_run_ingest('cndaily', True)
    m_path = try_run_ingest('cnminutely')

    # 拷贝调整数据库
    sql_fs = ['adjustments.sqlite', 'assets-7.sqlite']
    for f in sql_fs:
        src = join(d_path, f)
        dst = join(m_path, f)
        shutil.copy2(src, dst)
    
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
    log.info("使用`000002【A股指数】`日线数据作为基准收益率")
    # 代码在其子目录下 ** 代表当前目录的子目录
    db_codes = [p.stem.split('.')[0] for p in m_dir_path.glob("**/*.bcolz")]
    web_codes = get_recent_trading_stocks()

    # 全新股票代码采用插入方式
    to_insert = set(web_codes).difference(db_codes)
    insert(dst, to_insert)

    # 已经存在的股票代码使用添加方式
    to_append = set(web_codes).intersection(db_codes)
    append(dst, to_append)

    # 基础数据【fundamentals使用】
    cmd = ['zipline', 'fm']
    execute(cmd)
    # 保留最新2次提取的日线数据
    cmd = ['zipline', 'clean', '-k', '2']
    execute(cmd)
