import re


# 浏览器设置常用
MAX_WAIT_SECOND = 60    # 尽量细化，防止请求大量数据
POLL_FREQUENCY = 0.2    # 默认值0.5太大
MAX_RELOAD_TIMES = 5    # 默认加载初始页面的最大次数


# 数据集区间条件
SETTING_CONDITION_1 = '.condition1' # 报告年份, input
SETTING_CONDITION_2 = '.condition2' # 报告类型, select
SETTING_CONDITION_3 = '.condition3' # 时间区间, input
SETTING_CONDITION_4 = '.condition4' # 单年份, input
SETTING_CONDITION_5 = '.condition5' # 单时间, input
SETTING_CONDITION_6 = '.condition6' # 单类型, select

DISPLAY_STYLE_1 = 'display: inline;'
DISPLAY_STYLE_2 = 'display: inline-block;'
DISPLAY_STYLE_3 = 'display: block;'
DISPLAY_STYLE_4 = 'display: none;'

# 悬浮窗口
FLOATING_WINDOW_1 = '#toTop' # 返回顶部
FLOATING_WINDOW_2 = '#miaov_float_layer' # 临时通知页面
FLOATING_WINDOW_3 = '.footArea' # 底部

# 数据表信息介绍
# 如 "显示第 1 到第 1 条记录，总共 1 条记录"
PAGE_INFO_PATTERN = re.compile(
    r"^显示第\s*"
    r"(?P<r1>[0-9]+)"
    r"\s*到第\s*"
    r"(?P<r2>[0-9]+)"
    r"\s*条记录，总共\s*"
    r"\s*(?P<rsum>[0-9]+)\s*"
    r"\s*条记录$"
)

# 页面首次加载判断
INIT_CSS = '#indexNavigation a[class="active"]'

## 数据搜索页 ##
BROWSE_QUERY_CSS = '.dataBrowseBtn'         # 预览数据 按钮
BROWSE_CONTENT_ROOT_CSS = '.databrowse-tree .tree > li:nth-child(1)'
BROWSE_CONTENT_LEAF_CSS_FMT = ' > ul:nth-child(2) > li:nth-child({})'


# 预览数据 响亮标识
ONLOADING_CSS_1 = '.onloading'           # 数据正在加载中, 则display: inline
TIP_CSS_1 = '.tips'                      # 查询数据量超过了20000条的限制，结果只展示20000条数据
TIMEOUT_CSS = '.timeout'                 # 单次请求的数据量过大，结果只展示部分股票代码的返回数据
BUSY_CSS = '.sysbusy'                    # 系统繁忙
NODATA_CSS = '.no-records-found'         # 无数据
CALCELL_CSS = '.cancel'                  # 数据获取请求超时
MIN_ROWS_PER_PAGE_2 = 10

DATA_BROWSE_ITEMS = {
    '1.1':   ('基本资料', ()),
    '2.1':   ('公司股东实际控制人', (SETTING_CONDITION_3, )),
    '2.2':   ('公司股本变动', (SETTING_CONDITION_3, )),
    '2.3':   ('上市公司高管持股变动', (SETTING_CONDITION_3, )),
    '2.4':   ('股东增（减）持情况', (SETTING_CONDITION_3, )),
    '2.5':   ('持股集中度', (SETTING_CONDITION_1, SETTING_CONDITION_2)),
    '3.1':   ('行情数据', (SETTING_CONDITION_3, )),
    '4.1':   ('投资评级', (SETTING_CONDITION_3, )),
    '5.1':   ('上市公司业绩预告', (SETTING_CONDITION_1, SETTING_CONDITION_2)),
    '6.1':   ('分红指标', (SETTING_CONDITION_4, )),
    '7.1':   ('公司增发股票预案', (SETTING_CONDITION_3, )),
    '7.2':   ('公司增发股票实施方案', (SETTING_CONDITION_3, )),
    '7.3':   ('公司配股预案', (SETTING_CONDITION_3, )),
    '7.4':   ('公司配股实施方案', (SETTING_CONDITION_3, )),
    '7.5':   ('公司首发股票', ()),
    '8.1.1': ('个股TTM财务利润表', (SETTING_CONDITION_1, SETTING_CONDITION_2)),
    '8.1.2': ('个股TTM现金流量表', (SETTING_CONDITION_1, SETTING_CONDITION_2)),
    '8.2.1': ('个股单季财务利润表', (SETTING_CONDITION_1, SETTING_CONDITION_2)),
    '8.2.2': ('个股单季现金流量表', (SETTING_CONDITION_1, SETTING_CONDITION_2)),
    '8.2.3': ('个股单季财务指标', (SETTING_CONDITION_1, SETTING_CONDITION_2)),
    '8.3.1': ('个股报告期资产负债表', (SETTING_CONDITION_1, SETTING_CONDITION_2)),
    '8.3.2': ('个股报告期利润表', (SETTING_CONDITION_1, SETTING_CONDITION_2)),
    '8.3.3': ('个股报告期现金表', (SETTING_CONDITION_1, SETTING_CONDITION_2)),
    '8.3.4': ('金融类资产负债表2007版', (SETTING_CONDITION_1, SETTING_CONDITION_2)),
    '8.3.5': ('金融类利润表2007版', (SETTING_CONDITION_1, SETTING_CONDITION_2)),
    '8.3.6': ('金融类现金流量表2007版', (SETTING_CONDITION_1, SETTING_CONDITION_2)),
    '8.4.1': ('个股报告期指标表', (SETTING_CONDITION_1, SETTING_CONDITION_2)),
    '8.4.2': ('财务指标行业排名', (SETTING_CONDITION_1, SETTING_CONDITION_2)),
}


## 专题统计页 ##
STATS_QUERY_CSS = '.thematicStatisticsBtn'               # 查询按钮
STATS_CONTENT_ROOT_CSS = '.thematicStatistics-tree'
STATS_CONTENT_LEAF_CSS_FMT = ' > ul > li:nth-child({})'

ONLOADING_CSS_2 = '.fixed-table-loading'
TIP_CSS_2 = '.tip'                        # 系统繁忙，数据加载失败，请稍后再次点击“查询”按钮刷新数据
MIN_ROWS_PER_PAGE_2 = 20

STATS_ITEMS = {
    '1.1':   ('大宗交易报表', (SETTING_CONDITION_5, )),
    '2.1':   ('融资融券明细', (SETTING_CONDITION_5, )),
    '3.1':   ('解禁报表明细', (SETTING_CONDITION_5, )),
    '4.1':   ('按天减持明细', (SETTING_CONDITION_5, )),
    '4.2':   ('按天增持明细', (SETTING_CONDITION_5, )),
    '4.3':   ('减持汇总统计', (SETTING_CONDITION_3, )),
    '4.4':   ('增持汇总统计', (SETTING_CONDITION_3, )),
    '5.1':   ('股本情况', (SETTING_CONDITION_6, )),
    '5.2':   ('高管持股变动明细', (SETTING_CONDITION_3, SETTING_CONDITION_6)),
    '5.3':   ('高管持股变动汇总', (SETTING_CONDITION_3, SETTING_CONDITION_6)),
    '5.4':   ('实际控制人持股变动', (SETTING_CONDITION_6)),
    '5.5':   ('股东人数及持股集中度', (SETTING_CONDITION_1, SETTING_CONDITION_2)),
    '6.1':   ('业绩预告', (SETTING_CONDITION_1, SETTING_CONDITION_2)),
    '6.2':   ('预告业绩扭亏个股', (SETTING_CONDITION_1, SETTING_CONDITION_2)),
    '6.3':   ('预告业绩大幅下降个股', (SETTING_CONDITION_1, SETTING_CONDITION_2)),
    '6.4':   ('预告业绩大幅上升个股', (SETTING_CONDITION_1, SETTING_CONDITION_2)),
    '7.1':   ('个股定报主要指标', (SETTING_CONDITION_1, SETTING_CONDITION_2))
}