# -*- coding: utf8 -*-
"""
深证信数据服务平台
"""
import sys
import time
import logbook
from logbook.more import ColorizedStderrHandler

import pandas as pd

from .szx_constants import (
    INIT_CSS,
    STATS_ITEMS,
    DATA_BROWSE_ITEMS,
    MAX_WAIT_SECOND,
    POLL_FREQUENCY,
    MAX_RELOAD_TIMES,
    MIN_ROWS_PER_PAGE_2,
    DISPLAY_STYLE_2,
    BROWSE_QUERY_CSS,
    BROWSE_CONTENT_ROOT_CSS,
    BROWSE_CONTENT_LEAF_CSS_FMT,
    STATS_QUERY_CSS,
    STATS_CONTENT_ROOT_CSS,
    STATS_CONTENT_LEAF_CSS_FMT,
    ONLOADING_CSS_1,
    ONLOADING_CSS_2,
    
)
from .szx_utils import (
    get_level_and_label,
    select_data_item,
    set_filters,
    query,
    get_page_numbers,
    flip_to_page,
    change_row_limit,
    prune_webpage,
    attr_to_be_present_in_element,
    infer_exchange_from_code,
    log_alert,
    add_codes,
    choose_single_code,
    choose_all_codes,
    choose_data_fields,
    wait_code_loaded,
)
from ..utils.web_utils import (
    make_headless_browser, 
    clear_firefox_cache
)
from ..utils.input_utils import ensure_list
from ..utils.exceptions import (
    NoWebData, 
    DataOverLimit,
    DataLoadingException
)
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


logbook.set_datetime_format('local')
#handler = ColorizedStderrHandler()
#handler.push_application()

logger = logbook.Logger('深证信')


class DataBrowse(object):
    """深证信数据服务平台数据搜索Api"""
    url = 'http://webapi.cninfo.com.cn/#/dataBrowse'

    def __init__(self, headless=True, clear_cache=False):
        self.browser = make_headless_browser(headless)
        self.wait = WebDriverWait(self.browser, MAX_WAIT_SECOND, POLL_FREQUENCY)
        if clear_cache:
            clear_firefox_cache(self.browser)
            logger.info('完成初始化及缓存清理')

        self._page_loaded = False
        self.mappings = DATA_BROWSE_ITEMS
        self.check_loaded_text = '基本资料'
        self.current_page = 0
        self.current_level = None
        
        # 加载网页
        self.load_page()         

    def query(self, indicator, categories, symbols, *args):
        """
        查询选定代码期间指标数据
        ----------------------------------------------
        """
        # 设置数据tab
        level, _ = get_level_and_label(indicator, self.mappings)
        self._select_data_item(level) 
        
        # 时间参数
        conditions = self.mappings[level][1]
        self._set_filters(conditions, *args)
        
        # 设置股票列表
        if symbols is None:
            for cat in ensure_list(categories):
                self._change_category(cat)
                self._add_codes()
        else:
            # 排序代码， 减少类别切换
            symbols = sorted(ensure_list(symbols))
            for s in symbols:
                self._change_category(infer_exchange_from_code(s))
                self._add_codes(s)
        
        # 字段选取, 全选字段
        self._choose_data_fields()        

        # 加载返回结果
        i = 0
        try: 
            self._load_results() 
            i += 1
        except NoWebData as e:
            logger.warning(e)
            return pd.DataFrame()
        except DataOverLimit as e:
            logger.error(e)
            sys.exit()
        except DataLoadingException as e:
            logger.warning(e)
            
            if i > MAX_RELOAD_TIMES:
                logger.error('>>> 数据加载失败，请重新运行')
                sys.exit()
            
            time.sleep(1)
            logger.info('尝试重新加载中')
            self._load_results()
            i += 1

        logger.info('>> 数据加载完成，开始读取')
        
        # 去掉悬浮窗口，以防影响点击        
        self._prune_webpage() 
        # 设置每页最大行数
        self._change_row_limit()  
        # 读取数据结果
        df = self._read_tables() 
        
        logger.info('<<< 共{}行数据'.format(df.shape[0]))
        
        return df

    def load_page(self):
        """加载网页"""
        check_css_1 = '#se1_sele'
        check_css_2 = '#se2_sele'
        
        i = 0
        cond = True
        while cond & (i <= MAX_RELOAD_TIMES):
            self._load_page()
            
            cond = any([
                len(self.browser.find_elements_by_css_selector(x)) != 1
                for x in [check_css_1, check_css_2]
            ])
            
            i += 1
            time.sleep(1)
                
    def _load_page(self):
        if self._page_loaded:
            self.browser.refresh()
            self._alert_to_log()
            logger.info('完成页面刷新')
        else:
            self.browser.get(self.url)
            logger.info('完成页面首次加载')
        
        check = self.check_loaded_text
        locator = (By.CSS_SELECTOR, INIT_CSS)
        m = EC.text_to_be_present_in_element(locator, check)
        self.wait.until(m, message='页面加载超时')
        
        self._page_loaded = True

    def _alert_to_log(self):
        log_alert(self.browser, logger)
    
    def _select_data_item(self, level):
        br = self.browser
        root_css = BROWSE_CONTENT_ROOT_CSS
        leaf_css_fmt = BROWSE_CONTENT_LEAF_CSS_FMT
        
        if self.current_level == level:
            logger.info('>> 数据项目无需更改')
        else:
            select_data_item(br, level, root_css, leaf_css_fmt, self.wait)
            self.current_level = level
            logger.info('>> 数据项目更改完成')
            
    def _set_filters(self, conds, *args):
        br = self.browser
        wait = self.wait
        for cond in conds:
            elem = br.find_element_by_css_selector(cond)
            wait.until(attr_to_be_present_in_element(elem, 'style', DISPLAY_STYLE_2))
            used_arg_number = set_filters(br, cond, *args)
            args = args[used_arg_number:]
        logger.info('>> 参数输入更改完成')
    
    def _change_category(self, category):
        """更改市场分类"""
        self._select_category(category)
        wait_code_loaded(self.wait)
        logger.info('>> {}代码分类更改完成'.format(category))
    
    def _select_category(self, level):
        """选择市场分类，准备选择查询代码"""
        root_css = '.cont-top-left > div:nth-child(2)'
        leaf_css_fmt = ' > ul > li:nth-child({})'
        
        br = self.browser
        wait = self.wait
        
        select_data_item(br, level, root_css, leaf_css_fmt, wait)

    def _add_codes(self, symbol=None):
        """添加查询代码"""
        br = self.browser
        wait = self.wait
        if symbol:
            choose_single_code(br, wait, symbol)
        else:
            choose_all_codes(br, wait)
            
        add_codes(br, wait)
        logger.info('>> 输入代码添加完成')

    def _choose_data_fields(self):
        """全选字段"""
        choose_data_fields(self.browser, self.wait)
        
    def _load_results(self):
        btn_css = BROWSE_QUERY_CSS
        response_css = ONLOADING_CSS_1
        query(self.browser, btn_css, response_css, self.wait)
        
    def _change_row_limit(self):
        if self.num_of_rows > MIN_ROWS_PER_PAGE_2:
            change_row_limit(self.browser, self.wait)
            logger.info('>> 调整为最大行数显示')
    
    def _prune_webpage(self):
        '''去掉悬浮窗口，以防影响点击'''
        prune_webpage(self.browser)
    
    def _read_tables(self):
        """读取当前网页的数据表格"""   
        page_num = self.num_of_pages
        
        dfs = []
        for i in range(1, page_num+1):
            logger.info('>>>  分页进度 第{}页/共{}页'.format(i, page_num))
            dfs.append(self._read_table().dropna(how='all').drop_duplicates())
            self._flip_to_page(i+1, page_num)
            
        return pd.concat(dfs)    
    
    @property
    def num_of_pages(self):
        return get_page_numbers(self.browser, self.wait)[0]
    
    @property
    def num_of_rows(self):
        return get_page_numbers(self.browser, self.wait)[1]    
        
    def _read_table(self):
        br = self.browser
        try:
            df = pd.read_html(br.page_source, na_values=['-'])[0]
        except:
            df = pd.DataFrame()
        return df
    
    def _flip_to_page(self, p, total_pages):
        if (self.current_page != p) & (p <= total_pages) & (p > 1):
            flip_to_page(self.browser, p, self.wait)
        
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.browser.quit()


class ThematicStatistics(object):
    """深证信数据服务平台专题统计Api"""
    url = 'http://webapi.cninfo.com.cn/#/thematicStatistics'

    def __init__(self, headless=True, clear_cache=False):
        self.browser = make_headless_browser(headless)
        self.wait = WebDriverWait(self.browser, MAX_WAIT_SECOND, POLL_FREQUENCY)
        if clear_cache:
            clear_firefox_cache(self.browser)
            logger.info('完成初始化及缓存清理')

        self._page_loaded = False
        self.mappings = STATS_ITEMS
        self.check_loaded_text = '大宗交易报表'
        self.current_page = 0
        self.current_level = None
        
        # 加载网页
        self.load_page()

    def query(self, indicator, *args):
        """
        查询选定代码期间指标数据
        ----------------------------------------------
        """
        level, _ = get_level_and_label(indicator, self.mappings)
        self._select_data_item(level) # 设置数据指标
        
        conditions = self.mappings[level][1]
        self._set_filters(conditions, *args) # 设置条件参数
        
        i = 0
        try: 
            self._load_results() # 加载返回结果
            i += 1
        except NoWebData as e:
            logger.warning(e)
            return pd.DataFrame()
        except DataOverLimit as e:
            logger.error(e)
            sys.exit()          
        except DataLoadingException as e:
            logger.warning(e)
          
            if i > MAX_RELOAD_TIMES:
                logger.error('>>> 数据加载失败，请重新运行')
                sys.exit()
            
            time.sleep(1)
            logger.info('尝试重新加载中')
            self._load_results()
            i += 1

        logger.info('>> 数据加载完成，开始读取')
        
        self._change_row_limit() # 设置每页最大行数
        
        self._prune_webpage() # 去掉悬浮窗口，以防影响点击
        df = self._read_tables() # 读取数据结果
        
        logger.info('<<< 共{}行数据'.format(df.shape[0]))
        
        return df

    def load_page(self):
        """加载网页"""
        br = self.browser
        check = self.check_loaded_text
        if not self._page_loaded:
            br.get(self.url)
            
            while(br.current_url != self.url):
                br.get(self.url)
                logger.info('>>> 页面加载错误，重复加载中')
                time.sleep(1)
            
            if check: # 等待页面加载完成
                locator = (By.CSS_SELECTOR, INIT_CSS)
                m = EC.text_to_be_present_in_element(locator, check)
                self.wait.until(m, message='页面加载超时')
        self._page_loaded = True
    
    def _select_data_item(self, level):
        br = self.browser
        root_css = STATS_CONTENT_ROOT_CSS
        leaf_css_fmt = STATS_CONTENT_LEAF_CSS_FMT
        
        if self.current_level == level:
            logger.info('>> 数据项目无需更改')
        else:
            select_data_item(br, level, root_css, leaf_css_fmt, self.wait)
            self.current_level = level
            logger.info('>> 数据项目更改完成')
            
    def _set_filters(self, conds, *args):
        br = self.browser
        wait = self.wait
        for cond in conds:
            elem = br.find_element_by_css_selector(cond)
            wait.until(attr_to_be_present_in_element(elem, 'style', DISPLAY_STYLE_2))
            used_arg_number = set_filters(br, cond, *args)
            args = args[used_arg_number:]
        logger.info('>> 参数输入更改完成')
    
    def _load_results(self):
        btn_css = STATS_QUERY_CSS
        response_css = ONLOADING_CSS_2
        query(self.browser, btn_css, response_css, self.wait)
        
    def _change_row_limit(self):
        if self.num_of_rows > MIN_ROWS_PER_PAGE_2:
            change_row_limit(self.browser, self.wait)
            logger.info('>> 调整为最大行数显示')
    
    def _prune_webpage(self):
        '''去掉悬浮窗口，以防影响点击'''
        prune_webpage(self.browser)
    
    def _read_tables(self):
        """读取当前网页的数据表格"""   
        page_num = self.num_of_pages
        
        dfs = []
        for i in range(1, page_num+1):
            logger.info('>>>  分页进度 第{}页/共{}页'.format(i, page_num))
            dfs.append(self._read_table().dropna(how='all').drop_duplicates())
            self._flip_to_page(i+1, page_num)
            
        return pd.concat(dfs)    
    
    @property
    def num_of_pages(self):
        return get_page_numbers(self.browser, self.wait)[0]
    
    @property
    def num_of_rows(self):
        return get_page_numbers(self.browser, self.wait)[1]    
        
    def _read_table(self):
        br = self.browser
        try:
            df = pd.read_html(br.page_source, na_values=['-'])[0]
        except:
            df = pd.DataFrame()
        return df
    
    def _flip_to_page(self, p, total_pages):
        if (self.current_page != p) & (p <= total_pages) & (p > 1):
            flip_to_page(self.browser, p, self.wait)
        
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.browser.quit()