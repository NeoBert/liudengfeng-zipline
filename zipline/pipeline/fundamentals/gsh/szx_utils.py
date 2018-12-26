import re
import time
import math
import random
import pandas as pd
from operator import itemgetter

from selenium.webdriver.common.by import By 
from selenium.webdriver.common.alert import Alert
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.common.exceptions import (
    NoSuchElementException, 
    NoAlertPresentException,
    UnexpectedAlertPresentException,
    ElementNotInteractableException,
    ElementClickInterceptedException,
    StaleElementReferenceException
)
from ..utils.exceptions import (
    NoWebData,
    DataOverLimit,
    DataLoadingException, 
    InvalidParamException
)
from .szx_constants import (
    SETTING_CONDITION_1,
    SETTING_CONDITION_2,
    SETTING_CONDITION_3,
    SETTING_CONDITION_4,
    SETTING_CONDITION_5,
    SETTING_CONDITION_6,
    DISPLAY_STYLE_1,
    DISPLAY_STYLE_2,
    DISPLAY_STYLE_3,
    DISPLAY_STYLE_4,
    FLOATING_WINDOW_1,
    FLOATING_WINDOW_2,
    FLOATING_WINDOW_3,
    BROWSE_QUERY_CSS,
    BROWSE_CONTENT_ROOT_CSS,
    BROWSE_CONTENT_LEAF_CSS_FMT,
    STATS_QUERY_CSS,
    STATS_CONTENT_ROOT_CSS,
    STATS_CONTENT_LEAF_CSS_FMT,
    ONLOADING_CSS_1,
    ONLOADING_CSS_2,
    TIP_CSS_1,
    TIP_CSS_2,
    TIMEOUT_CSS,
    BUSY_CSS,
    NODATA_CSS,
    CALCELL_CSS,
    PAGE_INFO_PATTERN,
)


class element_to_be_clickable(object):
    def __init__(self, element):
        self.element = element

    def __call__(self, driver):
        if self.element.is_displayed() and self.element.is_enabled():
            return self.element
        else:
            return False


class attr_to_be_present_in_element(object):
    def __init__(self, element, attr, value):
        self.element = element
        self.attr = attr
        self.value = value

    def __call__(self, driver):
        if self.element.get_attribute(self.attr) == self.value:
            return True
        return False 


class element_can_be_found(object):
    def __init__(self, css):
        self.css = css

    def __call__(self, driver):
        try:
            return driver.find_element_by_css_selector(self.css)
        except NoSuchElementException:
            return False


def infer_exchange_from_code(stock_code):
    """股票市场分类"""
    f = stock_code[0]
    if f == '2':
        raise
    elif f == '9':
        raise
    elif f == '6':
        return '6.7' # '沪市A'
    else:
        return '6.1' # '深市A'
    
            
def get_count(elem):
    """解析元素中提示数量信息"""
    if elem.tag_name != 'i':
        # 用i元素的值
        i = elem.find_element_by_tag_name('i')
    else:
        i = elem
    try:
        return int(i.text)
    except:
        return -1000  
            
            
def toggler_open(browser, li, wait=None):
    """展开树形折叠项目"""
    attr = li.get_attribute('class')
    if attr == 'tree-closed':
        elem = li.find_element_by_css_selector('span')
        safe_click(browser, elem, wait)
    
        if isinstance(wait, WebDriverWait):
            elem = li.find_element_by_class_name('treemenu')
            wait.until(attr_to_be_present_in_element(elem, 'style', DISPLAY_STYLE_3))

            
def safe_click(browser, elem, wait=None):
    if isinstance(wait, WebDriverWait):
        elem = wait.until(element_to_be_clickable(elem))
    elem.click()


def get_level_and_label(level_or_label, mappings):
    """
    返回tuple(层级, 名称)
    """
    level_or_label = level_or_label.strip()
    if '.' in level_or_label: # level
        level = level_or_label
        label = mappings[level_or_label][0]
    else:
        label = level_or_label # labels
        for k, v in mappings.items():
            if v[0] == label:
                break
        level = k
    return level, label


def select_data_item(browser, level, root_css, leaf_css_fmt, wait=None):
    """选择对应层级的数据项目"""    
    level = str(level).split('.')
    
    css = root_css
    for lvl in level:
        css += leaf_css_fmt.format(lvl)
        elem = browser.find_element_by_css_selector(css)
        toggler_open(browser, elem, wait)
    
    elem = elem.find_element_by_css_selector('a')
    safe_click(browser, elem, wait)


def set_filters(browser, cond, *args):
    """设置查询条件"""    
    nargs = 0 # number of args used in args
    
    if not cond.startswith('.'):
        assert cond in range(1, 7), '查询条件错误'
        cond = '.condition{}'.format(cond)
    
    if not cond.startswith('.condition'):
        raise Exception('查询条件错误')
        
    elem = browser.find_element_by_css_selector(cond)
    
    if elem.get_attribute('style') != DISPLAY_STYLE_2:
        raise InvalidParamException('{}设定错误'.format(cond))
    
    if cond in (SETTING_CONDITION_1, SETTING_CONDITION_4, SETTING_CONDITION_5):
        elem = elem.find_element_by_css_selector('input[type="text"]')
        elem.clear()
        elem.send_keys(args[0])
        nargs = 1

    elif cond in (SETTING_CONDITION_2, SETTING_CONDITION_6):
        elem = Select(elem.find_element_by_css_selector('select'))
        elem.select_by_index(args[0])
        nargs = 1
        
    elif cond == SETTING_CONDITION_3:
        dt1, dt2 = elem.find_elements_by_css_selector('input[type="text"]')
        
        dt1.clear()
        dt1.send_keys(args[0])
        
        dt2.clear()
        dt2.send_keys(args[1])
        
        nargs = 2

    return nargs


def query(browser, btn_css, response_css, wait):
    btn_elem = browser.find_element_by_css_selector(btn_css)
    safe_click(browser, btn_elem, wait)
    
    locator = (By.CSS_SELECTOR, response_css)
    m = EC.invisibility_of_element_located(locator)
    wait.until(m, message='查询数据超时')
    
    _check_query_response(browser)


def _check_query_response(browser):
    try:
        browser.find_element_by_css_selector(NODATA_CSS)
        raise NoWebData('无数据')
    except NoSuchElementException:
        time.sleep(0.2)
        
    e = {
        TIP_CSS_2: '系统繁忙，数据加载失败',
        BUSY_CSS: '系统繁忙', 
        CALCELL_CSS: '数据获取请求超时',
    }
    for css, txt in e.items():
        try:
            elem = browser.find_element_by_css_selector(css)
        except NoSuchElementException:
            continue
        
        if elem.get_attribute('style') != DISPLAY_STYLE_4:
            raise DataLoadingException(txt)
        
    e = {
        TIP_CSS_1: '查询数据量超过了20000条的限制，结果只展示20000条数据',
        TIMEOUT_CSS: '单次请求的数据量过大，结果只展示部分股票代码的返回数据',
    }
    for css, txt in e.items():
        try:
            elem = browser.find_element_by_css_selector(css)
        except NoSuchElementException:
            continue
        
        if elem.get_attribute('style') != DISPLAY_STYLE_4:
            raise DataOverLimit(txt)    
    

def flip_to_page(browser, p, wait=None):
    elem = browser.find_element_by_link_text(str(p))
    scroll_window(browser, elem)
    safe_click(browser, elem, wait)

        
def get_page_numbers(browser, wait):
    _get = itemgetter('r1', 'r2', 'rsum')
    
    css = '.pagination-info'
    elem = wait.until(element_can_be_found(css))

    match = PAGE_INFO_PATTERN.match(elem.text)
    
    if match is None:
        time.sleep(10)
        raise ValueError("Couldn't parse page information")
    
    r1, r2, rsum = map(int, _get(match.groupdict()))
    
    if r2 == rsum:
        npages = 1
    elif r2 != r1:
        max_rows_per_page = r2 - r1 + 1
        npages = math.ceil(rsum / max_rows_per_page)
    else:
        raise Exception("Refering number of pages is accurate at the last page")
        
    return npages, rsum


def change_row_limit(browser, wait=None):
    """数据正常响应后，更改每页显示行数"""
    css = 'li[role="menuitem"]'
    nth = len(browser.find_elements_by_css_selector(css))
    
    css = '.dropdown-toggle'
    elem = browser.find_element_by_css_selector(css)
    safe_click(browser, elem, wait)
            
    css = '.dropdown-menu > li:nth-child({})'.format(nth)
    elem = browser.find_element_by_css_selector(css)
    safe_click(browser, elem, wait)


def prune_webpage(browser):
    '''关闭悬浮窗口'''
    floating_window_css = [
        FLOATING_WINDOW_1, # 返回顶部
        FLOATING_WINDOW_2, # 临时通知页面
    ]
    
    for css in floating_window_css:
        try:
            elem = browser.find_element_by_css_selector(css)
            browser.execute_script("arguments[0].style.display = 'none';", elem)
        except NoSuchElementException:
            pass


def scroll_window(browser, elem=None):
    '''To monitor being Not Stale'''
    if elem:
        browser.execute_script("arguments[0].scrollIntoView();", elem)
    else:        
        x = random.randint(1, 10000)
        y = random.randint(1, 10000)
        browser.execute_script("window.scrollTo({}, {})".format(x, y))
        browser.execute_script("window.scrollTo(0, document.body.scrollHeight)")


def log_alert(browser, logger):
    """提示当前网页可能存在的警告信息"""
    msg = None
    try:
        alert = browser.switch_to.alert
        msg = alert.text
        alert.accept()
        time.sleep(1)
        logger.error(msg)
    except NoAlertPresentException:
        pass


def add_codes(browser, wait):
    """点击添加股票代码的按钮"""        
    add_css = 'div.detail-cont-top div.arrows-box:nth-child(2) > div:nth-child(1) > button:nth-child(1)'
    elem = browser.find_element_by_css_selector(add_css)
    safe_click(browser, elem, wait)


def choose_single_code(browser, wait, symbol):
    css_fmt = 'div.select-box:nth-child(1) > div:nth-child(3) > ul:nth-child(1) span[data-id="{}"]'
    css = css_fmt.format(symbol)
    elem = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, css)))
    safe_click(browser, elem, wait)


def choose_all_codes(browser, wait):
    init_css = 'div.select-box:nth-child(1) > div:nth-child(3) > ul:nth-child(1) .my_protocol'
    chk_css = 'div.detail-cont-top > div.cont-top-right.clearfix > div.select-box.left label > i'
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, init_css)))
    elem = browser.find_element_by_css_selector(chk_css)
    safe_click(browser, elem, wait)


def wait_code_loaded(wait):
    """数据搜索页面：更改市场分类后，等待代码完成加载"""
    def f(br):
        css = 'div.select-box:nth-child(1) > div:nth-child(1) > span:nth-child(2)'
        i = br.find_element_by_css_selector(css)
        return get_count(i) >= 0
    
    wait.until(f, '代码加载超时')

        
def choose_data_fields(browser, wait):
    """数据搜索页面：全选数据字段"""
    check_css = 'div.select-box:nth-child(2) > div:nth-child(1) > span:nth-child(2)'
    elem = browser.find_element_by_css_selector(check_css)
    if get_count(elem) > 0:
        css = '.detail-cont-bottom label > i'
        elem = browser.find_element_by_css_selector(css)
        safe_click(browser, elem, wait)
    
        btn_css = '.detail-cont-bottom button[class="arrow-btn right"]'
        elem = browser.find_element_by_css_selector(btn_css)
        safe_click(browser, elem, wait)