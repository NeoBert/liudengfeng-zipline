import re

import pytest

from zipline.pipeline.fundamentals.localdata import (TO_DORP_PAT_0,
                                                     TO_DORP_PAT_1,
                                                     _normalized_col_name)


@pytest.mark.parametrize("x,expected", [
    ('四、汇率变动对现', '汇率变动对现'),
    ('四(2)、其他原因对', '其他原因对'),
    ('五、现金及现金等', '现金及现金等'),
    ('（一）基本每股收益', '基本每股收益'),
    ('（二）稀释每股收益', '稀释每股收益'),
])
def test_sub_1(x, expected):
    """测试去除前导大写数字"""
    actual = re.sub(TO_DORP_PAT_0, '', x)
    assert actual == expected


@pytest.mark.parametrize("x,expected", [
    ('1、将净利润调节为', '将净利润调节为'),
    ('2、不涉及现金收', '不涉及现金收'),
    ('2类股东', '2类股东'),
])
def test_sub_2(x, expected):
    """测试前导小写数字"""
    actual = re.sub(TO_DORP_PAT_1, '', x)
    assert actual == expected


@pytest.mark.parametrize("x,expected", [
    ('其中：子公司吸收', '其中子公司吸收'),
    ('2、不涉及现金收支', '不涉及现金收支'),
    ('分配股利、利润或', '分配股利利润或'),
    ('固定资产折旧、油气资产折耗、生产性生物资产折旧', '固定资产折旧油气资产折耗生产性生物资产折旧'),
    ('其中_子公司支付给少数股东的股利_利润', '其中子公司支付给少数股东的股利利润'),
    ('净资产收益率-加权(扣除非经常性损益)', '净资产收益率加权扣除非经常性损益'),
    ('净资产收益率-加权', '净资产收益率加权'),
    ('EBIT', 'EBIT'),
])
def test_sub_3(x, expected):
    """测试列名称规范"""
    actual = _normalized_col_name(x)
    assert actual == expected
