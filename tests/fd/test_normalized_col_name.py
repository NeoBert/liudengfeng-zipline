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
    actual = re.sub(TO_DORP_PAT_0, '', x)
    assert actual == expected


@pytest.mark.parametrize("x,expected", [
    ('1、将净利润调节为', '将净利润调节为'),
    ('2、不涉及现金收', '不涉及现金收'),
])
def test_sub_2(x, expected):
    actual = re.sub(TO_DORP_PAT_1, '', x)
    assert actual == expected


@pytest.mark.parametrize("x,expected", [
    ('其中：子公司吸收', '其中_子公司吸收'),
    ('2、不涉及现金收支', '不涉及现金收支'),
    ('分配股利、利润或', '分配股利_利润或'),
])
def test_sub_3(x, expected):
    actual = _normalized_col_name(x)
    assert actual == expected