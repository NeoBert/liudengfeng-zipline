"""
部门编码
同花顺行业 -> 部门代码
"""
MARKET_MAPS = {
    9: '上海B股',
    6: '上海主板',
    3: '创业板',
    2: '深圳B股',
    1: '中小板',
    0: '深圳主板',
    -1: '未知',
}

SUPER_SECTOR_NAMES = {
    1: ('周期', 'Cyclical'),
    2: ('防御', 'Defensive'),
    3: ('敏感', 'Sensitive'),
}

SECTOR_NAMES = {
    101: ('基本材料', 'BASIC_MATERIALS'),
    102: ('主要消费', 'CONSUMER_CYCLICAL'),
    103: ('金融服务', 'FINANCIAL_SERVICES'),
    104: ('房地产', 'REAL_ESTATE'),
    205: ('可选消费', 'CONSUMER_DEFENSIVE'),
    206: ('医疗保健', 'HEALTHCARE'),
    207: ('公用事业', 'UTILITIES'),
    308: ('通讯服务', 'COMMUNICATION_SERVICES'),
    309: ('能源', 'ENERGY'),
    310: ('工业领域', 'INDUSTRIALS'),
    311: ('工程技术', 'TECHNOLOGY'),
}

QUARTERLY_TABLES = [
    'balance_sheets', 'profit_statements', 'cashflow_statements', 'chnls',
    'cznls', 'ylnls', 'yynls', 'zyzbs'
]
