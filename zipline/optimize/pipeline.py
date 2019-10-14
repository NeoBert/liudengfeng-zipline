from zipline.pipeline import Pipeline
from zipline.pipeline.domain import CN_EQUITIES

from .factors.sector.cn import BasicMaterials
from .factors.sector.cn import CommunicationServices as cn_CS
from .factors.sector.cn import (ConsumerCyclical, ConsumerDefensive, Energy,
                                FinancialServices, HealthCare, Industrials)
from .factors.sector.cn import RealEstate as cn_RE
from .factors.sector.cn import Technology, Utilities
from .factors.sector.sw import (IT, Agriculture, Appliances, Auto, Banks,
                                BuildingDecorations, BuildingMaterials,
                                Chemicals, Commerce)
from .factors.sector.sw import CommunicationServices as sw_CS
from .factors.sector.sw import (Conglomerate, DefenseMilitary, Electricals,
                                Electronics, Food, LightManufacturing,
                                Machinery, Media, Metals, Mining,
                                NonbankFinancials, Pharmaceuticals)
from .factors.sector.sw import RealEstate as sw_RE
from .factors.sector.sw import (Services, Steel, Textiles, Transportation,
                                Utilities)
from .factors.style import STR, Momentum, Size, Value, Volatility


def style_columns():
    """主题列集合"""
    return {
        'momentum': Momentum().zscore(),
        'value': Value().zscore(),
        'size': Size().zscore(),
        'short_term_reversal': STR().zscore(),
        'volatility': Volatility().zscore(),
    }


def cn_sector_columns():
    """国证行业列集合"""
    return {
        'consumer_cyclical': ConsumerCyclical(),
        'consumer_defensive': ConsumerDefensive(),
        'financial_services': FinancialServices(),
        'health_care': HealthCare(),
        'industrials': Industrials(),
        'energy': Energy(),
        'real_estate': cn_RE(),
        'technology': Technology(),
        'utilities': Utilities(),
        'communication_services': cn_CS(),
        'basic_materials': BasicMaterials(),
    }


def sw_sector_columns():
    """申万行业列集合"""
    return {
        'agriculture': Agriculture(),
        'mining': Mining(),
        'chemicals': Chemicals(),
        'steel': Steel(),
        'metals': Metals(),
        'electronics': Electronics(),
        'appliances': Appliances(),
        'food': Food(),
        'textiles': Textiles(),
        'light_manufacturing': LightManufacturing(),
        'pharmaceuticals': Pharmaceuticals(),
        'utilities': Utilities(),
        'transportation': Transportation(),
        'real_estate': sw_RE(),
        'commerce': Commerce(),
        'services': Services(),
        'conglomerate': Conglomerate(),
        'building_materials': BuildingMaterials(),
        'building_decorations': BuildingDecorations(),
        'electricals': Electricals(),
        'defense_military': DefenseMilitary(),
        'it': IT(),
        'media': Media(),
        'communication_services': sw_CS(),
        'banks': Banks(),
        'nonbank_financials': NonbankFinancials(),
        'auto': Auto(),
        'machinery': Machinery()
    }


def risk_loading_pipeline(sector_type='cn'):
    """
    为风险模型创建一个包含所有风险加载pipeline

    返回
    ----
    pipeline:Pipeline
        包含风险模型中每个因子的风险加载的pipeline
    """
    columns = style_columns()
    if sector_type == 'sw':
        columns.update(sw_sector_columns())
    elif sector_type == 'cn':
        columns.update(cn_sector_columns())
    else:
        raise ValueError(f"不支持{sector_type}")
    return Pipeline(columns=columns, domain=CN_EQUITIES)
