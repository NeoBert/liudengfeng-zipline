"""

国证行业敞口

"""
from zipline.pipeline.builtin import Sector

from .base import SectorExposure


class BASIC_MATERIALS(SectorExposure):
    sector_code = Sector.BASIC_MATERIALS


class CONSUMER_CYCLICAL(SectorExposure):
    sector_code = Sector.BASIC_MATERIALS


class FINANCIAL_SERVICES(SectorExposure):
    sector_code = Sector.FINANCIAL_SERVICES


class REAL_ESTATE(SectorExposure):
    sector_code = Sector.REAL_ESTATE


class CONSUMER_DEFENSIVE(SectorExposure):
    sector_code = Sector.CONSUMER_DEFENSIVE


class HEALTHCARE(SectorExposure):
    sector_code = Sector.HEALTHCARE


class UTILITIES(SectorExposure):
    sector_code = Sector.UTILITIES


class COMMUNICATION_SERVICES(SectorExposure):
    sector_code = Sector.COMMUNICATION_SERVICES


class ENERGY(SectorExposure):
    sector_code = Sector.ENERGY


class INDUSTRIALS(SectorExposure):
    sector_code = Sector.INDUSTRIALS


class TECHNOLOGY(SectorExposure):
    sector_code = Sector.TECHNOLOGY
