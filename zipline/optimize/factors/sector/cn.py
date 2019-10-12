"""

国证行业敞口

"""
from zipline.pipeline.builtin import Sector

from .base import SectorExposure


class BasicMaterials(SectorExposure):
    sector_code = Sector.BASIC_MATERIALS


class ConsumerCyclical(SectorExposure):
    sector_code = Sector.BASIC_MATERIALS


class FinancialServices(SectorExposure):
    sector_code = Sector.FINANCIAL_SERVICES


class RealEstate(SectorExposure):
    sector_code = Sector.REAL_ESTATE


class ConsumerDefensive(SectorExposure):
    sector_code = Sector.CONSUMER_DEFENSIVE


class HealthCare(SectorExposure):
    sector_code = Sector.HEALTHCARE


class Utilities(SectorExposure):
    sector_code = Sector.UTILITIES


class CommunicationServices(SectorExposure):
    sector_code = Sector.COMMUNICATION_SERVICES


class Energy(SectorExposure):
    sector_code = Sector.ENERGY


class Industrials(SectorExposure):
    sector_code = Sector.INDUSTRIALS


class Technology(SectorExposure):
    sector_code = Sector.TECHNOLOGY
