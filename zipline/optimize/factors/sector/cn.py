"""

国证行业敞口

"""
from zipline.pipeline.builtin import Sector

from .base import CNSectorExposure


class BasicMaterials(CNSectorExposure):
    sector_code = Sector.BASIC_MATERIALS


class ConsumerCyclical(CNSectorExposure):
    sector_code = Sector.BASIC_MATERIALS


class FinancialServices(CNSectorExposure):
    sector_code = Sector.FINANCIAL_SERVICES


class RealEstate(CNSectorExposure):
    sector_code = Sector.REAL_ESTATE


class ConsumerDefensive(CNSectorExposure):
    sector_code = Sector.CONSUMER_DEFENSIVE


class HealthCare(CNSectorExposure):
    sector_code = Sector.HEALTHCARE


class Utilities(CNSectorExposure):
    sector_code = Sector.UTILITIES


class CommunicationServices(CNSectorExposure):
    sector_code = Sector.COMMUNICATION_SERVICES


class Energy(CNSectorExposure):
    sector_code = Sector.ENERGY


class Industrials(CNSectorExposure):
    sector_code = Sector.INDUSTRIALS


class Technology(CNSectorExposure):
    sector_code = Sector.TECHNOLOGY
