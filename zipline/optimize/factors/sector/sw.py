"""

申万行业敞口

"""

from zipline.pipeline.builtin import SWSector

from .base import SWSectorExposure


class Agriculture(SWSectorExposure):
    sector_code = SWSector.AGRICULTURE


class Mining(SWSectorExposure):
    sector_code = SWSector.MINING


class Chemicals(SWSectorExposure):
    sector_code = SWSector.CHEMICALS


class Steel(SWSectorExposure):
    sector_code = SWSector.STEEL


class Metals(SWSectorExposure):
    sector_code = SWSector.METALS


class Electronics(SWSectorExposure):
    sector_code = SWSector.ELECTRONICS


class Appliances(SWSectorExposure):
    sector_code = SWSector.APPLIANCES


class Food(SWSectorExposure):
    sector_code = SWSector.FOOD


class Textiles(SWSectorExposure):
    sector_code = SWSector.TEXTILES


class LightManufacturing(SWSectorExposure):
    sector_code = SWSector.LIGHT_MANUFACTURING


class Pharmaceuticals(SWSectorExposure):
    sector_code = SWSector.PHARMACEUTICALS


class Utilities(SWSectorExposure):
    sector_code = SWSector.UTILITIES


class Transportation(SWSectorExposure):
    sector_code = SWSector.TRANSPORTATION


class RealEstate(SWSectorExposure):
    sector_code = SWSector.REAL_ESTATE


class Commerce(SWSectorExposure):
    sector_code = SWSector.COMMERCE


class Services(SWSectorExposure):
    sector_code = SWSector.SERVICES


class Conglomerate(SWSectorExposure):
    sector_code = SWSector.CONGLOMERATE


class BuildingMaterials(SWSectorExposure):
    sector_code = SWSector.BUILDING_MATERIALS


class BuildingDecorations(SWSectorExposure):
    sector_code = SWSector.BUILDING_DECORATIONS


class Electricals(SWSectorExposure):
    sector_code = SWSector.ELECTRICALS


class DefenseMilitary(SWSectorExposure):
    sector_code = SWSector.DEFENSE_MILITARY


class IT(SWSectorExposure):
    sector_code = SWSector.IT


class Media(SWSectorExposure):
    sector_code = SWSector.MEDIA


class CommunicationServices(SWSectorExposure):
    sector_code = SWSector.COMMUNICATION_SERVICES


class Banks(SWSectorExposure):
    sector_code = SWSector.BANKS


class NonbankFinancials(SWSectorExposure):
    sector_code = SWSector.NONBANK_FINANCIALS


class Auto(SWSectorExposure):
    sector_code = SWSector.AUTO


class Machinery(SWSectorExposure):
    sector_code = SWSector.MACHINERY
