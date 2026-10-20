"""
火山引擎云服务模块

统一参考阿里云设计范式重构
"""
from .VolcInstance import VolcInstance
from .common_request import (
    VCMServiceCommonRequest,
    VCMSearchResourcesRequest,
    VCMGetResourceCountsRequest,
)
from .ECS import ECS, ENI, Disks
from .IAM import IAM
from .RDS import RDS
from .Redis import Redis
from .CLB import CLB
from .NAT import NAT
from .VPC import EIP, BandwidthPackage
from .VolcObserve import OBServer
from .Resource import Resource

__all__ = [
    'VolcInstance',
    'VCMServiceCommonRequest',
    'VCMSearchResourcesRequest',
    'VCMGetResourceCountsRequest',
    'ECS',
    'ENI',
    'Disks',
    'IAM',
    'RDS',
    'Redis',
    'CLB',
    'NAT',
    'EIP',
    'BandwidthPackage',
    'OBServer',
    'Resource',
]
