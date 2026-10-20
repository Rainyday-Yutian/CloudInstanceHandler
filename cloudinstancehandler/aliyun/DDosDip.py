# CloudInstanceHandler/cloudinstancehandler/aliyun/DDosDip.py

import pandas as pd
from typing import List, Dict, Any

from cloudinstancehandler.common.funcs import *
from cloudinstancehandler.aliyun.AliyunInstance import AliyunInstance

# --- 新版 Tea SDK 导入 ---
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_antiddos_public20170518.client import Client as AntiddosClient
from alibabacloud_antiddos_public20170518 import models as antiddos_models


class DDoSDip(AliyunInstance):
    def __init__(self, access_key, secret_key) -> None:
        super().__init__(access_key, secret_key)
        self.namespace = "acs_ddosdip"
        self.ProductCategory = "ddosdip"
