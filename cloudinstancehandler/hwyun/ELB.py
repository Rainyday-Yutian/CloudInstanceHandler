# coding: utf-8
"""
华为云 ELB (弹性负载均衡) 模块
重构版本：统一使用 fetch_instance_info 模式
"""
import pandas as pd
from typing import List, Optional, Dict, Any
from pprint import pprint

from cloudinstancehandler.hwyun.HwyunInstance import HwyunInstance
from cloudinstancehandler.common.funcs import get_time_range

from huaweicloudsdkcore.auth.credentials import BasicCredentials
from huaweicloudsdkcore.exceptions import exceptions
from huaweicloudsdkelb.v3.region.elb_region import ElbRegion
from huaweicloudsdkelb.v3 import *


class ELB(HwyunInstance):
    """
    华为云弹性负载均衡 (ELB)

    重构要点：
    - fetch_instance_info: 返回 raw_records，统一累加逻辑
    - 支持批量查询（按ID列表）
    """
    def __init__(self, access_key, secret_key) -> None:
        super().__init__(access_key, secret_key)
        self.namespace = "SYS.ELB"
        self.DimensionsName = "lbaas_instance_id"
        self.csp_name = "Huawei"

    def fetch_instance_info(
        self,
        region_id: str,
        instance_id_list: Optional[List[str]] = None,
        page_size: int = 100,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取负载均衡器列表

        华为云限制：单次查询最多支持100个负载均衡器

        Args:
            region_id: 地域ID
            instance_id_list: 负载均衡器ID列表（精确匹配）
            page_size: 每页数量（最大100）
            auto_accumulate: 是否自动累加到 self.instance_info

        Returns:
            List[Dict]: 原始记录列表
        """
        if page_size > 100:
            page_size = 100

        credentials = BasicCredentials(self.access_key, self.secret_key)
        client = ElbClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(ElbRegion.value_of(region_id)) \
            .build()

        raw_records = []
        try:
            request = ListLoadBalancersRequest()
            request.limit = page_size

            if instance_id_list:
                # 批量查询（华为云限制：最多100个）
                for idx in range(0, len(instance_id_list), page_size):
                    batch_ids = instance_id_list[idx:idx + page_size]
                    request.id = batch_ids
                    response_json = client.list_load_balancers(request).to_json_object()
                    loadbalancers = response_json.get('loadbalancers', [])
                    raw_records.extend(loadbalancers)
            else:
                # 查询所有负载均衡器（分页）
                offset = 0
                has_more = True
                while has_more:
                    request.offset = offset
                    response_json = client.list_load_balancers(request).to_json_object()
                    current_page = response_json.get('loadbalancers', [])

                    if not current_page:
                        break

                    raw_records.extend(current_page)

                    # 判断是否还有下一页
                    total_count = response_json.get('total_count', 0)
                    if total_count and (offset + len(current_page)) >= total_count:
                        has_more = False
                    else:
                        offset += len(current_page)
                        if len(current_page) < page_size:
                            has_more = False

        except exceptions.ClientRequestException as e:
            print(f"[Error] Failed to fetch ELB info in {region_id}: Request id: {e.request_id}\n"
                  f"Status code: {e.status_code}, error code: {e.error_code}\n"
                  f"Error message: {e.error_msg}")

        if auto_accumulate and raw_records:
            self._accumulate_instance_info(raw_records)

        return raw_records
