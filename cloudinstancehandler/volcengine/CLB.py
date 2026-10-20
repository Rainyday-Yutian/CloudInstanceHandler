# coding: utf-8

import asyncio
from typing import Any, Dict, List, Optional

import pandas as pd

from cloudinstancehandler.volcengine.VolcInstance import VolcInstance

import volcenginesdkcore
import volcenginesdkclb
from volcenginesdkcore.rest import ApiException


class CLB(VolcInstance):
    """
    火山云负载均衡 CLB

    重构要点（原 volcinsdatahandler.CLB）：
    - getInsInfo → fetch_instance_info（异步，page_number 分页）
    - InsInfo → instance_info；记录注入 billing_type_text 与 region_id
    """

    def __init__(self, access_key: str, secret_key: str) -> None:
        super().__init__(access_key, secret_key)
        self.namespace = "VCM_CLB"
        self.sub_namespace = "loadbalancer"
        self.billing_type_map: Dict[int, str] = {
            1: "包年包月",
            2: "按量计费"
        }

    def _create_api_instance(self, region_id: str) -> volcenginesdkclb.CLBApi:
        """创建 CLB API 客户端（每请求独立 ApiClient，避免并发下 set_default 全局状态竞争）"""
        configuration = self._create_configuration(region_id)
        return volcenginesdkclb.CLBApi(volcenginesdkcore.ApiClient(configuration))

    async def fetch_instance_info(
        self,
        region_id: str,
        instance_id_list: Optional[List[str]] = None,
        instance_name: Optional[str] = None,
        eip_address: Optional[str] = None,
        page_size: int = 100,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取 CLB 实例信息（异步，page_number 分页）

        Args:
            region_id: 地域ID
            instance_id_list: 负载均衡实例ID列表（精确查询）
            instance_name: 实例名称过滤
            eip_address: 绑定的 EIP 地址过滤
            page_size: 每页返回数量（最大100）
            auto_accumulate: 是否自动累积到 instance_info

        Returns:
            List[Dict[str, Any]]: 原始记录列表（已注入 billing_type_text 与 region_id 字段）
        """
        api_instance = self._create_api_instance(region_id)
        loop = asyncio.get_running_loop()

        # DescribeLoadBalancers 的 LoadBalancerIds 单次请求有数量上限，超出自动分批
        if instance_id_list:
            id_batches = [instance_id_list[i:i + 100] for i in range(0, len(instance_id_list), 100)]
        else:
            id_batches = [None]

        raw_records: List[Dict[str, Any]] = []
        for batch_ids in id_batches:
            describe_load_balancers_request = volcenginesdkclb.DescribeLoadBalancersRequest(
                page_size=page_size
            )
            if batch_ids:
                describe_load_balancers_request.load_balancer_ids = batch_ids
            if instance_name:
                describe_load_balancers_request.load_balancer_name = instance_name
            if eip_address:
                describe_load_balancers_request.eip_address = eip_address

            page_number = 1
            has_next_page = True

            while has_next_page:
                describe_load_balancers_request.page_number = page_number
                try:
                    def sync_fetch():
                        response = api_instance.describe_load_balancers(describe_load_balancers_request)
                        return response.to_dict()

                    response_dict = await loop.run_in_executor(None, sync_fetch)
                    load_balancers = response_dict.get('load_balancers') or []
                    raw_records.extend(load_balancers)
                    total_count = response_dict.get('total_count') or 0
                    if total_count <= page_number * page_size:
                        has_next_page = False
                    else:
                        page_number += 1
                except ApiException as e:
                    print(f"Exception when fetching CLB instances in {region_id}: {e}\n")
                    break
                except Exception as e:
                    print(f"Unexpected error when fetching CLB instances in {region_id}: {e}\n")
                    break

        # 注入衍生字段：计费类型文本与 region_id（接口返回中无 region 字段）
        for record in raw_records:
            record['billing_type_text'] = self.billing_type_map.get(
                record.get('load_balancer_billing_type')
            )
            record.setdefault('region_id', region_id)

        if auto_accumulate and raw_records:
            self._accumulate_instance_info(raw_records)

        return raw_records
