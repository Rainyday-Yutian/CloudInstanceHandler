# coding: utf-8

import asyncio
from typing import Any, Dict, List, Optional

import pandas as pd

from cloudinstancehandler.volcengine.VolcInstance import VolcInstance

import volcenginesdkcore
import volcenginesdknatgateway
from volcenginesdkcore.rest import ApiException


class NAT(VolcInstance):
    """
    火山云 NAT 网关 (NAT Gateway)

    设计范式对齐 CLB.py（同为 page_number 分页 + ID 列表分批）：
    - fetch_instance_info（异步，nat_gateway_ids 分批 + page_number 分页）
    - 记录注入 region_id（接口返回中无该字段）
    - 主要用途：EIP.query_binded_info 中 Nat 分支的名称回查
    """

    def __init__(self, access_key: str, secret_key: str) -> None:
        super().__init__(access_key, secret_key)
        # 监控指标：Namespace=VCM_NAT，SubNamespace=natgateway，
        # MetricName 如 out_bps/in_bps（出/入方向带宽）等
        self.namespace = "VCM_NAT"
        self.sub_namespace = "natgateway"

    def _create_api_instance(self, region_id: str) -> volcenginesdknatgateway.NATGATEWAYApi:
        """创建 NAT 网关 API 客户端（每请求独立 ApiClient，避免并发下 set_default 全局状态竞争）"""
        configuration = self._create_configuration(region_id)
        return volcenginesdknatgateway.NATGATEWAYApi(volcenginesdkcore.ApiClient(configuration))

    async def fetch_instance_info(
        self,
        region_id: str,
        instance_id_list: Optional[List[str]] = None,
        nat_gateway_name: Optional[str] = None,
        vpc_id: Optional[str] = None,
        subnet_id: Optional[str] = None,
        page_size: int = 100,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取 NAT 网关信息（异步，page_number 分页）

        Args:
            region_id: 地域ID
            instance_id_list: NAT 网关ID列表（nat_gateway_id，精确查询；超100自动分批）
            nat_gateway_name: NAT 网关名称过滤
            vpc_id: 所属 VPC ID 过滤
            subnet_id: 所属子网 ID 过滤
            page_size: 每页返回数量（最大100）
            auto_accumulate: 是否自动累积到 instance_info

        Returns:
            List[Dict[str, Any]]: 原始记录列表（已注入 region_id 字段）
        """
        api_instance = self._create_api_instance(region_id)
        loop = asyncio.get_running_loop()

        # DescribeNatGateways 的 NatGatewayIds 单次请求有数量上限，超出自动分批
        # （串行分片范式，对齐 hwyun/ECS.py::query_ins_info 与本包 CLB.py）
        if instance_id_list:
            id_batches = [instance_id_list[i:i + 100] for i in range(0, len(instance_id_list), 100)]
        else:
            id_batches = [None]

        raw_records: List[Dict[str, Any]] = []
        for batch_ids in id_batches:
            describe_nat_gateways_request = volcenginesdknatgateway.DescribeNatGatewaysRequest(
                page_size=page_size
            )
            if batch_ids:
                describe_nat_gateways_request.nat_gateway_ids = batch_ids
            if nat_gateway_name:
                describe_nat_gateways_request.nat_gateway_name = nat_gateway_name
            if vpc_id:
                describe_nat_gateways_request.vpc_id = vpc_id
            if subnet_id:
                describe_nat_gateways_request.subnet_id = subnet_id

            page_number = 1
            has_next_page = True

            while has_next_page:
                describe_nat_gateways_request.page_number = page_number
                try:
                    def sync_fetch():
                        response = api_instance.describe_nat_gateways(describe_nat_gateways_request)
                        return response.to_dict()

                    response_dict = await loop.run_in_executor(None, sync_fetch)
                    nat_gateways = response_dict.get('nat_gateways') or []
                    raw_records.extend(nat_gateways)
                    total_count = response_dict.get('total_count') or 0
                    if total_count <= page_number * page_size:
                        has_next_page = False
                    else:
                        page_number += 1
                except ApiException as e:
                    print(f"Exception when fetching NAT gateways in {region_id}: {e}\n")
                    break
                except Exception as e:
                    print(f"Unexpected error when fetching NAT gateways in {region_id}: {e}\n")
                    break

        # 接口返回中无 region 字段，手动注入以保证统一规范
        for record in raw_records:
            record['region_id'] = region_id

        if auto_accumulate and raw_records:
            self._accumulate_instance_info(raw_records)

        return raw_records
