# CloudInstanceHandler/cloudinstancehandler/aliyun/NGW.py

import pandas as pd
import asyncio
from typing import List, Dict, Any, Optional

from cloudinstancehandler.aliyun.AliyunInstance import AliyunInstance

# --- 新版 Tea SDK 导入 ---
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_vpc20160428.client import Client as VpcClient
from alibabacloud_vpc20160428 import models as vpc_models


class NGW(AliyunInstance):
    """
    阿里云 NAT 网关（NAT Gateway）管理

    支持功能：
    1. 查询 NAT 网关实例信息（精确查询、分页查询）
    2. 异步并发获取（适合批量查询场景）
    """

    def __init__(self, access_key: str, secret_key: str) -> None:
        super().__init__(access_key, secret_key)
        self.namespace = "acs_nat_gateway"
        self.ProductCategory = "nat_gateway"

    def _create_vpc_client(self, region_id: str) -> VpcClient:
        """
        创建 VPC 客户端实例（NAT 网关使用 VPC API）
        """
        config = open_api_models.Config(
            access_key_id=self.access_key,
            access_key_secret=self.secret_key,
            endpoint=f'vpc.{region_id}.aliyuncs.com'
        )
        return VpcClient(config)

    async def fetch_instance_info(
        self,
        region_id: str,
        instance_list: Optional[List[str]] = None,
        page_size: int = 50,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取 NAT 网关实例信息（异步）

        Args:
            region_id: 地域 ID
            instance_list: 实例 ID 列表（精确查询）
            page_size: 每页数量（最大 50）
            auto_accumulate: 是否自动累积到 self.instance_info

        Returns:
            List[Dict[str, Any]]: 原始记录列表

        Note:
            - 精确查询时，NAT 网关 API 每次只能查 1 个实例
            - 使用并发查询提升批量获取性能
        """
        if instance_list:
            return await self._fetch_instances_by_ids_async(
                region_id=region_id,
                instance_ids=instance_list,
                auto_accumulate=auto_accumulate
            )
        else:
            return await self._fetch_all_instances_async(
                region_id=region_id,
                page_size=page_size,
                auto_accumulate=auto_accumulate
            )

    async def _fetch_instances_by_ids_async(
        self,
        region_id: str,
        instance_ids: List[str],
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        按实例 ID 批量查询（并发，每次1个）
        """
        client = self._create_vpc_client(region_id)

        async def _fetch_one(nat_gateway_id: str, sem: asyncio.Semaphore) -> List[Dict]:
            async with sem:
                try:
                    req = vpc_models.DescribeNatGatewaysRequest(
                        page_size=1,
                        page_number=1,
                        nat_gateway_id=nat_gateway_id
                    )
                    resp = await client.describe_nat_gateways_with_options_async(req, self.runtime)
                    items = resp.body.nat_gateways.nat_gateway or []
                    return [item.to_map() for item in items]
                except Exception as e:
                    print(f"[Warn] Failed to fetch NGW {nat_gateway_id} in {region_id}: {e}")
                    return []

        semaphore = asyncio.Semaphore(5)
        tasks = [_fetch_one(nat_gateway_id, semaphore) for nat_gateway_id in instance_ids]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        records = []
        for res in results:
            if isinstance(res, list):
                records.extend(res)
            elif isinstance(res, Exception):
                print(f"[Error] Unexpected task error: {res}")

        if auto_accumulate and records:
            self._accumulate_instance_info(records)

        return records

    async def _fetch_all_instances_async(
        self,
        region_id: str,
        page_size: int = 50,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        查询该地域下所有 NAT 网关实例（分页）
        """
        client = self._create_vpc_client(region_id)
        req = vpc_models.DescribeNatGatewaysRequest(
            page_size=page_size,
            page_number=1
        )

        records = []
        total_count = None

        while True:
            try:
                resp = await client.describe_nat_gateways_with_options_async(req, self.runtime)
                body = resp.body
                items = body.nat_gateways.nat_gateway or []
                if not items:
                    break
                records.extend([item.to_map() for item in items])

                if total_count is None:
                    total_count = body.total_count or 0
                if len(records) >= total_count:
                    break

                req.page_number += 1
            except Exception as e:
                print(f"[Error] Failed to fetch NGW instances in {region_id}: {e}")
                break

        if auto_accumulate and records:
            self._accumulate_instance_info(records)

        return records
