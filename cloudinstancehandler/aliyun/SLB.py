# CloudInstanceHandler/cloudinstancehandler/aliyun/SLB.py

import pandas as pd
import asyncio
from typing import List, Dict, Any, Optional

from cloudinstancehandler.aliyun.AliyunInstance import AliyunInstance

# --- 新版 Tea SDK 导入 ---
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_slb20140515.client import Client as SlbClient
from alibabacloud_slb20140515 import models as slb_models


class SLB(AliyunInstance):
    """
    阿里云负载均衡（Server Load Balancer）管理

    支持功能：
    1. 查询 SLB 实例信息（精确查询、分页查询）
    2. 异步并发获取（适合批量查询场景）
    """

    def __init__(self, access_key: str, secret_key: str) -> None:
        super().__init__(access_key, secret_key)
        self.namespace = "acs_slb_dashboard"
        self.ProductCategory = "slb"

    def _create_slb_client(self, region_id: str) -> SlbClient:
        """
        创建 SLB 客户端实例
        """
        config = open_api_models.Config(
            access_key_id=self.access_key,
            access_key_secret=self.secret_key,
            region_id=region_id,  # 必须设置 region_id
            endpoint=f'slb.{region_id}.aliyuncs.com'
        )
        return SlbClient(config)

    async def fetch_instance_info(
        self,
        region_id: str,
        instance_list: Optional[List[str]] = None,
        page_size: int = 10,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取 SLB 实例信息（异步）

        Args:
            region_id: 地域 ID
            instance_list: 实例 ID 列表（精确查询）
            page_size: 每页数量（精确查询时每批10个，分页查询时最大100）
            auto_accumulate: 是否自动累积到 self.instance_info

        Returns:
            List[Dict[str, Any]]: 原始记录列表

        Note:
            - 阿里云 SLB API 精确查询时，单次最多支持 10 个 LoadBalancerId
            - 使用并发查询提升批量获取性能
        """
        if instance_list:
            return await self._fetch_instances_by_ids_async(
                region_id=region_id,
                instance_ids=instance_list,
                page_size=page_size,
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
        page_size: int = 10,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        按实例 ID 批量查询（并发）
        """
        BATCH_LIMIT = min(10, page_size)  # SLB API 精确查询上限为 10
        client = self._create_slb_client(region_id)

        async def _batch_fetch(batch_list: List[str], sem: asyncio.Semaphore) -> List[Dict]:
            async with sem:
                try:
                    req = slb_models.DescribeLoadBalancersRequest(
                        region_id=region_id,  # 必须显式设置 region_id
                        load_balancer_id=",".join(batch_list),
                        page_size=BATCH_LIMIT,
                        page_number=1
                    )
                    resp = await client.describe_load_balancers_with_options_async(req, self.runtime)
                    items = resp.body.load_balancers.load_balancer or []
                    return [item.to_map() for item in items]
                except Exception as e:
                    print(f"[Warn] Failed to fetch SLB instances {batch_list} in {region_id}: {e}")
                    return []

        semaphore = asyncio.Semaphore(5)
        batches = [instance_ids[i:i+BATCH_LIMIT] for i in range(0, len(instance_ids), BATCH_LIMIT)]
        tasks = [_batch_fetch(batch, semaphore) for batch in batches]

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
        page_size: int = 100,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        查询该地域下所有 SLB 实例（分页）
        """
        client = self._create_slb_client(region_id)
        req = slb_models.DescribeLoadBalancersRequest(
            region_id=region_id,  # 必须显式设置 region_id
            page_size=page_size,
            page_number=1
        )

        records = []
        while True:
            try:
                resp = await client.describe_load_balancers_with_options_async(req, self.runtime)
                body = resp.body
                items = body.load_balancers.load_balancer or []
                if not items:
                    break
                records.extend([item.to_map() for item in items])

                total_count = body.total_count or 0
                if len(records) >= total_count:
                    break

                req.page_number += 1
            except Exception as e:
                print(f"[Error] Failed to fetch SLB instances in {region_id}: {e}")
                break

        if auto_accumulate and records:
            self._accumulate_instance_info(records)

        return records
