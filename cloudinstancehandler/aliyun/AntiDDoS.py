# CloudInstanceHandler/cloudinstancehandler/aliyun/AntiDDoS.py

import pandas as pd
import asyncio
from typing import List, Dict, Any, Optional

from cloudinstancehandler.common.funcs import *
from cloudinstancehandler.aliyun.AliyunInstance import AliyunInstance

# --- 新版 Tea SDK 导入 ---
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_antiddos_public20170518.client import Client as AntiddosClient
from alibabacloud_antiddos_public20170518 import models as antiddos_models


class AntiDDoS(AliyunInstance):
    def __init__(self, access_key, secret_key) -> None:
        """
        Anti-DDoS Public 服务 - IP 地理位置查询
        注意：此服务与 DDoSDip 不同，使用的是公共 DDoS 防护（Anti-DDoS Public）
        """
        super().__init__(access_key, secret_key)
        self.instance_type = "AntiDDoS_Public"  # 重命名以避免与 DDoSDip 混淆

    def _create_antiddos_client(self, region_id: str = "cn-hangzhou") -> AntiddosClient:
        """创建 Anti-DDoS Public 客户端"""
        config = open_api_models.Config(
            access_key_id=self.access_key,
            access_key_secret=self.secret_key,
            region_id=region_id
        )
        return AntiddosClient(config)

    async def fetch_eip_location_async(
        self,
        ip_address: str,
        region_id: str = "cn-hangzhou",
        auto_accumulate: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        查询单个 IP 地理位置信息

        Args:
            ip_address: 要查询的 IP 地址
            region_id: 地域 ID，默认为 cn-hangzhou
            auto_accumulate: 是否自动累积到 instance_info 中

        Returns:
            Optional[Dict[str, Any]]: IP 位置信息字典
        """
        client = self._create_antiddos_client(region_id)

        req = antiddos_models.DescribeIpLocationServiceRequest(
            internet_ip=ip_address
        )

        try:
            resp = await client.describe_ip_location_service_with_options_async(req, self.runtime)
            if resp.body and resp.body.instance:
                ip_info = resp.body.instance.to_map()
                if auto_accumulate:
                    self._accumulate_instance_info([ip_info])
                return ip_info
            else:
                return None
        except Exception as e:
            print(f"[Error] Failed to query IP location for {ip_address}: {e}")
            return None

    async def fetch_eip_location_batch_async(
        self,
        ip_addresses: List[str],
        region_id: str = "cn-hangzhou",
        page_size: int = 10,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        批量查询 IP 地理位置信息（异步并发）

        Args:
            ips: IP 地址列表
            region_id: 地域 ID，默认为 cn-hangzhou
            page_size: 并发批处理大小，默认 10
            auto_accumulate: 是否自动累积到 instance_info 中

        Returns:
            List[Dict[str, Any]]: IP 位置信息列表
            
        Note:
            - 阿里云该接口的单用户 QPS 限制为 10 次/秒，请自行控制调用频率，建议 ip_addresses 不超过10个
        """
        page_size = min(page_size, 10)
        if not ip_addresses:
            return []

        async def _query_single_ip(ip: str, sem: asyncio.Semaphore) -> Optional[Dict]:
            async with sem:
                return await self.fetch_eip_location_async(
                    ip_address=ip,
                    region_id=region_id,
                    auto_accumulate=False
                )

        # 使用信号量控制并发度
        semaphore = asyncio.Semaphore(5)

        # 分批并发查询
        tasks = []
        for i in range(0, len(ip_addresses), page_size):
            batch = ip_addresses[i:i+page_size]
            batch_tasks = [_query_single_ip(ip, semaphore) for ip in batch]
            tasks.extend(batch_tasks)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 过滤结果
        valid_results = []
        for res in results:
            if isinstance(res, dict):
                valid_results.append(res)
            elif isinstance(res, Exception):
                print(f"[Error] Unexpected task error: {res}")

        if auto_accumulate and valid_results:
            self._accumulate_instance_info(valid_results)

        return valid_results
