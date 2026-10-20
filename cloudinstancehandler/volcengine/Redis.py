# coding: utf-8

import asyncio
from typing import Any, Dict, List, Optional

import pandas as pd

from cloudinstancehandler.volcengine.VolcInstance import VolcInstance

import volcenginesdkcore
import volcenginesdkredis
from volcenginesdkcore.rest import ApiException


class Redis(VolcInstance):
    """
    火山云云数据库 Redis

    重构要点（原 volcinsdatahandler.Redis）：
    - getInsInfo → fetch_instance_info（异步，page_number 分页）
    - InsInfo → instance_info
    """

    def __init__(self, access_key: str, secret_key: str) -> None:
        super().__init__(access_key, secret_key)
        self.namespace = "VCM_Redis"
        self.sub_namespace = "aggregated_server"

    def _create_api_instance(self, region_id: str) -> volcenginesdkredis.REDISApi:
        """创建 Redis API 客户端（每请求独立 ApiClient，避免并发下 set_default 全局状态竞争）"""
        configuration = self._create_configuration(region_id)
        return volcenginesdkredis.REDISApi(volcenginesdkcore.ApiClient(configuration))

    async def fetch_instance_info(
        self,
        region_id: str,
        page_size: int = 100,
        instance_name: Optional[str] = None,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取 Redis 实例信息（异步，page_number 分页）

        Args:
            region_id: 地域ID
            page_size: 每页返回数量（最大100）
            instance_name: 实例名称过滤（可选）
            auto_accumulate: 是否自动累积到 instance_info

        Returns:
            List[Dict[str, Any]]: 原始记录列表（已注入 region_id 字段）
        """
        api_instance = self._create_api_instance(region_id)
        loop = asyncio.get_running_loop()

        describe_db_instances_request = volcenginesdkredis.DescribeDBInstancesRequest(
            page_number=1,
            region_id=region_id,
            page_size=page_size
        )
        if instance_name:
            describe_db_instances_request.instance_name = instance_name

        raw_records: List[Dict[str, Any]] = []
        page_number = 1
        has_next_page = True

        while has_next_page:
            describe_db_instances_request.page_number = page_number
            try:
                def sync_fetch():
                    response = api_instance.describe_db_instances(describe_db_instances_request)
                    return response.to_dict()

                response_dict = await loop.run_in_executor(None, sync_fetch)
                instances = response_dict.get('instances') or []
                raw_records.extend(instances)
                total = response_dict.get('total_instances_num') or 0
                if total <= page_number * page_size:
                    has_next_page = False
                else:
                    page_number += 1
            except ApiException as e:
                print(f"Exception when fetching Redis instances in {region_id}: {e}\n")
                break
            except Exception as e:
                print(f"Unexpected error when fetching Redis instances in {region_id}: {e}\n")
                break

        # 接口返回中已含 region_id 字段；如缺失则手动注入以保证统一规范
        for record in raw_records:
            record.setdefault('region_id', region_id)

        if auto_accumulate and raw_records:
            self._accumulate_instance_info(raw_records)

        return raw_records
