# coding: utf-8

import asyncio
from typing import Any, Dict, List, Optional

import pandas as pd

from cloudinstancehandler.common.BasicDataFrame import BasicDataFrame
from cloudinstancehandler.volcengine.common_request import (
    VCMSearchResourcesRequest,
    VCMGetResourceCountsRequest,
)


class Resource(BasicDataFrame):
    """
    火山云资源中心（ResourceCenter）

    基于通用签名请求（common_request.py）搜索账号下所有云资源，无需指定具体产品。

    重构要点（原 volcinsdatahandler.Resource）：
    - getInsInfo → fetch_instance_info（异步：签名 HTTP 调用经 asyncio.to_thread，
      分页间隔 time.sleep → asyncio.sleep）
    - 新增 fetch_resource_counts 包装 GetResourceCounts 接口
    - 修复老实现跳过 super().__init__() 导致无 instance_info 等 DataFrame 属性的隐患
    """

    def __init__(self, access_key: str, secret_key: str) -> None:
        super().__init__()
        self.access_key = access_key
        self.secret_key = secret_key
        self.instance_type = self.__class__.__name__
        self.csp_name = "VolcEngine"

    async def fetch_instance_info(
        self,
        page_size: int = 100,
        filter: Optional[List[Dict[str, Any]]] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        sleep_time: float = 2,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        搜索账号下所有云资源（异步，NextToken 分页）

        Args:
            page_size: 每页返回数量（最大100）
            filter: 过滤条件列表（参考 SearchResources API 文档）
            sort_by: 排序字段
            sort_order: 排序方式（asc/desc）
            sleep_time: 分页间隔休眠秒数（避免接口限流，默认2秒）
            auto_accumulate: 是否自动累积到 instance_info（覆盖式，与老实现一致）

        Returns:
            List[Dict[str, Any]]: 原始资源记录列表
        """
        search_request = VCMSearchResourcesRequest(
            access_key=self.access_key,
            secret_key=self.secret_key,
            max_results=page_size,
            filter=filter,
            sort_by=sort_by,
            sort_order=sort_order
        )

        raw_records: List[Dict[str, Any]] = []

        while True:
            # 签名 HTTP 调用为同步 requests 实现，放入线程池避免阻塞事件循环
            response_json = await asyncio.to_thread(search_request.do_request)
            if not response_json:
                print("[Error] SearchResources returned empty result.")
                break
            resources = response_json.get("Resources") or []
            raw_records.extend(resources)
            next_token = response_json.get("NextToken")
            if not next_token:
                break
            search_request.next_token = next_token
            await asyncio.sleep(sleep_time)

        if auto_accumulate and raw_records:
            # 与老实现保持一致：全量覆盖而非追加
            self.instance_info = pd.DataFrame(raw_records)

        return raw_records

    async def fetch_resource_counts(
        self,
        group_by_key: str = "ResourceType",
        filter: Optional[List[Dict[str, Any]]] = None
    ) -> pd.DataFrame:
        """
        统计现存资源数量分布（异步）

        Args:
            group_by_key: 分组维度，'ResourceType' 或 'Region'
            filter: 过滤条件列表（可选）

        Returns:
            pd.DataFrame: 资源计数表
        """
        counts_request = VCMGetResourceCountsRequest(
            access_key=self.access_key,
            secret_key=self.secret_key,
            filter=filter,
            group_by_key=group_by_key
        )
        response_json = await asyncio.to_thread(counts_request.do_request)
        if not response_json:
            print("[Error] GetResourceCounts returned empty result.")
            return pd.DataFrame()
        # 响应结构：{"GroupByKey": ..., "ResourceCounts": [...]} 或直接为计数列表
        counts = response_json.get("ResourceCounts", response_json)
        df_data = pd.DataFrame(counts) if not isinstance(counts, dict) else pd.DataFrame([counts])
        return df_data
