# coding: utf-8

import asyncio
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from cloudinstancehandler.volcengine.VolcInstance import VolcInstance

import volcenginesdkcore
import volcenginesdkrdsmysqlv2
from volcenginesdkcore.rest import ApiException


class RDS(VolcInstance):
    """
    火山云云数据库 RDS for MySQL

    重构要点（原 volcinsdatahandler.RDS）：
    - getInsInfo → fetch_instance_info（异步）
    - getRegions → fetch_regions（异步）
    - getInsDetailsInfo → fetch_instance_detail_info（异步并发）
    - InsInfo/InsData → instance_info/instance_data
    """

    # 接口异常时的兜底地域列表（沿用老实现）
    DEFAULT_REGIONS: List[Dict[str, str]] = [
        {'region_id': 'cn-beijing', 'region_name': '华北2（北京）'},
        {'region_id': 'cn-shanghai', 'region_name': '华东2（上海）'},
        {'region_id': 'cn-guangzhou', 'region_name': '华南1（广州）'},
        {'region_id': 'cn-hongkong', 'region_name': '中国香港'},
        {'region_id': 'ap-southeast-1', 'region_name': '亚太东南（柔佛）'},
    ]

    def __init__(self, access_key: str, secret_key: str) -> None:
        super().__init__(access_key, secret_key)
        self.namespace = "VCM_RDS_MySQL"
        self.sub_namespace = "resource_monitor"

    def _create_api_instance(self, region_id: str) -> volcenginesdkrdsmysqlv2.RDSMYSQLV2Api:
        """创建 RDS API 客户端（每请求独立 ApiClient，避免并发下 set_default 全局状态竞争）"""
        configuration = self._create_configuration(region_id)
        return volcenginesdkrdsmysqlv2.RDSMYSQLV2Api(volcenginesdkcore.ApiClient(configuration))

    async def fetch_regions(self, raw_data: bool = False, region_id: str = "cn-beijing") -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """
        获取 RDS 可用地域列表（异步）

        Args:
            raw_data: 为 True 时返回原始 List[Dict]，否则返回 pd.DataFrame
            region_id: 发起请求的地域，默认 cn-beijing

        Returns:
            Union[pd.DataFrame, List[Dict]]: 地域列表；接口异常时返回内置兜底列表
        """
        api_instance = self._create_api_instance(region_id)
        loop = asyncio.get_running_loop()

        def sync_fetch():
            describe_regions_request = volcenginesdkrdsmysqlv2.DescribeRegionsRequest()
            response = api_instance.describe_regions(describe_regions_request)
            return response.to_dict()

        try:
            response_dict = await loop.run_in_executor(None, sync_fetch)
            regions_list_data = response_dict.get('regions') or self.DEFAULT_REGIONS
        except ApiException as e:
            print(f"Exception when calling describe_regions: {e}\n")
            regions_list_data = self.DEFAULT_REGIONS

        if raw_data:
            return regions_list_data
        return pd.DataFrame(regions_list_data)

    async def fetch_instance_info(
        self,
        region_id: str,
        page_size: int = 100,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取 RDS MySQL 实例信息（异步，page_number 分页）

        Args:
            region_id: 地域ID
            page_size: 每页返回数量（最大100）
            auto_accumulate: 是否自动累积到 instance_info

        Returns:
            List[Dict[str, Any]]: 原始记录列表（已注入 region_id 字段）
        """
        api_instance = self._create_api_instance(region_id)
        loop = asyncio.get_running_loop()

        describe_db_instances_request = volcenginesdkrdsmysqlv2.DescribeDBInstancesRequest(
            page_size=page_size
        )

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
                total = response_dict.get('total') or 0
                if total <= page_number * page_size:
                    has_next_page = False
                else:
                    page_number += 1
            except ApiException as e:
                print(f"Exception when fetching RDS instances in {region_id}: {e}\n")
                break
            except Exception as e:
                print(f"Unexpected error when fetching RDS instances in {region_id}: {e}\n")
                break

        # 接口返回中已含 region_id 字段；如缺失则手动注入以保证统一规范
        for record in raw_records:
            record.setdefault('region_id', region_id)

        if auto_accumulate and raw_records:
            self._accumulate_instance_info(raw_records)

        return raw_records

    async def fetch_instance_detail_info(
        self,
        merge: bool = True,
        max_concurrency: int = 10
    ) -> pd.DataFrame:
        """
        获取 instance_info 中所有 RDS 实例的详细信息（异步并发）

        注意：调用前需先执行 fetch_instance_info，且 instance_info 中需包含
        region_id 与 instance_id 字段。

        Args:
            merge: 是否将详细信息左合并回 instance_info（on=instance_id）
            max_concurrency: 最大并发数（默认10，可根据 API 限流调整）

        Returns:
            pd.DataFrame: 详细信息表（同时存入 self.instance_data）

        Raises:
            ValueError: instance_info 为空时抛出
        """
        if self.instance_info.empty:
            raise ValueError("instance_info is empty. Please call fetch_instance_info first.")

        loop = asyncio.get_running_loop()
        semaphore = asyncio.Semaphore(max_concurrency)

        async def _fetch_one_detail(region: str, instance_id: str) -> Optional[Dict[str, Any]]:
            async with semaphore:
                api_instance = self._create_api_instance(region)

                def sync_fetch():
                    request = volcenginesdkrdsmysqlv2.DescribeDBInstanceDetailRequest(
                        instance_id=instance_id
                    )
                    response = api_instance.describe_db_instance_detail(request)
                    return response.to_dict()

                try:
                    response_dict = await loop.run_in_executor(None, sync_fetch)
                    # 提升 basic_info.instance_id 至顶层，便于后续 merge
                    basic_info = response_dict.get('basic_info') or {}
                    response_dict['instance_id'] = basic_info.get('instance_id', instance_id)
                    return response_dict
                except ApiException as e:
                    print(f"Exception when fetching detail of {instance_id} in {region}: {e}\n")
                    return None

        tasks = []
        for region in self.instance_info['region_id'].dropna().unique().tolist():
            instance_ids = (
                self.instance_info[self.instance_info['region_id'] == region]['instance_id']
                .dropna().unique().tolist()
            )
            for instance_id in instance_ids:
                tasks.append(_fetch_one_detail(region, instance_id))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        list_data = []
        for res in results:
            if isinstance(res, dict):
                list_data.append(res)
            elif isinstance(res, Exception):
                print(f"[Error] Unexpected task error: {res}")

        df_data = pd.DataFrame(list_data)
        self.instance_data = df_data

        if merge and not df_data.empty:
            self.instance_info = pd.merge(
                self.instance_info, df_data, on='instance_id', how='left'
            )

        return df_data
