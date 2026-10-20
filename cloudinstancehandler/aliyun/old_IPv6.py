# CloudInstanceHandler/cloudinstancehandler/aliyun/IPv6.py

import pandas as pd
import asyncio
from typing import List, Dict, Any, Optional
from pprint import pprint

from cloudinstancehandler.common.funcs import *
from cloudinstancehandler.aliyun.AliyunInstance import AliyunInstance, getTimeDict

# --- 新版 Tea SDK 导入 ---
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_vpc20160428.client import Client as VpcClient
from alibabacloud_vpc20160428 import models as vpc_models


class IPv6(AliyunInstance):
    def __init__(self, access_key, secret_key) -> None:
        super().__init__(access_key, secret_key)
        self.namespace = "acs_ipv6_bandwidth"
        self.ProductCategory = "ipv6gateway"
        self.DimensionsName = "instanceId"  # IPv6 的维度名称

    def _create_vpc_client(self, region_id: str) -> VpcClient:
        config = open_api_models.Config(
            access_key_id=self.access_key,
            access_key_secret=self.secret_key,
            region_id=region_id
        )
        return VpcClient(config)

    async def fetch_instance_info(
        self,
        region_id: str,
        instance_ids: List[str] = None,
        ipv6_addresses: List[str] = None,
        page_size: int = 50,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取 IPv6 地址信息。支持按 ID 列表或地址列表查询。
        支持单地域多实例批量查询。

        Args:
            region_id: 地域 ID
            instance_ids: IPv6 地址 ID 列表（优先级高于 ipv6_addresses）
            ipv6_addresses: IPv6 地址列表
            page_size: 页面大小，范围 1-50
            auto_accumulate: 是否自动累积到 instance_info 中

        Returns:
            List[Dict[str, Any]]: 原始记录列表
        """
        raw_records: List[Dict[str, Any]] = []

        try:
            if instance_ids:
                raw_records = await self._fetch_ipv6_by_ids_async(region_id, instance_ids, page_size)
            elif ipv6_addresses:
                raw_records = await self._fetch_ipv6_by_addresses_async(region_id, ipv6_addresses, page_size)
            else:
                raw_records = await self._fetch_all_ipv6_async(region_id, page_size)

            if auto_accumulate and raw_records:
                self._accumulate_instance_info(raw_records)

        except Exception as e:
            print(f"[Error] Failed to fetch IPv6 info in {region_id}: {e}")
            raise

        return raw_records

    async def _fetch_ipv6_by_ids_async(
        self,
        region_id: str,
        instance_ids: List[str],
        page_size: int = 50
    ) -> List[Dict[str, Any]]:
        """
        按 IPv6 地址 ID 批量查询（异步）
        """
        if not 1 <= page_size <= 50:
            print(f"[WARNING] page_size must be between 1 and 50, default to {page_size}")
            page_size = 50

        client = self._create_vpc_client(region_id)

        async def _batch_fetch(batch_list: List[str], sem: asyncio.Semaphore) -> List[Dict]:
            async with sem:
                req = vpc_models.DescribeIpv6AddressesRequest(
                    page_size=page_size,
                    page_number=1,
                    ipv_6address_id=",".join(batch_list)
                )
                try:
                    resp = await client.describe_ipv_6addresses_with_options_async(req, self.runtime)
                    items = resp.body.ipv_6addresses.ipv_6address or []
                    # 添加 RegionId 字段到每条记录
                    result = [item.to_map() for item in items]
                    for item in result:
                        item['RegionId'] = region_id
                    return result
                except Exception as e:
                    print(f"[Error] Failed to fetch IPv6 by IDs in {region_id}: {e}")
                    return []

        # 分片并发查询
        semaphore = asyncio.Semaphore(5)
        batches = [instance_ids[i:i+page_size] for i in range(0, len(instance_ids), page_size)]
        tasks = [_batch_fetch(batch, semaphore) for batch in batches]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        records = []
        for res in results:
            if isinstance(res, list):
                records.extend(res)
            elif isinstance(res, Exception):
                print(f"[Error] Unexpected task error: {res}")

        return records

    async def _fetch_ipv6_by_addresses_async(
        self,
        region_id: str,
        ipv6_addresses: List[str],
        page_size: int = 50
    ) -> List[Dict[str, Any]]:
        """
        按 IPv6 地址列表查询（异步）
        """
        if not 1 <= page_size <= 50:
            print(f"[WARNING] page_size must be between 1 and 50, default to {page_size}")
            page_size = 50

        client = self._create_vpc_client(region_id)

        async def _batch_fetch(batch_list: List[str], sem: asyncio.Semaphore) -> List[Dict]:
            async with sem:
                req = vpc_models.DescribeIpv6AddressesRequest(
                    page_size=page_size,
                    page_number=1,
                    ipv_6address=",".join(batch_list)
                )
                try:
                    resp = await client.describe_ipv_6addresses_with_options_async(req, self.runtime)
                    items = resp.body.ipv_6addresses.ipv_6address or []
                    # 添加 RegionId 字段到每条记录
                    result = [item.to_map() for item in items]
                    for item in result:
                        item['RegionId'] = region_id
                    return result
                except Exception as e:
                    print(f"[Error] Failed to fetch IPv6 by addresses in {region_id}: {e}")
                    return []

        # 分片并发查询
        semaphore = asyncio.Semaphore(5)
        batches = [ipv6_addresses[i:i+page_size] for i in range(0, len(ipv6_addresses), page_size)]
        tasks = [_batch_fetch(batch, semaphore) for batch in batches]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        records = []
        for res in results:
            if isinstance(res, list):
                records.extend(res)
            elif isinstance(res, Exception):
                print(f"[Error] Unexpected task error: {res}")

        return records

    async def _fetch_all_ipv6_async(
        self,
        region_id: str,
        page_size: int = 50
    ) -> List[Dict[str, Any]]:
        """
        查询所有 IPv6 地址（异步）
        """
        if not 1 <= page_size <= 50:
            print(f"[WARNING] page_size must be between 1 and 50, default to {page_size}")
            page_size = 50

        client = self._create_vpc_client(region_id)
        records: List[Dict[str, Any]] = []
        total_count: Optional[int] = None
        req = vpc_models.DescribeIpv6AddressesRequest(
            page_size=page_size,
            page_number=1
        )

        while True:
            try:
                resp = await client.describe_ipv_6addresses_with_options_async(req, self.runtime)
                body = resp.body
                items = body.ipv_6addresses.ipv_6address or []
                if not items:
                    break
                # 添加 RegionId 字段到每条记录
                for item in items:
                    item_map = item.to_map()
                    item_map['RegionId'] = region_id
                    records.append(item_map)
                if total_count is None:
                    total_count = body.total_count or 0
                if len(records) >= total_count:
                    break
                req.page_number += 1
            except Exception as e:
                print(f"[Error] Failed to fetch IPv6 page {req.page_number} in {region_id}: {e}")
                break

        return records

    # 2. 降维 Ipv6InternetBandwidth 信息 
    def extract_bandwidth_info(self) -> None:
        """
        降维 Ipv6InternetBandwidth 字段信息（将 JSON 字段展开为扁平化列）
        直接修改 self.instance_info，将 JSON 字段展开并合并

        Note:
            此方法直接修改 self.instance_info，不返回新的 DataFrame
        """
        if self.instance_info.empty:
            print("[Info] instance_info is empty. Nothing to extract.")
            return

        # 查找包含 Ipv6InternetBandwidth 的记录
        if "Ipv6InternetBandwidth" in self.instance_info.columns:
            # 使用 json_normalize 展开 JSON 字段
            bandwidth_df = pd.json_normalize(self.instance_info['Ipv6InternetBandwidth'])

            # 添加前缀，避免列名冲突
            bandwidth_df.columns = [f"Bandwidth_{col}" for col in bandwidth_df.columns]

            # 直接修改 self.instance_info
            self.instance_info = pd.concat(
                [self.instance_info.reset_index(drop=True), bandwidth_df.reset_index(drop=True)],
                axis=1
            )

            print(f"[Info] Extracted bandwidth info for {len(bandwidth_df)} records.")
        else:
            print("[Info] No Ipv6InternetBandwidth field found.")

    # ===========================================================================
    # 3. 查询绑定的实例信息 (简化版，对应原 getIPv6Data 中的 ENI/ECS 补充逻辑)
    # 参考 EIP.query_binded_info 的设计
    # ===========================================================================
    async def query_binded_info(
        self,
        return_raw: bool = False,
        input_df: Optional[pd.DataFrame] = None,
        max_concurrency: int = 15
    ) -> pd.DataFrame:
        """
        注意：调用此方法前，self.instance_info 或 input_df 必须包含以下字段：
        - AssociatedInstanceId: IPv6 绑定的实例 ID
        - AssociatedInstanceType: 绑定的实例类型（EcsInstance/NetworkInterface）
        - RegionId: 实例所在的地域

        Args:
            return_raw: 是否返回原始查询数据。默认 False（返回合并后的数据）
            input_df: 可选的输入 DataFrame。如果提供，则只处理该 DataFrame 中的数据；
                     如果为 None（默认），则处理 self.instance_info 中的所有数据
            max_concurrency: 最大并发数（默认 15，可根据 API 限流调整）

        Returns:
            pd.DataFrame:
                - return_raw=False: 返回合并绑定信息后的数据（input_df 或 self.instance_info）
                - return_raw=True: 返回原始查询的绑定信息（包含 AssociatedInstanceId, InstanceId, InstanceName, InstanceType）
        """
        from cloudinstancehandler.aliyun.ECS import ECS as ECShandler
        from cloudinstancehandler.aliyun.ECS import ENI as ENIhandler

        # 确定处理的数据源
        if input_df is not None:
            df = input_df.copy()
        else:
            if self.instance_info.empty:
                print("[Info] No IPv6 data in instance_info. Returning empty DataFrame.")
                return pd.DataFrame()
            df = self.instance_info.copy()

        # 初始化结果列表
        all_records = []

        # 按 RegionId 分组处理
        for region_id in df["RegionId"].dropna().unique():
            df_region = df[df["RegionId"] == region_id].copy()

            # ========== 1. 处理 NetworkInterface（弹性网卡）==========
            eni_df = df_region[df_region["AssociatedInstanceType"] == "NetworkInterface"].copy()
            if not eni_df.empty:
                eni_ids = eni_df["AssociatedInstanceId"].dropna().unique().tolist()
                eni_handler = ENIhandler(self.access_key, self.secret_key)
                await eni_handler.fetch_instance_info(
                    region_id=region_id,
                    instance_list=eni_ids,
                    auto_accumulate=True
                )
                if not eni_handler.instance_info.empty:
                    df_eni = eni_handler.instance_info[['NetworkInterfaceId', 'InstanceId']].dropna(subset=['InstanceId'])
                    df_eni = df_eni[df_eni['InstanceId'].str.strip() != '']

                    if not df_eni.empty:
                        # 关联 ENI ID 到 IPv6 记录
                        df_eni.rename(columns={'NetworkInterfaceId': 'AssociatedInstanceId'}, inplace=True)
                        eni_df = pd.merge(eni_df, df_eni, how='left', on='AssociatedInstanceId')
                        all_records.append(eni_df[['AssociatedInstanceId', 'RegionId', 'InstanceId']])

            # ========== 2. 处理 EcsInstance（ECS）==========
            ecs_df = df_region[df_region["AssociatedInstanceType"] == "EcsInstance"].copy()
            if not ecs_df.empty:
                ecs_df['InstanceId'] = ecs_df['AssociatedInstanceId']  # 直接使用
                all_records.append(ecs_df[['AssociatedInstanceId', 'RegionId', 'InstanceId']])

        # 合并所有记录
        if not all_records:
            print("[Info] No binded instances found.")
            return pd.DataFrame()

        df_result = pd.concat(all_records, ignore_index=True)
        df_result = df_result[df_result['InstanceId'].notna() & (df_result['InstanceId'] != '')]

        # ========== 统一查询所有 InstanceId 的名称 ==========
        name_mapping = {}

        for region_id in df_result['RegionId'].dropna().unique():
            region_data = df_result[df_result['RegionId'] == region_id]

            instance_ids = region_data['InstanceId'].unique().tolist()
            if instance_ids:
                ecs_handler = ECShandler(self.access_key, self.secret_key)
                ecs_result = await ecs_handler.fetch_instance_info(
                    region_id=region_id,
                    instance_list=instance_ids,
                    auto_accumulate=False
                )
                if ecs_result:
                    df_ecs = pd.DataFrame(ecs_result)[['InstanceId', 'InstanceName']]
                    for _, row in df_ecs.iterrows():
                        name_mapping[row['InstanceId']] = row['InstanceName']

        # 添加名称字段
        df_result['InstanceName'] = df_result['InstanceId'].map(name_mapping)

        # 根据 return_raw 和 input_df 参数决定返回方式
        if not return_raw:
            if input_df is not None:
                # 与输入的 df 合并
                result = pd.merge(
                    input_df,
                    df_result,
                    on=['AssociatedInstanceId', 'RegionId'],
                    how='left'
                )
                return result
            else:
                # 合并到 self.instance_info
                self.instance_info = pd.merge(
                    self.instance_info,
                    df_result,
                    on=['AssociatedInstanceId', 'RegionId'],
                    how='left'
                )
                return self.instance_info

        return df_result

    # ===========================================================================
    # 4. 主入口方法：获取完整的 IPv6 实例数据 (对应原 getIPv6Data)
    # 参考 EIP.get_eip_bandwidth_rank 的设计
    # ===========================================================================
    # async def get_ins_data(
    #     self,
    #     region_list: List[str],
    #     metric_list: List[str],
    #     time_dict: Dict[str, Any] = None,
    #     display_name_dict: Dict[str, str] = None,
    #     auto_accumulate: bool = True
    # ) -> pd.DataFrame:
    #     """
    #     获取 IPv6 地址数据及指标信息（主入口方法）

    #     Args:
    #         region_list: 地域列表，如 ["cn-chengdu"]
    #         metric_list: 指标列表，如 ["Ipv6Address.RatePercentageOutToInternet", "Ipv6Address.RatePercentageInFromInternet"]
    #         time_dict: 时间范围字典，使用 getTimeDict 生成
    #         display_name_dict: 指标重命名字典，如 {"Ipv6Address.RatePercentageOutToInternet": "OutToInternet", ...}
    #         auto_accumulate: 是否自动累积到 instance_data 中

    #     Returns:
    #         pd.DataFrame: 包含 IPv6 信息和指标数据的完整 DataFrame
    #     """
    #     print(f'Obtaining {self.instance_type} Data in {region_list}...')
    #     all_dfs = []

    #     for region in region_list:
    #         # ========== 1. 获取基础 IPv6 信息 ==========
    #         print(f"[{region}] Fetching IPv6 info...")
    #         ipv6_records = await self.fetch_instance_info(
    #             region_id=region,
    #             auto_accumulate=False
    #         )

    #         if not ipv6_records:
    #             print(f"[{region}] No {self.instance_type} found!")
    #             continue

    #         df = pd.DataFrame(ipv6_records)
    #         print(f"[{region}] Found {len(df)} IPv6 addresses")

    #         # ========== 2. 查询绑定的实例信息 ==========
    #         print(f"[{region}] Querying binded instance info...")
    #         df = await self.query_binded_info(
    #             return_raw=False,
    #             input_df=df,
    #             max_concurrency=15
    #         )

    #         # ========== 3. 降维 Ipv6InternetBandwidth 信息 ==========
    #         print(f"[{region}] Extracting bandwidth info...")
    #         # extract_bandwidth_info 已经直接修改 instance_info
    #         # 这里我们不需要处理 df，因为 extract_bandwidth_info 没有 return
    #         # 等待后续调整逻辑
    #         print(f"[{region}] Bandwidth info extraction completed")

    #         # ========== 4. 精简字段 ==========
    #         columns_to_keep = [
    #             "Ipv6AddressId",
    #             "Ipv6Isp",
    #             "NetworkType",
    #             "InternetChargeType",
    #             "Bandwidth",
    #             "Bandwidth_InternetChargeType",
    #             "Bandwidth_Bandwidth",
    #             "InstanceId",
    #             "InstanceName",
    #             "RegionId"
    #         ]
    #         # 只保留存在的列
    #         columns_to_keep = [col for col in columns_to_keep if col in df.columns]
    #         df_filtered = df[columns_to_keep].copy()

    #         # ========== 5. 获取指标数据 ==========
    #         for metric in metric_list:
    #             print(f"[{region}] Fetching metric: {metric}")
    #             if not df_filtered.empty:
    #                 df_metrics = self.getMetricList(
    #                     metric_name=metric,
    #                     TimeDict=time_dict,
    #                     instance_list=df_filtered["Ipv6AddressId"].to_list(),
    #                     period='300',
    #                     field_name='Maximum',
    #                     statistics_approach=['max'],
    #                     group_by='instanceId',
    #                     rename_field_name=display_name_dict.get(metric, metric) if display_name_dict else metric
    #                 )
    #                 if not df_metrics.empty:
    #                     df_metrics.rename(columns={"instanceId": "Ipv6AddressId"}, inplace=True)
    #                     df_filtered = pd.merge(df_filtered, df_metrics, on="Ipv6AddressId", how="left")
    #             else:
    #                 print(f"[{region}] No IPv6 addresses found, skipping metric {metric}")

    #         all_dfs.append(df_filtered)
    #         print(f"[{region}] The {self.instance_type} data is obtained!")

    #     if all_dfs:
    #         result_df = pd.concat(all_dfs, axis=0, ignore_index=True)
    #         if auto_accumulate:
    #             self.instance_data = pd.concat([self.instance_data, result_df], axis=0, ignore_index=True)
    #         return result_df
    #     else:
    #         return pd.DataFrame()
