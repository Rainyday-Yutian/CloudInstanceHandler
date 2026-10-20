# coding: utf-8

import pandas as pd
from typing import List, Optional, Dict, Any
from pprint import pprint

from cloudinstancehandler.hwyun.HwyunInstance import HwyunInstance
from cloudinstancehandler.common.funcs import get_time_range

from huaweicloudsdkcore.auth.credentials import BasicCredentials
from huaweicloudsdkcore.exceptions import exceptions
from huaweicloudsdkecs.v2.region.ecs_region import EcsRegion
from huaweicloudsdkecs.v2 import *


class ECS(HwyunInstance):
    """
    华为云弹性云服务器 (ECS)

    重构要点：
    - query_ins_info: 基于 ListCloudServers 接口（支持批量查询）
    - fetch_instance_info: 基于 NovaListServersDetails 接口（完整实例详情）
    - get_spec_data: 获取规格数据
    - get_ecs_spec: 合并规格信息到 instance_info
    """
    def __init__(self, access_key, secret_key) -> None:
        super().__init__(access_key, secret_key)
        self.namespace = "SYS.ECS"
        self.DimensionsName = "instance_id"
        self.csp_name = "Huawei"
        self.specification_data: pd.DataFrame = pd.DataFrame()

    def query_ins_info(
        self,
        region_id: str,
        instance_id_list: List[str],
        page_size: int = 100,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        查询云服务器列表（轻量级接口，支持批量查询）

        注意：此方法与 fetch_instance_info 基于不同接口，返回数据结构不同
        如需同时使用，请注意数据结构可能冲突

        Args:
            region_id: 地域ID
            instance_id_list: 实例ID列表
            page_size: 每页数量（最大100）
            auto_accumulate: 是否自动累加

        Returns:
            List[Dict]: 原始记录列表
        """
        if page_size > 100:
            page_size = 100

        credentials = BasicCredentials(self.access_key, self.secret_key)
        client = EcsClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(EcsRegion.value_of(region_id)) \
            .build()

        raw_records = []
        try:
            request = ListCloudServersRequest()
            request.limit = page_size

            for idx in range(0, len(instance_id_list), page_size):
                batch_ids = instance_id_list[idx:idx + page_size]
                request.id = batch_ids
                response_json = client.list_cloud_servers(request).to_json_object()
                servers = response_json.get('servers', [])
                raw_records.extend(servers)
        except exceptions.ClientRequestException as e:
            print(f"[Error] Failed to query ECS info in {region_id}: Request id: {e.request_id}\n"
                  f"Status code: {e.status_code}, error code: {e.error_code}\n"
                  f"Error message: {e.error_msg}")

        if auto_accumulate and raw_records:
            self._accumulate_instance_info(raw_records)

        return raw_records

    def fetch_instance_info(
        self,
        region_id: str,
        ip: Optional[str] = None,
        not_tags: str = "CCE-Dynamic-Provisioning-Node",
        page_size: int = 1000,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取云服务器详情列表（Nova接口，完整信息）

        Args:
            region_id: 地域ID
            ip: IP地址过滤
            not_tags: 排除的标签（默认排除CCE动态节点）
            page_size: 每页数量（默认1000）
            auto_accumulate: 是否自动累加

        Returns:
            List[Dict]: 原始记录列表
        """
        credentials = BasicCredentials(self.access_key, self.secret_key)
        client = EcsClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(EcsRegion.value_of(region_id)) \
            .build()

        raw_records = []
        try:
            request = NovaListServersDetailsRequest()
            if not_tags:
                request.not_tags = not_tags
            if ip:
                request.ip = ip

            response_json = client.nova_list_servers_details(request).to_json_object()
            raw_records = response_json.get('servers', [])

            # 补充 region_id 和 spec_id
            for record in raw_records:
                record['region_id'] = region_id
                if 'flavor' in record and 'id' in record['flavor']:
                    record['spec_id'] = record['flavor']['id']
        except exceptions.ClientRequestException as e:
            print(f"[Error] Failed to fetch ECS info in {region_id}: Request id: {e.request_id}\n"
                  f"Status code: {e.status_code}, error code: {e.error_code}\n"
                  f"Error message: {e.error_msg}")

        if auto_accumulate and raw_records:
            self._accumulate_instance_info(raw_records)

        return raw_records

    def get_spec_data(self) -> pd.DataFrame:
        """
        获取云服务器规格数据（全量规格）

        Returns:
            pd.DataFrame: 规格数据表
        """
        credentials = BasicCredentials(self.access_key, self.secret_key)
        client = EcsClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(EcsRegion.value_of("cn-south-1")) \
            .build()

        try:
            request = ListFlavorsRequest()
            response_json = client.list_flavors(request).to_json_object()
            flavors = response_json.get('flavors', [])

            df_data = pd.DataFrame(flavors)
            if not df_data.empty and 'os_extra_specs' in df_data.columns:
                spec_df = pd.json_normalize(df_data['os_extra_specs'])
                df_data = pd.concat([df_data, spec_df], axis=1)

            self.specification_data = df_data
            return df_data
        except exceptions.ClientRequestException as e:
            print(f"[Error] Failed to get spec data: Request id: {e.request_id}\n"
                  f"Status code: {e.status_code}, error code: {e.error_code}\n"
                  f"Error message: {e.error_msg}")
            return pd.DataFrame()

    def get_ecs_spec(self, field_list: Optional[List[str]] = None) -> pd.DataFrame:
        """
        获取并合并ECS规格信息到 instance_info

        Args:
            field_list: 需要保留的字段列表（默认全部字段）

        Returns:
            pd.DataFrame: 合并后的 instance_info
        """
        if self.specification_data.empty:
            self.get_spec_data()

        if field_list is None:
            df_spec = self.specification_data
        else:
            if 'id' not in field_list:
                field_list.append('id')
            df_spec = self.specification_data[field_list]

        # 添加前缀
        df_spec = df_spec.add_prefix('spec_')

        # 合并规格信息
        self.instance_info = pd.merge(
            self.instance_info,
            df_spec,
            how='left',
            left_on='spec_id',
            right_on='spec_id'
        )

        return self.instance_info
