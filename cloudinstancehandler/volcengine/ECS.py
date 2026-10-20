# coding: utf-8

import asyncio
import json
from typing import List, Dict, Any, Optional, Union
from pprint import pprint
import pandas as pd

from cloudinstancehandler.volcengine.VolcInstance import VolcInstance
from cloudinstancehandler.common.funcs import get_time_range

import volcenginesdkcore
import volcenginesdkecs
import volcenginesdkvpc
from volcenginesdkcore.rest import ApiException


class ECS(VolcInstance):
    """
    火山云弹性云服务器 (ECS)

    重构要点：
    - 使用异步并发模式（参考阿里云ECS.py）
    - 方法命名规范与阿里云对齐
    - 统一使用 instance_info 存储实例信息
    - 支持批量查询和分页
    """
    def __init__(self, access_key: str, secret_key: str) -> None:
        super().__init__(access_key, secret_key)
        self.namespace = "VCM_ECS"
        self.sub_namespace = "Instance"
        self.specification_info: pd.DataFrame = pd.DataFrame()
        self.regions: pd.DataFrame = pd.DataFrame()

    async def fetch_regions(
        self,
        raw_data: bool = False,
        region_id: str = "cn-beijing",
        page_size: int = 100
    ) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """
        获取所有可用地域列表（异步，next_token 分页）

        对齐 aliyun/ECS.py::fetch_regions_async：拉取后存入 self.regions
        （pd.DataFrame，对应 aliyun 的 self.Regions，新范式统一 snake_case）；
        签名与本包 RDS.fetch_regions 保持一致（raw_data 控制返回形态）。

        官方接口：ECS DescribeRegions（version 2020-04-01）
        https://api.volcengine.com/api-explorer?action=DescribeRegions&serviceCode=ecs&version=2020-04-01

        注意：火山云该接口每个地域仅返回 region_id 字段（无地域显示名），
        与阿里云（含 local_name 等）不同。

        Args:
            raw_data: 为 True 时返回原始 List[Dict]，否则返回 pd.DataFrame
            region_id: 发起请求的接入地域（DescribeRegions 返回全局地域列表，
                       该参数仅指定请求入口），默认 cn-beijing
            page_size: 每页返回数量（max_results）

        Returns:
            Union[pd.DataFrame, List[Dict]]: 地域列表；接口异常时返回已获取的部分
        """
        configuration = self._create_configuration(region_id)
        api_instance = volcenginesdkecs.ECSApi(volcenginesdkcore.ApiClient(configuration))
        loop = asyncio.get_running_loop()

        raw_records: List[Dict[str, Any]] = []
        # 注意：DescribeRegions 对 next_token 严格校验，首页请求必须不传该参数
        next_token = None
        while True:
            try:
                def sync_fetch():
                    describe_regions_request = volcenginesdkecs.DescribeRegionsRequest(
                        max_results=page_size
                    )
                    if next_token:
                        describe_regions_request.next_token = next_token
                    response = api_instance.describe_regions(describe_regions_request)
                    return response.to_dict()

                response_dict = await loop.run_in_executor(None, sync_fetch)
                regions = response_dict.get('regions') or []
                raw_records.extend(regions)
                next_token = response_dict.get('next_token')
                if not next_token:
                    break
            except ApiException as e:
                print(f"Exception when fetching regions: {e}\n")
                break

        self.regions = pd.DataFrame(raw_records)

        if raw_data:
            return raw_records
        return self.regions

    async def fetch_instance_info(
        self,
        region_id: str,
        instance_id_list: Optional[List[str]] = None,
        instance_name: Optional[str] = None,
        eip_addresses_list: Optional[List[str]] = None,
        primary_ip_address: Optional[str] = None,
        instance_charge_type: str = "PrePaid",
        page_size: int = 100,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取ECS实例信息（异步）

        Args:
            region_id: 地域ID
            instance_id_list: 实例ID列表，用于查询特定实例
            instance_name: 实例名称（支持模糊匹配）
            eip_addresses_list: EIP地址列表
            primary_ip_address: 主IP地址
            instance_charge_type: 计费类型，'PrePaid'（默认）| 'PostPaid' | 'all'（不过滤）
            page_size: 每页返回数量（最大100）
            auto_accumulate: 是否自动累积到 instance_info

        Returns:
            List[Dict[str, Any]]: 原始记录列表
        """
        if instance_charge_type not in ["PrePaid", "PostPaid", "all"]:
            raise ValueError("instance_charge_type must be 'PrePaid', 'PostPaid' or 'all'")

        configuration = self._create_configuration(region_id)
        volcenginesdkcore.Configuration.set_default(configuration)
        api_instance = volcenginesdkecs.ECSApi()

        describe_instances_request = volcenginesdkecs.DescribeInstancesRequest(
            max_results=page_size,
        )

        if instance_name:
            describe_instances_request.instance_name = instance_name
        if instance_charge_type and instance_charge_type != "all":
            describe_instances_request.instance_charge_type = instance_charge_type
        if eip_addresses_list:
            describe_instances_request.eip_addresses = eip_addresses_list
        if primary_ip_address:
            describe_instances_request.primary_ip_address = primary_ip_address

        # DescribeInstances 的 InstanceIds 单次请求上限 100 个，超出自动分批
        if instance_id_list:
            id_batches = [instance_id_list[i:i + 100] for i in range(0, len(instance_id_list), 100)]
        else:
            id_batches = [None]

        raw_records = []
        # 火山云SDK不支持原生async，使用run_in_executor包装
        loop = asyncio.get_running_loop()
        for batch_ids in id_batches:
            if batch_ids:
                describe_instances_request.instance_ids = batch_ids
            next_token = True

            while next_token:
                try:
                    def sync_fetch():
                        describe_instances_request.next_token = next_token
                        response = api_instance.describe_instances(describe_instances_request)
                        return response.to_dict()

                    response_dict = await loop.run_in_executor(None, sync_fetch)
                    instances = response_dict.get('instances', [])
                    raw_records.extend(instances)
                    next_token = response_dict.get('next_token')
                except ApiException as e:
                    print(f"Exception occurred while fetching instances in {region_id}: {e}\n")
                    break

        # 添加region_id字段（火山云API返回数据中不包含）
        for record in raw_records:
            record['region_id'] = region_id

        if auto_accumulate and raw_records:
            self._accumulate_instance_info(raw_records)

        return raw_records

    async def fetch_specification_info(
        self,
        region_id: str = "cn-beijing",
        instance_type_ids_list: Optional[List[str]] = None,
        merge: bool = True
    ) -> pd.DataFrame:
        """
        获取ECS实例规格信息（异步）

        Args:
            region_id: 地域ID
            instance_type_ids_list: 规格ID列表
            merge: 是否合并到 instance_info

        Returns:
            pd.DataFrame: 规格信息
        """
        configuration = self._create_configuration(region_id)
        volcenginesdkcore.Configuration.set_default(configuration)
        api_instance = volcenginesdkecs.ECSApi()

        describe_instance_types_request = volcenginesdkecs.DescribeInstanceTypesRequest()

        # 如果 instance_info 不为空，优先查询已获取实例的规格信息
        if not self.instance_info.empty:
            unique_type_ids = self.instance_info['instance_type_id'].unique().tolist()
            if len(unique_type_ids) <= 100:
                describe_instance_types_request.max_results = 100
                describe_instance_types_request.instance_type_ids = unique_type_ids
            else:
                describe_instance_types_request.max_results = 1000
        elif instance_type_ids_list:
            if len(instance_type_ids_list) <= 100:
                describe_instance_types_request.max_results = 100
                describe_instance_types_request.instance_type_ids = instance_type_ids_list
            else:
                raise ValueError('instance_type_ids_list length must be less than or equal to 100')
        else:
            raise ValueError('Either instance_info or instance_type_ids_list must not be empty')

        raw_records = []
        next_token = True

        while next_token:
            try:
                loop = asyncio.get_running_loop()

                def sync_fetch():
                    describe_instance_types_request.next_token = next_token
                    response = api_instance.describe_instance_types(describe_instance_types_request)
                    return response.to_dict()

                response_dict = await loop.run_in_executor(None, sync_fetch)
                instance_types = response_dict.get('instance_types', [])
                raw_records.extend(instance_types)
                next_token = response_dict.get('next_token')
            except ApiException as e:
                print(f"Exception when calling API in {region_id}: {e}\n")
                break

        df_data = pd.DataFrame(raw_records)
        self.specification_info = df_data

        # 展开 network 字段
        if not df_data.empty and 'network' in df_data.columns:
            normalized_df = pd.json_normalize(df_data['network'])
            prefix = "network_"
            normalized_df.columns = [f"{prefix}{col}" for col in normalized_df.columns]
            df_data = df_data.join(normalized_df)

        if merge and not self.instance_info.empty:
            self.instance_info = pd.merge(
                self.instance_info,
                df_data,
                on='instance_type_id',
                how='left'
            )

        return df_data

    async def modify_instance_attribute(
        self,
        region_id: str,
        instance_id: str,
        instance_name: Optional[str] = None,
        description: Optional[str] = None
    ) -> bool:
        """
        修改实例属性（如实例名称）

        Args:
            region_id: 地域ID
            instance_id: 实例ID
            instance_name: 实例名称
            description: 描述信息

        Returns:
            bool: 操作是否成功
        """
        configuration = self._create_configuration(region_id)
        volcenginesdkcore.Configuration.set_default(configuration)
        api_instance = volcenginesdkecs.ECSApi()

        try:
            loop = asyncio.get_running_loop()

            def sync_modify():
                modify_instance_attribute_request = volcenginesdkecs.ModifyInstanceAttributeRequest(
                    instance_id=instance_id
                )
                if instance_name:
                    modify_instance_attribute_request.instance_name = instance_name
                if description:
                    modify_instance_attribute_request.description = description
                return api_instance.modify_instance_attribute(modify_instance_attribute_request)

            await loop.run_in_executor(None, sync_modify)
            return True
        except ApiException as e:
            print(f"Exception when modifying instance {instance_id} in {region_id}: {e}\n")
            return False

    def extract_eni_from_instance_info(self) -> pd.DataFrame:
        """
        从ECS实例信息中提取弹性网卡（ENI）信息

        Returns:
            pd.DataFrame: ENI信息表
        """
        df_eni = pd.DataFrame()
        if self.instance_info.empty:
            print(f'[Info] No {self.instance_type} Info Data Found...')
            return df_eni

        df = self.instance_info.copy()
        if 'network_interfaces' in df.columns:
            df['NetworkInterfaces_List'] = df['network_interfaces'].apply(
                lambda x: x if isinstance(x, list) else []
            )
        else:
            # 火山云ECS API返回的字段可能不同，需要适配
            print("[Warning] 'network_interfaces' field not found in instance_info")
            return df_eni

        df = df[df['NetworkInterfaces_List'].apply(len) > 0]
        df = df.explode('NetworkInterfaces_List').reset_index(drop=True)
        eni_details = pd.json_normalize(df['NetworkInterfaces_List'].tolist())

        df_eni = pd.concat([df[['instance_name', 'instance_id', 'region_id']], eni_details], axis=1)
        return df_eni


class ENI(VolcInstance):
    """
    火山云弹性网卡（Network Interface）
    """
    def __init__(self, access_key: str, secret_key: str) -> None:
        super().__init__(access_key, secret_key)

    async def fetch_instance_info(
        self,
        region_id: str,
        instance_id_list: Optional[List[str]] = None,
        page_size: int = 100,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取弹性网卡（ENI）信息（异步，next_token 分页）

        注意：
        - DescribeNetworkInterfaces 接口属于 VPC 产品线
          （volcenginesdkvpc.VPCApi.describe_network_interfaces），
          返回数据在 network_interface_sets 字段中
        - 网卡挂载的 ECS 实例 ID 位于 attachment.instance_id 子字段，
          此处将其提升到记录顶层的 instance_id 字段，
          以便下游（如 EIP.query_binded_info）统一使用；未挂载时为 None

        Args:
            region_id: 地域ID
            instance_id_list: 网卡ID列表（network_interface_id）
            page_size: 每页返回数量（max_results）
            auto_accumulate: 是否自动累积到 instance_info

        Returns:
            List[Dict[str, Any]]: 原始记录列表（已提升 instance_id、注入 region_id 字段）
        """
        configuration = self._create_configuration(region_id)
        api_instance = volcenginesdkvpc.VPCApi(volcenginesdkcore.ApiClient(configuration))

        # DescribeNetworkInterfaces 的 NetworkInterfaceIds 单次请求上限 100 个，
        # 超出会报 LimitExceeded.NetworkInterfaceIDs，此处自动分批
        if instance_id_list:
            id_batches = [instance_id_list[i:i + 100] for i in range(0, len(instance_id_list), 100)]
        else:
            id_batches = [None]

        raw_records = []
        loop = asyncio.get_running_loop()
        for batch_ids in id_batches:
            describe_network_interfaces_request = volcenginesdkvpc.DescribeNetworkInterfacesRequest(
                max_results=page_size
            )
            if batch_ids:
                describe_network_interfaces_request.network_interface_ids = batch_ids

            # 注意：DescribeNetworkInterfaces 对 next_token 有严格校验，
            # 首页请求必须不传该参数（传 True/字符串会报 InvalidNextToken.Malformed）
            next_token = None
            while True:
                try:
                    def sync_fetch():
                        if next_token:
                            describe_network_interfaces_request.next_token = next_token
                        response = api_instance.describe_network_interfaces(describe_network_interfaces_request)
                        return response.to_dict()

                    response_dict = await loop.run_in_executor(None, sync_fetch)
                    network_interfaces = response_dict.get('network_interface_sets') or []
                    raw_records.extend(network_interfaces)
                    next_token = response_dict.get('next_token')
                    if not next_token:
                        break
                except ApiException as e:
                    print(f"Exception when fetching ENI info in {region_id}: {e}\n")
                    break

        # 提升挂载的 ECS 实例 ID 至顶层字段，并注入 region_id（接口返回中无该字段）
        for record in raw_records:
            attachment = record.get('attachment') or {}
            record['instance_id'] = attachment.get('instance_id')
            record['region_id'] = region_id

        if auto_accumulate and raw_records:
            self._accumulate_instance_info(raw_records)

        return raw_records


class Disks(VolcInstance):
    """
    火山云云盘（Disk）管理
    """
    def __init__(self, access_key: str, secret_key: str) -> None:
        super().__init__(access_key, secret_key)
        self.namespace = "VCM_ECS"
        self.sub_namespace = "Disk"

    async def fetch_instance_info(
        self,
        region_id: str,
        disk_id_list: Optional[List[str]] = None,
        instance_id: Optional[str] = None,
        disk_type: str = "all",
        page_size: int = 100,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取云盘信息（异步）

        Args:
            region_id: 地域ID
            disk_id_list: 云盘ID列表
            instance_id: 实例ID（查询该实例下的所有云盘）
            disk_type: 云盘类型，'all' | 'system' | 'data'
            page_size: 每页返回数量
            auto_accumulate: 是否自动累积到 instance_info

        Returns:
            List[Dict[str, Any]]: 原始记录列表
        """
        if disk_type not in ['all', 'system', 'data']:
            raise ValueError('disk_type must be "all", "system", or "data"')

        configuration = self._create_configuration(region_id)
        volcenginesdkcore.Configuration.set_default(configuration)
        api_instance = volcenginesdkecs.ECSApi()

        describe_disks_request = volcenginesdkecs.DescribeDisksRequest(
            max_results=page_size
        )

        if disk_type != 'all':
            describe_disks_request.disk_type = disk_type
        if disk_id_list:
            describe_disks_request.disk_ids = disk_id_list
        if instance_id:
            describe_disks_request.instance_id = instance_id

        raw_records = []
        next_token = True

        while next_token:
            try:
                loop = asyncio.get_running_loop()

                def sync_fetch():
                    describe_disks_request.next_token = next_token
                    response = api_instance.describe_disks(describe_disks_request)
                    return response.to_dict()

                response_dict = await loop.run_in_executor(None, sync_fetch)
                disks = response_dict.get('disks', [])
                raw_records.extend(disks)
                next_token = response_dict.get('next_token')
            except ApiException as e:
                print(f"Exception when fetching disks in {region_id}: {e}\n")
                break

        if auto_accumulate and raw_records:
            self._accumulate_instance_info(raw_records)

        return raw_records

    async def create_snapshot(
        self,
        region_id: str,
        disk_id: str,
        snapshot_name: str = "created_by_CIH",
        description: str = "CIH Snapshot",
        retention_days: int = 30
    ) -> Optional[Dict[str, Any]]:
        """
        创建快照（异步）

        Args:
            region_id: 地域ID
            disk_id: 云盘ID
            snapshot_name: 快照名称
            description: 快照描述
            retention_days: 保留天数

        Returns:
            Optional[Dict[str, Any]]: 快照信息，失败返回 None
        """
        configuration = self._create_configuration(region_id)
        volcenginesdkcore.Configuration.set_default(configuration)
        api_instance = volcenginesdkecs.ECSApi()

        try:
            loop = asyncio.get_running_loop()

            def sync_create():
                create_snapshot_request = volcenginesdkecs.CreateSnapshotRequest(
                    disk_id=disk_id,
                    snapshot_name=snapshot_name,
                    description=description,
                    retention_days=retention_days
                )
                response = api_instance.create_snapshot(create_snapshot_request)
                return response.to_dict()

            result = await loop.run_in_executor(None, sync_create)
            return result
        except ApiException as e:
            print(f"Create Snapshot Failed, DiskId: {disk_id}, Error: {e}\n")
            return None
