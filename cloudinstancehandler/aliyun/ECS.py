# CloudInstanceHandler/cloudinstancehandler/aliyun/ECS.py

import pandas as pd
import asyncio
from typing import List, Dict, Any, Optional
from pprint import pprint

from cloudinstancehandler.aliyun.AliyunInstance import AliyunInstance
from cloudinstancehandler.common.funcs import get_time_range

# --- 新版 Tea SDK 导入 ---
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_ecs20140526.client import Client as EcsClient
from alibabacloud_ecs20140526 import models as ecs_models

import json

class ECS(AliyunInstance):
    def __init__(self, access_key: str, secret_key: str) -> None:
        super().__init__(access_key, secret_key)
        self.namespace = "acs_ecs_dashboard"
        self.ProductCategory = "ecs"
        self.ENIsFromAllECS = None
        self.Regions = None

    def _create_ecs_client(self, region_id: str) -> EcsClient:
        """
        创建 ECS 客户端实例
        """
        config = open_api_models.Config(
            access_key_id=self.access_key,
            access_key_secret=self.secret_key,
            endpoint=f'ecs.{region_id}.aliyuncs.com'
        )
        return EcsClient(config)

    async def fetch_regions_async(self):
        """
        获取所有可用的地域列表（异步）

        Returns:
            List[Dict]: 地域信息列表，每个元素是一个字典
        """
        client = self._create_ecs_client('cn-hangzhou')
        request = ecs_models.DescribeRegionsRequest()

        try:
            resp = await client.describe_regions_with_options_async(request, self.runtime)
            body = resp.body
            regions = body.regions.region or []
            self.Regions = pd.DataFrame([item.to_map() for item in regions])
            return [item.to_map() for item in regions]  # 返回原始列表字典
        except Exception as e:
            print(f"[Error] Failed to fetch regions: {e}")
            return []

    async def fetch_instance_info(
        self,
        region_id: str,
        instance_list: Optional[List[str]] = None,
        instance_name: Optional[str] = None,
        page_size: int = 100,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取 ECS 实例信息（异步）

        Args:
            region_id: 地域 ID
            instance_list: 实例 ID 列表，用于查询特定实例
            instance_name: 实例名称（支持模糊匹配）
            page_size: 每页返回的数量（最大 100）
            auto_accumulate: 是否自动累积到 self.instance_info

        Returns:
            List[Dict[str, Any]]: 原始记录列表
        """
        raw_records = []

        if instance_list:
            # 批量查询指定实例
            raw_records = await self._fetch_instances_by_ids_async(region_id, instance_list, page_size)
        elif instance_name:
            # 按实例名称模糊匹配
            raw_records = await self._fetch_instances_by_name_async(region_id, instance_name, page_size)
        else:
            # 查询该地域下所有实例
            raw_records = await self._fetch_all_instances_async(region_id, page_size)

        if auto_accumulate and raw_records:
            self._accumulate_instance_info(raw_records)

        return raw_records

    async def _fetch_instances_by_ids_async(
        self,
        region_id: str,
        instance_ids: List[str],
        page_size: int = 100
    ) -> List[Dict[str, Any]]:
        """
        按实例 ID 批量查询（分批处理，每批最多100个实例）
        """
        page_size = min(100, page_size)
        client = self._create_ecs_client(region_id)

        async def _batch_fetch(batch_list: List[str], sem: asyncio.Semaphore) -> List[Dict]:
            async with sem:
                # instance_ids 参数需要传入 JSON 字符串格式的数组
                print(json.dumps(batch_list))
                req = ecs_models.DescribeInstancesRequest(
                    region_id=region_id,
                    instance_ids=json.dumps(batch_list),  # JSON 格式字符串
                    max_results=page_size
                )
                try:
                    resp = await client.describe_instances_with_options_async(req, self.runtime)
                    items = resp.body.instances.instance or []
                    return [item.to_map() for item in items]
                except Exception as e:
                    print(f"[Error] Failed to fetch ECS instances {batch_list} in {region_id}: {e}")
                    return []

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

    async def _fetch_instances_by_name_async(
        self,
        region_id: str,
        instance_name: str,
        page_size: int = 100
    ) -> List[Dict[str, Any]]:
        """
        按实例名称（支持模糊匹配）查询
        """
        client = self._create_ecs_client(region_id)
        req = ecs_models.DescribeInstancesRequest(
            region_id=region_id,
            max_results=page_size,
            instance_name=instance_name
        )

        records = []
        while True:
            try:
                resp = await client.describe_instances_with_options_async(req, self.runtime)
                body = resp.body
                items = body.instances.instance or []
                if not items:
                    break
                records.extend([item.to_map() for item in items])
                if body.next_token:
                    req.next_token = body.next_token
                else:
                    break
            except Exception as e:
                print(f"[Error] Failed to fetch ECS instances by name '{instance_name}' in {region_id}: {e}")
                break

        return records

    async def _fetch_all_instances_async(
        self,
        region_id: str,
        page_size: int = 100
    ) -> List[Dict[str, Any]]:
        """
        查询该地域下所有 ECS 实例（支持分页）
        """
        client = self._create_ecs_client(region_id)
        req = ecs_models.DescribeInstancesRequest(
            region_id=region_id,
            max_results=page_size
        )

        records = []
        while True:
            try:
                resp = await client.describe_instances_with_options_async(req, self.runtime)
                body = resp.body
                items = body.instances.instance or []
                if not items:
                    break
                records.extend([item.to_map() for item in items])
                if body.next_token:
                    req.next_token = body.next_token
                else:
                    break
            except Exception as e:
                print(f"[Error] Failed to fetch ECS instances in {region_id}: {e}")
                break

        return records

    def extract_eip_from_ins_info(self) -> pd.DataFrame:
        """
        从 ECS 实例信息中提取弹性网卡（ENI）信息
        """
        df_ENI = pd.DataFrame()
        if self.instance_info.empty:
            print(f'[Info] No {self.instance_type} Info Data Found...')
            return df_ENI

        df = self.instance_info.copy()
        df['NetworkInterfaces_List'] = df['NetworkInterfaces'].apply(
            lambda x: x.get('NetworkInterface', []) if isinstance(x, dict) else []
        )

        df = df[df['NetworkInterfaces_List'].apply(len) > 0]
        df = df.explode('NetworkInterfaces_List').reset_index(drop=True)
        eni_details = pd.json_normalize(df['NetworkInterfaces_List'].tolist())

        df_ENI = pd.concat([df[['InstanceName', 'InstanceId', 'RegionId']], eni_details], axis=1)
        self.ENIsFromAllECS = df_ENI
        return df_ENI

    async def modify_instance_attribute(
        self,
        region_id: str,
        instance_id: str,
        instance_name: Optional[str] = None,
        description: Optional[str] = None
    ) -> bool:
        """
        修改实例属性（如实例名称）
        """
        client = self._create_ecs_client(region_id)
        req = ecs_models.ModifyInstanceAttributeRequest(
            region_id=region_id,
            instance_id=instance_id
        )

        if instance_name is not None:
            req.instance_name = instance_name
        if description is not None:
            req.description = description

        try:
            resp = await client.modify_instance_attribute_with_options_async(req, self.runtime)
            return resp.body.success if hasattr(resp.body, 'success') else True
        except Exception as e:
            print(f"[Error] Failed to modify instance {instance_id} in {region_id}: {e}")
            return False


class ENI(AliyunInstance):
    """
    弹性网卡（Network Interface）管理
    """
    def __init__(self, access_key: str, secret_key: str) -> None:
        super().__init__(access_key, secret_key)

    def _create_ecs_client(self, region_id: str) -> EcsClient:
        config = open_api_models.Config(
            access_key_id=self.access_key,
            access_key_secret=self.secret_key,
            endpoint=f'ecs.{region_id}.aliyuncs.com'
        )
        return EcsClient(config)

    async def fetch_instance_info(
        self,
        region_id: str,
        instance_list: Optional[List[str]] = None,
        page_size: int = 500,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取弹性网卡信息
        """
        client = self._create_ecs_client(region_id)
        req = ecs_models.DescribeNetworkInterfacesRequest(
            region_id=region_id,
            max_results=page_size
        )

        if instance_list:
            req.network_interface_id = instance_list

        records = []
        while True:
            try:
                resp = await client.describe_network_interfaces_with_options_async(req, self.runtime)
                body = resp.body
                items = body.network_interface_sets.network_interface_set or []
                if not items:
                    break
                records.extend([item.to_map() for item in items])
                if body.next_token:
                    req.next_token = body.next_token
                else:
                    break
            except Exception as e:
                print(f"[Error] Failed to fetch ENI info in {region_id}: {e}")
                break
        if auto_accumulate and records:
            self._accumulate_instance_info(records, custom_field={"RegionId": region_id})
        return records

    async def create_network_interface(
        self,
        region_id: str,
        v_switch_id: str,
        security_group_id: Optional[str] = None,
        security_group_ids: Optional[List[str]] = None,
        network_interface_name: Optional[str] = None,
        description: Optional[str] = None,
        primary_ip_address: Optional[str] = None,
        resource_group_id: Optional[str] = None,
        instance_type: str = "Secondary",
        client_token: Optional[str] = None,
        auto_accumulate: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        创建弹性网卡（ENI）

        目前仅适配 CreateNetworkInterface 接口的基础/必填参数，
        其余参数（IPv6、队列数、连接跟踪等）后续按需扩展。

        Args:
            region_id: 地域 ID（必填）
            v_switch_id: 交换机 ID，决定网卡所属 VPC 与可用区（必填）
            security_group_id: 安全组 ID，与 security_group_ids 二选一
            security_group_ids: 安全组 ID 列表（最多 5 个），与 security_group_id 二选一
            network_interface_name: 网卡名称（可选）
            description: 网卡描述（可选）
            primary_ip_address: 主私网 IP，需为 v_switch_id 所在网段内的空闲 IP（可选，默认自动分配）
            resource_group_id: 资源组 ID（可选）
            instance_type: 网卡类型，默认 "Secondary"（辅助网卡）
            client_token: 幂等性 Token（可选）
            auto_accumulate: 是否将创建结果累积到 self.instance_data

        Returns:
            Optional[Dict[str, Any]]: 创建成功返回网卡信息（含 NetworkInterfaceId、MacAddress、
                                      PrivateIpAddress、Status 等字段，并补充 RegionId），失败返回 None
        """
        if not security_group_id and not security_group_ids:
            raise ValueError("Either security_group_id or security_group_ids must be specified")

        client = self._create_ecs_client(region_id)
        req = ecs_models.CreateNetworkInterfaceRequest(
            region_id=region_id,
            v_switch_id=v_switch_id,
            instance_type=instance_type
        )

        if security_group_id:
            req.security_group_id = security_group_id
        if security_group_ids:
            req.security_group_ids = security_group_ids
        if network_interface_name is not None:
            req.network_interface_name = network_interface_name
        if description is not None:
            req.description = description
        if primary_ip_address is not None:
            req.primary_ip_address = primary_ip_address
        if resource_group_id is not None:
            req.resource_group_id = resource_group_id
        if client_token is not None:
            req.client_token = client_token

        try:
            resp = await client.create_network_interface_with_options_async(req, self.runtime)
            record = resp.body.to_map()
            record['RegionId'] = region_id

            if auto_accumulate:
                df = pd.DataFrame([record])
                self.instance_data = pd.concat([self.instance_data, df], axis=0, ignore_index=True)

            return record
        except Exception as e:
            print(f"[Error] Create NetworkInterface Failed in {region_id}, VSwitchId: {v_switch_id}, Error: {e}")
            return None

    async def attach_network_interface(
        self,
        region_id: str,
        network_interface_id: str,
        instance_id: str,
        network_card_index: Optional[int] = None,
        auto_accumulate: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        将弹性网卡（ENI）绑定到 ECS 实例上

        目前仅适配 AttachNetworkInterface 接口的基础/必填参数。
        注意：该接口不提供 client_token 参数，故不支持幂等重试；
        trunk_network_instance_id、wait_for_network_configuration_ready 官方已标记为不可用/不再支持，未适配。

        常见约束（不满足时接口会报错）：
        - 网卡需处于 Available（可用）状态，且未绑定其他实例
        - ECS 实例需处于 Running（运行中）或 Stopped（已停止）状态
        - 网卡与实例需属于同一 VPC、同一可用区（可用区由网卡所属交换机决定）
        - 实例规格可绑定的网卡数量有上限

        Args:
            region_id: 地域 ID（必填）
            network_interface_id: 弹性网卡 ID，如 'eni-uf662o4432llz4w1ycge'（必填）
            instance_id: 待绑定的 ECS 实例 ID，如 'i-uf6xxxxxxxx'（必填）
            network_card_index: 网卡索引（NIC index），仅当实例规格支持多物理网卡时需要指定，
                                取值范围与实例规格有关；实例规格不支持时留空即可（可选）
            auto_accumulate: 是否将绑定结果累积到 self.instance_data

        Returns:
            Optional[Dict[str, Any]]: 绑定成功返回本次操作的记录（包含 RequestId、NetworkInterfaceId、
                                      InstanceId、NetworkCardIndex、RegionId），失败返回 None

        Note:
            AttachNetworkInterface 接口的响应体仅包含 RequestId，因此存入 self.instance_data 的记录
            由请求参数回显 + RequestId 组成。绑定后网卡状态会变为 InUse，
            可通过 fetch_instance_info 回查 InstanceId / Status 确认实际生效。
        """
        client = self._create_ecs_client(region_id)
        req = ecs_models.AttachNetworkInterfaceRequest(
            region_id=region_id,
            network_interface_id=network_interface_id,
            instance_id=instance_id
        )

        if network_card_index is not None:
            req.network_card_index = network_card_index

        try:
            resp = await client.attach_network_interface_with_options_async(req, self.runtime)
            record = resp.body.to_map()
            # 响应体仅有 RequestId，补充请求参数回显与 RegionId，便于后续追踪绑定关系
            record.update({
                "RegionId": region_id,
                "NetworkInterfaceId": network_interface_id,
                "InstanceId": instance_id,
                "NetworkCardIndex": network_card_index
            })

            if auto_accumulate:
                df = pd.DataFrame([record])
                self.instance_data = pd.concat([self.instance_data, df], axis=0, ignore_index=True)

            return record
        except Exception as e:
            print(f"[Error] Attach NetworkInterface Failed in {region_id}, NetworkInterfaceId: {network_interface_id}, InstanceId: {instance_id}, Error: {e}")
            return None


class Disks(AliyunInstance):
    """
    云盘（Disk）管理
    """
    def __init__(self, access_key: str, secret_key: str) -> None:
        super().__init__(access_key, secret_key)
        self.namespace = "acs_ecs_dashboard"
        self.ProductCategory = "ecs"

    def _create_ecs_client(self, region_id: str) -> EcsClient:
        config = open_api_models.Config(
            access_key_id=self.access_key,
            access_key_secret=self.secret_key,
            endpoint=f'ecs.{region_id}.aliyuncs.com'
        )
        return EcsClient(config)

    async def fetch_instance_info(
        self,
        region_id: str,
        disk_ids_list: Optional[List[str]] = None,
        instance_id: Optional[str] = None,
        instance_name: Optional[str] = None,
        disk_type: str = "all",
        status: str = "All",
        page_size: int = 100,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取云盘信息
        """
        if disk_type not in ['all', 'system', 'data']:
            raise ValueError('disk_type must be "all", "system", or "data"')

        client = self._create_ecs_client(region_id)
        req = ecs_models.DescribeDisksRequest(
            region_id=region_id,
            disk_type=disk_type,
            status=status,
            page_size=page_size,
            page_number=1
        )

        if instance_id:
            req.instance_id = instance_id
        if disk_ids_list:
            req.disk_id = disk_ids_list

        records = []
        while True:
            try:
                resp = await client.describe_disks_with_options_async(req, self.runtime)
                body = resp.body
                items = body.disks.disk or []
                if not items:
                    break
                records.extend([item.to_map() for item in items])
                total_count = body.total_count or 0
                if len(records) >= total_count:
                    break
                req.page_number += 1
            except Exception as e:
                print(f"[Error] Failed to fetch disks in {region_id}: {e}")
                break

        if auto_accumulate and records:
            self._accumulate_instance_info(records)

        return records

    async def create_snapshot(
        self,
        region_id: str,
        disk_id: str,
        retention_days: int = 30,
        snapshot_name: str = "created_by_CIH",
        description: str = "CIH Snapshot"
    ) -> Optional[Dict[str, Any]]:
        """
        创建快照
        """
        client = self._create_ecs_client(region_id)
        req = ecs_models.CreateSnapshotRequest(
            region_id=region_id,
            disk_id=disk_id,
            snapshot_name=snapshot_name,
            description=description,
            retention_days=retention_days
        )

        try:
            resp = await client.create_snapshot_with_options_async(req, self.runtime)
            return resp.body.to_map()
        except Exception as e:
            print(f"[Error] Create Snapshot Failed, DiskId: {disk_id}, Error: {e}")
            return None


class AutoSnapshotPolicy(AliyunInstance):
    """
    自动快照策略管理

    核心功能：
    1. 查询自动快照策略列表
    2. 查询策略关联的磁盘
    3. 查询磁盘关联的策略
    """
    def __init__(self, access_key: str, secret_key: str) -> None:
        super().__init__(access_key, secret_key)

    def _create_ecs_client(self, region_id: str) -> EcsClient:
        config = open_api_models.Config(
            access_key_id=self.access_key,
            access_key_secret=self.secret_key,
            endpoint=f'ecs.{region_id}.aliyuncs.com'
        )
        return EcsClient(config)

    async def fetch_instance_info(
        self,
        region_id: str,
        policy_id: Optional[str] = None,
        policy_name: Optional[str] = None,
        page_size: int = 100,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取自动快照策略列表（支持全量、按ID、按名称查询）

        Args:
            region_id: 地域 ID
            policy_id: 策略 ID，用于查询特定策略（可选）
            policy_name: 策略名称，用于模糊查询（可选）
            page_size: 每页返回数量（最大 100）
            auto_accumulate: 是否自动累积到 self.instance_info

        Returns:
            List[Dict[str, Any]]: 策略列表原始记录
        """
        client = self._create_ecs_client(region_id)
        req = ecs_models.DescribeAutoSnapshotPolicyExRequest(
            region_id=region_id,
            page_size=page_size,
            page_number=1
        )

        if policy_id:
            req.auto_snapshot_policy_id = policy_id
        if policy_name:
            req.auto_snapshot_policy_name = policy_name

        records = []
        while True:
            try:
                resp = await client.describe_auto_snapshot_policy_ex_with_options_async(req, self.runtime)
                body = resp.body
                items = body.auto_snapshot_policies.auto_snapshot_policy or []
                if not items:
                    break
                records.extend([item.to_map() for item in items])
                total_count = body.total_count or 0
                if len(records) >= total_count:
                    break
                req.page_number += 1
            except Exception as e:
                print(f"[Error] Failed to fetch auto snapshot policies in {region_id}: {e}")
                break

        if auto_accumulate and records:
            self._accumulate_instance_info(records)

        return records

    async def fetch_policy_disk_associations(
        self,
        region_id: str,
        policy_id: Optional[str] = None,
        disk_id: Optional[str] = None,
        page_size: int = 100,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取策略与磁盘的关联关系（必须指定 policy_id 或 disk_id）

        Args:
            region_id: 地域 ID
            policy_id: 策略 ID（可选，只能与 disk_id 二选一）
            disk_id: 磁盘 ID（可选，只能与 policy_id 二选一）
            page_size: 每页返回数量（最大 100）
            auto_accumulate: 是否累积到 self.instance_data

        Returns:
            List[Dict[str, Any]]: 原始记录列表
        """
        if not policy_id and not disk_id:
            raise ValueError("Either policy_id or disk_id must be specified")
        if policy_id and disk_id:
            raise ValueError("policy_id and disk_id cannot be specified at the same time")

        client = self._create_ecs_client(region_id)
        req = ecs_models.DescribeAutoSnapshotPolicyAssociationsRequest(
            region_id=region_id,
            max_results=page_size
        )

        if policy_id:
            req.auto_snapshot_policy_id = policy_id
        elif disk_id:
            req.disk_id = disk_id

        records = []
        while True:
            try:
                resp = await client.describe_auto_snapshot_policy_associations_with_options_async(req, self.runtime)
                body = resp.body
                items = body.auto_snapshot_policy_associations.auto_snapshot_policy_association or []
                if not items:
                    break
                records.extend([item.to_map() for item in items])
                if body.next_token:
                    req.next_token = body.next_token
                else:
                    break
            except Exception as e:
                print(f"[Error] Failed to fetch policy-disk associations in {region_id}: {e}")
                break

        if auto_accumulate and records:
            df = pd.DataFrame(records)
            self.instance_data = pd.concat([self.instance_data, df], axis=0, ignore_index=True)

        return records

    async def fetch_disk_policies(
        self,
        region_id: str,
        disk_ids: Optional[List[str]] = None,
        policy_ids: Optional[List[str]] = None,
        page_size: int = 100,
        auto_accumulate: bool = True
    ) -> pd.DataFrame:
        """
        并发批量查询多个磁盘或多个策略关联的关系（模拟批量查询）

        Args:
            region_id: 地域 ID
            disk_ids: 磁盘 ID 列表（并发批量查询）
            policy_ids: 策略 ID 列表（并发批量查询）
            page_size: 每页返回数量（最大 100）
            auto_accumulate: 是否累积到 self.instance_data

        Returns:
            pd.DataFrame: 包含所有关联关系
        """
        if not disk_ids and not policy_ids:
            raise ValueError("Either disk_ids or policy_ids must be specified")

        async def _fetch_one(id_value: str, id_type: str, sem: asyncio.Semaphore) -> List[Dict]:
            async with sem:
                try:
                    if id_type == "disk":
                        return await self.fetch_policy_disk_associations(
                            region_id=region_id,
                            disk_id=id_value,
                            page_size=page_size,
                            auto_accumulate=False  # 统一在外部累积
                        )
                    else:
                        return await self.fetch_policy_disk_associations(
                            region_id=region_id,
                            policy_id=id_value,
                            page_size=page_size,
                            auto_accumulate=False  # 统一在外部累积
                        )
                except Exception as e:
                    print(f"[Error] Failed to fetch associations for {id_type} {id_value} in {region_id}: {e}")
                    return []

        semaphore = asyncio.Semaphore(10)
        if disk_ids:
            tasks = [_fetch_one(disk_id, "disk", semaphore) for disk_id in disk_ids]
        else:
            tasks = [_fetch_one(policy_id, "policy", semaphore) for policy_id in policy_ids]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        records = []
        for res in results:
            if isinstance(res, list):
                records.extend(res)
            elif isinstance(res, Exception):
                print(f"[Error] Unexpected task error: {res}")

        df = pd.DataFrame(records)

        if auto_accumulate and not df.empty:
            self.instance_data = pd.concat([self.instance_data, df], axis=0, ignore_index=True)

        return df


class Snapshot(AliyunInstance):
    """
    手动快照管理
    """
    def __init__(self, access_key: str, secret_key: str) -> None:
        super().__init__(access_key, secret_key)
        self.namespace = "acs_ecs_dashboard"
        self.ProductCategory = "ecs"

    def _create_ecs_client(self, region_id: str) -> EcsClient:
        config = open_api_models.Config(
            access_key_id=self.access_key,
            access_key_secret=self.secret_key,
            endpoint=f'ecs.{region_id}.aliyuncs.com'
        )
        return EcsClient(config)

    async def fetch_instance_info(
        self,
        region_id: str,
        snapshot_ids: Optional[List[str]] = None,
        disk_ids: Optional[List[str]] = None,
        status: str = "all",
        page_size: int = 100,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取快照列表
        """
        client = self._create_ecs_client(region_id)
        req = ecs_models.DescribeSnapshotsRequest(
            region_id=region_id,
            status=status,
            page_size=page_size,
            page_number=1
        )

        if snapshot_ids:
            req.snapshot_id = snapshot_ids
        if disk_ids:
            req.disk_id = disk_ids

        records = []
        while True:
            try:
                resp = await client.describe_snapshots_with_options_async(req, self.runtime)
                body = resp.body
                items = body.snapshots.snapshot or []
                if not items:
                    break
                records.extend([item.to_map() for item in items])
                total_count = body.total_count or 0
                if len(records) >= total_count:
                    break
                req.page_number += 1
            except Exception as e:
                print(f"[Error] Failed to fetch snapshots in {region_id}: {e}")
                break

        if auto_accumulate and records:
            self._accumulate_instance_info(records)

        return records
