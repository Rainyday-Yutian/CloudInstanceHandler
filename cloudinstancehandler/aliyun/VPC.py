# CloudInstanceHandler/cloudinstancehandler/aliyun/VPC.py

import pandas as pd
import asyncio
from typing import List, Dict, Any, Optional
from pprint import pprint 

from cloudinstancehandler.common.funcs import *
from cloudinstancehandler.aliyun.AliyunInstance import AliyunInstance
from cloudinstancehandler.common.funcs import get_time_range

from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_vpc20160428.client import Client as VpcClient
# from alibabacloud_vpc20160428 import models as vpc_models
from alibabacloud_vpc20160428 import models as vpc_models


from cloudinstancehandler.aliyun.ECS import ECS as ECShandler
from cloudinstancehandler.aliyun.ECS import ENI as ENIhandler
from cloudinstancehandler.aliyun.SLB import SLB as SLBhandler
from cloudinstancehandler.aliyun.NGW import NGW as NGWhandler


class VPC(AliyunInstance):
    def __init__(self, access_key, secret_key) -> None:
        super().__init__(access_key, secret_key)
        self.namespace = "acs_vpc"
        self.ProductCategory = "vpcs"
        
    def fetch_regions(self) -> List[str]:
        pass
        

class CBWP(AliyunInstance):
    def __init__(self, access_key, secret_key) -> None:
        super().__init__(access_key, secret_key)
        self.namespace = "acs_bandwidth_package"
        self.ProductCategory = "sharebandwidthpackages"
        # self.EIPsFromAllCBWP = None

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
        instance_list: List[str] = None,
        # instance_name: str = None,
        page_size: int = 50,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        raw_records = []
        if instance_list:
            raw_records = await self._fetch_single_cbwp_async(region_id, instance_list)
        # elif instance_name:
        #     raw_records = self.filter_by_name(raw_records, instance_name)
        else:
            raw_records = await self._fetch_all_cbwp_async(region_id, page_size)
        if auto_accumulate:
            self._accumulate_instance_info(raw_records)
        return raw_records

    async def _fetch_single_cbwp_async(self, region_id: str, instance_id_list: List[str]) -> List[Dict[str, Any]]:
        client = self._create_vpc_client(region_id)

        # 定义局部原子协程：负责单个 ID 的请求与异常隔离
        async def _fetch_one(bandwidth_package_id: str, sem: asyncio.Semaphore) -> List[Dict]:
            async with sem:  # 获取信号量，控制并发度
                req = vpc_models.DescribeCommonBandwidthPackagesRequest(
                    bandwidth_package_id=bandwidth_package_id,
                    page_size=1,
                    page_number=1
                )
                try:
                    resp = await client.describe_common_bandwidth_packages_with_options_async(req, self.runtime)
                    items = resp.body.common_bandwidth_packages.common_bandwidth_package or []
                    return [item.to_map() for item in items]
                except Exception as e:
                    print(f"[Warn] Failed to fetch CBWP {bandwidth_package_id} in {region_id}: {e}")
                    # log.Warning(f"Failed to fetch CBWP {cbwp_id} in {region_id}: {e}")
                    return []  # 失败返回空列表，不中断整体流程

        # 并发执行，semaphore限流
        semaphore = asyncio.Semaphore(5)
        tasks = [_fetch_one(bandwidth_package_id, semaphore) for bandwidth_package_id in instance_id_list]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        records = []
        for res in results:
            if isinstance(res, list):
                records.extend(res)
            elif isinstance(res, Exception):
                print(f"[Error] Unexpected task error: {res}")

        return records

    async def _fetch_all_cbwp_async(self, region_id: str, page_size: int) -> List[Dict[str, Any]]:
        client = self._create_vpc_client(region_id)
        records: List[Dict[str, Any]] = []
        total_count: Optional[int] = None
        req = vpc_models.DescribeCommonBandwidthPackagesRequest(
            page_size=page_size,
            page_number=1
        )

        # 关于for循环优化至while循环，for循环需要提前获取总数据量并计算页数，而数据如果在请求时变化，如减少，可能会导致拉取失败。
        while True:
            try:
                resp = await client.describe_common_bandwidth_packages_with_options_async(req, self.runtime)
                body = resp.body
                items = body.common_bandwidth_packages.common_bandwidth_package or []
                if not items:
                    break
                records.extend([item.to_map() for item in items])
                if total_count is None:
                    total_count = body.total_count or 0
                if len(records) >= total_count:
                    break
                req.page_number += 1

            except Exception as e:
                print(f"[Error] Failed to fetch page {req.page_number} in {region_id}: {e}")
                break

        return records

    def extract_eip_from_ins_info(self) -> pd.DataFrame:
        all_dfs = []
        for _, row in self.instance_info.iterrows():
            public_ips = row.get('PublicIpAddresses')
            if isinstance(public_ips, dict) and 'PublicIpAddresse' in public_ips:
                df_single = pd.DataFrame(public_ips['PublicIpAddresse'])
                all_dfs.append(df_single)

        if not all_dfs:
            return pd.DataFrame()

        return pd.concat(all_dfs, ignore_index=True)
    
class EIP(AliyunInstance):
    def __init__(self, access_key, secret_key) -> None:
        super().__init__(access_key, secret_key)
        self.namespace = "acs_vpc_eip"
        self.ProductCategory = "eip"
        self.DimensionsName = "instanceId"  # EIP 的维度名称

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
        page_size: int = 100,
        instance_id_list: Optional[List[str]] = None,
        ip_address_list: Optional[List[str]] = None,
        # associate_instance_id: Optional[List[str]] = None,
        status: Optional[str] = None,
        associated_instance_type: Optional[str] = None,
        isp: Optional[str] = None,
        charge_type: Optional[str] = None,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取 EIP 信息。支持多种过滤条件。
        注意：阿里云 API 对多值查询有长度限制，内部会自动分批处理。
        """
        raw_records: List[Dict[str, Any]] = []

        try:
            if instance_id_list or ip_address_list:
                raw_records = await self._fetch_eip_specific_async(
                    region_id, page_size, instance_id_list, ip_address_list
                )
            # elif associate_instance_id:
            #     # 按 AssociatedInstanceId 查询
            #     raw_records = await self._fetch_by_filter_async(
            #         region_id, "AssociatedInstanceId", associate_instance_id, page_size
            #     )
            else:
                raw_records = await self._fetch_eip_by_filter_async(region_id, page_size, status=status, associated_instance_type=associated_instance_type, isp=isp, charge_type=charge_type)

            if auto_accumulate and raw_records:
                self._accumulate_instance_info(raw_records)

        except Exception as e:
            print(f"[Error] Failed to fetch EIP info in {region_id}: {e}")
            raise

        return raw_records

    async def _fetch_eip_specific_async(
        self,
        region_id: str,
        page_size: int = 50,
        allocation_ids: List[str] = None,
        ip_addresses: List[str] =  None
    ) -> List[Dict[str, Any]]:
        if not 1 <= page_size <= 50:
            print(f"[WARNING] page_size must be between 1 and 50, default to {page_size}")
            page_size = 50
        client = self._create_vpc_client(region_id)

        async def _batch_fetch(batch_list: List[str], param_name:str, sem: asyncio.Semaphore) -> List[Dict]:
            async with sem:  # 获取信号量，控制并发度
                req = vpc_models.DescribeEipAddressesRequest(
                    region_id=region_id,  # 必须显式设置 region_id
                    page_size=page_size,
                    page_number=1
                )
                if param_name == "allocation_id":
                    req.allocation_id = ",".join(batch_list)
                elif param_name == "ip_address":
                    req.eip_address = ",".join(batch_list)
                try:
                    resp = await client.describe_eip_addresses_with_options_async(req,self.runtime)
                    items = resp.body.eip_addresses.eip_address or []
                    return [item.to_map() for item in items]
                except Exception as e:
                    print(f"[Error] Failed to fetch EIP info in {region_id}: {e}")
                    return []

        # 并发执行，semaphore限流
        semaphore = asyncio.Semaphore(5)
        # 分片
        all_tasks = []
        if allocation_ids:
            batches = [allocation_ids[i:i+page_size] for i in range(0, len(allocation_ids), page_size)]
            for batch in batches:
                all_tasks.append(_batch_fetch(batch, "allocation_id", semaphore))

        if ip_addresses:
            batches = [ip_addresses[i:i+page_size] for i in range(0, len(ip_addresses), page_size)]
            for batch in batches:
                all_tasks.append(_batch_fetch(batch, "ip_address", semaphore))

        if not all_tasks:
            return []

        results = await asyncio.gather(*all_tasks, return_exceptions=True)

        records = []
        for res in results:
            if isinstance(res, list):
                records.extend(res)
            elif isinstance(res, Exception):
                print(f"[Error] Unexpected task error: {res}")

        # seen_keys = set()
        # unique_records = []
        # for item in records:
        #     uid = item.get('AllocationId') or item.get('EipAddress')
        #     if uid and uid not in seen_keys:
        #         seen_keys.add(uid)
        #         unique_records.append(item)
        # return unique_records
        return records


    async def _fetch_eip_by_filter_async(
        self,
        region_id: str,
        page_size: int = 100,
        status: str = None,
        associated_instance_type: str = None,
        isp: str = None,
        charge_type: str = None
    ) -> List[Dict[str, Any]]:
        # if status not in [None, "Associating", "Unassociating", "InUse", "Available", "Releasing"]:
        #     raise ValueError(f"Invalid status value: {status}. Must be one of Associating, Unassociating, InUse, Available, Releasing.")
        #     # Associating：绑定中。
        #     # Unassociating：解绑中。
        #     # InUse：已分配。
        #     # Available：可用。
        #     # Releasing：释放中。
        # if associated_instance_type not in [None, "EcsInstance", "SlbInstance", "Nat", "HaVip", "NetworkInterface", "IpAddress"]:
        #     raise ValueError(f"Invalid isp value: {isp}. Must be one of BGP, ChinaTelecom, ChinaUnicom, ChinaMobile, ChinaTelecomForBGP, ChinaUnicomForBGP, ChinaMobileForBGP, BGPGroup, Anycast.")
        #     # EcsInstance（默认值）：VPC 类型的 ECS 实例。
        #     # SlbInstance：VPC 类型的 CLB 实例。
        #     # Nat：NAT 网关。
        #     # HaVip：高可用虚拟 IP。
        #     # NetworkInterface：辅助弹性网卡。
        #     # IpAddress：IP 地址。
        # if isp not in [None, "BGP", "ChinaTelecom", "ChinaUnicom", "ChinaMobile", "ChinaTelecomForBGP", "ChinaUnicomForBGP", "ChinaMobileForBGP", "BGPGroup", "Anycast"]:
        #     raise ValueError(f"Invalid isp value: {isp}. Must be one of BGP, ChinaTelecom, ChinaUnicom, ChinaMobile, ChinaTelecomForBGP, ChinaUnicomForBGP, ChinaMobileForBGP, BGPGroup, Anycast.")
        if not 1 <= page_size <= 100:
            print(f"[WARNING] page_size must be between 1 and 100, default to {page_size}")
            page_size = 100
        client = self._create_vpc_client(region_id)
        records: List[Dict[str, Any]] = []
        total_count: Optional[int] = None
        req = vpc_models.DescribeEipAddressesRequest(
            region_id=region_id,  # 必须显式设置 region_id
            page_size=page_size,
            page_number = 1,
            status = status,
            associated_instance_type = associated_instance_type,
            isp = isp,
            charge_type = charge_type
        )

        while True:
            try:
                resp = await client.describe_eip_addresses_with_options_async(req, self.runtime)
                body = resp.body
                # pprint(body.to_map())
                if not body or not body.eip_addresses or not body.eip_addresses.eip_address:
                    break
                items = body.eip_addresses.eip_address
                records.extend([item.to_map() for item in items])
                if total_count is None:
                    total_count = body.total_count or 0
                if len(records) >= total_count:
                    break
                req.page_number += 1
            except Exception as e:
                print(f"[Error] Failed to fetch EIP page {req.page_number} in {region_id}: {e}")
                break

        return records

    async def query_binded_info(
        self,
        return_raw: bool = False,
        input_df: Optional[pd.DataFrame] = None,
        max_concurrency: int = 15
    ) -> pd.DataFrame:
        """
        查询 EIP 绑定的实例信息（异步并发优化）

        注意：调用此方法前，self.instance_info 或 input_df 必须包含以下字段：
        - InstanceId: EIP 绑定的实例 ID
        - InstanceType: 绑定的实例类型（EcsInstance/SlbInstance/Nat/NetworkInterface）
        - RegionId: 实例所在的地域（从 fetch_instance_info 返回的数据中获取）

        Args:
            return_raw: 是否返回原始查询数据。默认 False（返回合并后的数据）
            input_df: 可选的输入 DataFrame。如果提供，则只处理该 DataFrame 中的数据；
                     如果为 None（默认），则处理 self.instance_info 中的所有数据
            max_concurrency: 最大并发数（默认 15，可根据 API 限流调整）

        Returns:
            pd.DataFrame:
                - return_raw=False: 返回合并绑定信息后的数据（input_df 或 self.instance_info）
                - return_raw=True: 返回原始查询的绑定信息（包含 InstanceId, target_instance_id, target_instance_name, InstanceType）
        """

        # 确定处理的数据源
        if input_df is not None:
            df = input_df.copy()
        else:
            if self.instance_info.empty:
                print("[Info] No EIP data in instance_info. Returning empty DataFrame.")
                return pd.DataFrame()
            df = self.instance_info.copy()

        # 初始化结果列表
        all_records = []

        # 按 RegionId 分组处理
        for region_id in df["RegionId"].dropna().unique():
            df_region = df[df["RegionId"] == region_id].copy()

            # ========== 1. 处理 NetworkInterface（弹性网卡）==========
            eni_df = df_region[df_region["InstanceType"] == "NetworkInterface"].copy()
            if not eni_df.empty:
                eni_ids = eni_df["InstanceId"].dropna().unique().tolist()
                eni_handler = ENIhandler(self.access_key, self.secret_key)
                await eni_handler.fetch_instance_info(
                    region_id=region_id,
                    instance_list=eni_ids,
                    auto_accumulate=True
                )
                if not eni_handler.instance_info.empty:
                    df_eni = eni_handler.instance_info[['NetworkInterfaceId', 'InstanceId', 'RegionId']].dropna(subset=['InstanceId'])
                    df_eni = df_eni[df_eni['InstanceId'].str.strip() != '']

                    if not df_eni.empty:
                        df_eni = df_eni[['NetworkInterfaceId', 'InstanceId', 'RegionId']]
                        df_eni.rename(columns={'InstanceId': 'target_instance_id','NetworkInterfaceId': 'InstanceId'}, inplace=True)
                        eni_df = pd.merge(eni_df, df_eni, how='left',on=['InstanceId', 'RegionId'])
                        all_records.append(eni_df[['AllocationId', 'InstanceId', 'InstanceType', 'RegionId', 'target_instance_id']])

            # ========== 2. 处理 EcsInstance（ECS）==========
            ecs_df = df_region[df_region["InstanceType"] == "EcsInstance"].copy()
            if not ecs_df.empty:
                ecs_df['target_instance_id'] = ecs_df['InstanceId']  # 直接使用
                all_records.append(ecs_df[['AllocationId', 'InstanceId', 'InstanceType', 'RegionId', 'target_instance_id']])

            # ========== 3. 处理 SlbInstance（负载均衡）==========
            slb_df = df_region[df_region["InstanceType"] == "SlbInstance"].copy()
            if not slb_df.empty:
                slb_df['target_instance_id'] = slb_df['InstanceId']  # 直接使用
                all_records.append(slb_df[['AllocationId', 'InstanceId', 'InstanceType', 'RegionId', 'target_instance_id']])

            # ========== 4. 处理 Nat（NAT 网关）==========
            nat_df = df_region[df_region["InstanceType"] == "Nat"].copy()
            if not nat_df.empty:
                nat_df['target_instance_id'] = nat_df['InstanceId']  # 直接使用
                all_records.append(nat_df[['AllocationId', 'InstanceId', 'InstanceType', 'RegionId', 'target_instance_id']])

        # 合并所有记录
        if not all_records:
            print("[Info] No binded instances found.")
            return pd.DataFrame()

        df_result = pd.concat(all_records, ignore_index=True)
        df_result = df_result[df_result['target_instance_id'].notna() & (df_result['target_instance_id'] != '')]
        # 按 EIP 唯一 ID 去重（防御性）：每个 EIP 在映射表中只应有一行
        df_result = df_result.drop_duplicates(subset=['AllocationId'])

        # ========== 统一查询所有 target_instance_id 的名称 ==========
        name_mapping = {}

        for region_id in df_result['RegionId'].dropna().unique():
            region_data = df_result[df_result['RegionId'] == region_id]

            # 查询 ECS 名称（包括 ENI 关联的 ECS 和直接的 ECS）
            ecs_targets = region_data[
                region_data['InstanceType'].isin(['NetworkInterface', 'EcsInstance'])
            ]['target_instance_id'].unique().tolist()
            if ecs_targets:
                ecs_handler = ECShandler(self.access_key, self.secret_key)
                ecs_result = await ecs_handler.fetch_instance_info(
                    region_id=region_id,
                    instance_list=ecs_targets,
                    auto_accumulate=False
                )
                if ecs_result:
                    df_ecs = pd.DataFrame(ecs_result)[['InstanceId', 'InstanceName']]
                    for _, row in df_ecs.iterrows():
                        name_mapping[row['InstanceId']] = row['InstanceName']

            # 查询 SLB 名称
            slb_targets = region_data[
                region_data['InstanceType'] == 'SlbInstance'
            ]['target_instance_id'].unique().tolist()
            if slb_targets:
                slb_handler = SLBhandler(self.access_key, self.secret_key)
                slb_result = await slb_handler.fetch_instance_info(
                    region_id=region_id,
                    instance_list=slb_targets,
                    auto_accumulate=False
                )
                if slb_result:
                    df_slb = pd.DataFrame(slb_result)[['LoadBalancerId', 'LoadBalancerName']]
                    for _, row in df_slb.iterrows():
                        name_mapping[row['LoadBalancerId']] = row['LoadBalancerName']

            # 查询 NGW (Nat) 名称
            nat_targets = region_data[
                region_data['InstanceType'] == 'Nat'
            ]['target_instance_id'].unique().tolist()
            if nat_targets:
                ngw_handler = NGWhandler(self.access_key, self.secret_key)
                nat_result = await ngw_handler.fetch_instance_info(
                    region_id=region_id,
                    instance_list=nat_targets,
                    auto_accumulate=False
                )
                if nat_result:
                    df_nat = pd.DataFrame(nat_result)[['NatGatewayId', 'Name']]
                    for _, row in df_nat.iterrows():
                        name_mapping[row['NatGatewayId']] = row['Name']

        # 添加名称字段
        df_result['target_instance_name'] = df_result['target_instance_id'].map(name_mapping)

        # 根据 return_raw 和 input_df 参数决定返回方式
        if not return_raw:
            # 必须以 AllocationId（EIP 唯一 ID）作为合并键：
            # 绑定实例键 (InstanceId/InstanceType/RegionId) 不是逐行唯一的——
            # 多个 EIP 绑定同一实例（如同一 NAT 网关）时，left merge 会发生笛卡尔扇出，产生重复行
            df_merge = df_result[['AllocationId', 'target_instance_id', 'target_instance_name']]
            if input_df is not None:
                # 与输入的 df 合并
                result = pd.merge(
                    input_df,
                    df_merge,
                    on='AllocationId',
                    how='left'
                )
                return result
            else:
                # 合并到 self.instance_info
                self.instance_info = pd.merge(
                    self.instance_info,
                    df_merge,
                    on='AllocationId',
                    how='left'
                )
                return self.instance_info
        return df_result

    async def get_eip_bandwidth_rank(
        self,
        time_dict: Dict[str, Any],
        direction: str = "out",
        statistics_approach: str = 'max',
        display_size: int = 99999
    ) -> pd.DataFrame:
        """
        获取 EIP 带宽排名数据

        Args:
            time_dict: 时间范围字典（start_time, end_time, start_datetime, end_datetime）
            direction: 流量方向（"out"=出带宽，"in"=入带宽）
            display_size: 返回排名数量

        Returns:
            pd.DataFrame: 包含带宽指标和排名的数据
        """
        if self.instance_info.empty:
            print("[Warning] instance_info is empty. Please fetch instance info first.")
            return pd.DataFrame()

        if direction == "out":
            metric_name = "net_tx.rate"
        elif direction == "in":
            metric_name = "net_rx.rate"
        else:
            raise ValueError("direction must be 'out' or 'in'")

        # 准备昨日时间
        previous_day_time_dict = get_time_range(
            start_datetime=time_dict['end_datetime']-datetime.timedelta(days=1,minutes=5),
            end_datetime=time_dict['end_datetime']-datetime.timedelta(days=1)
        )
        previous_metric_name = f'previous_day_{metric_name}'

        # 获取当前时间的指标数据
        df_current = self.get_metric_list(
            instance_list=self.instance_info['AllocationId'].tolist(),
            metric_name=metric_name,
            TimeDict=time_dict,
            period='60',
            field_name='Maximum',
            statistics_approach=[statistics_approach],
            group_by='instanceId',
            rename_field_name=metric_name
        )

        # 获取昨日同比的指标数据
        df_previous = self.get_metric_list(
            instance_list=self.instance_info['AllocationId'].tolist(),
            metric_name=metric_name,
            TimeDict=previous_day_time_dict,
            period='60',
            field_name='Maximum',
            statistics_approach=[statistics_approach],
            group_by='instanceId',
            rename_field_name=previous_metric_name
        )

        # 合并指标数据
        df_metrics_merged = pd.merge(df_current, df_previous, on=self.DimensionsName, how='left')
        df_metrics_merged.rename(columns={self.DimensionsName: 'AllocationId'}, inplace=True)

        # 计算指标
        self.instance_data = pd.merge(self.instance_info, df_metrics_merged, on=['AllocationId'], how='left')
        self.instance_data['query_bandwidth'] = (self.instance_data[f'{metric_name}_{statistics_approach}'] / (1000**2)).round(2)
        self.instance_data['previous_day_bandwidth'] = (self.instance_data[f'{previous_metric_name}_{statistics_approach}'] / (1000**2)).round(2)
        self.instance_data['bandwidth_delta'] = (self.instance_data['query_bandwidth'] - self.instance_data['previous_day_bandwidth']).round(2)
        self.instance_data.sort_values(by=[f'{metric_name}_{statistics_approach}'], ascending=False, inplace=True)
        self.instance_data.reset_index(drop=True, inplace=True)

        # 查询绑定实例信息
        result_df = await self.query_binded_info(return_raw=False, input_df=self.instance_data.head(display_size))
        # result_df = self.instance_data

        return result_df

    async def associate_eip_address(
        self,
        region_id: str,
        allocation_id: str,
        instance_id: str,
        instance_type: str = "EcsInstance",
        mode: Optional[str] = None,
        private_ip_address: Optional[str] = None,
        vpc_id: Optional[str] = None,
        instance_region_id: Optional[str] = None,
        client_token: Optional[str] = None,
        auto_accumulate: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        将 EIP 绑定（关联）到指定实例上

        目前仅适配 AssociateEipAddress 接口的基础/必填参数，
        其余参数（带宽、计费相关等）后续按需扩展。

        Args:
            region_id: EIP 所在的地域 ID（必填）
            allocation_id: EIP 的实例 ID，如 'eip-8vbocc9mqsi59qiill0gn'（必填）
            instance_id: 待绑定的实例 ID，可以是 NAT 网关、CLB、ECS、辅助网卡、HaVip 或 IP 地址（必填）
            instance_type: 待绑定的实例类型，默认 "EcsInstance"，可选值：
                           - EcsInstance（默认值）：VPC 类型的 ECS 实例
                           - SlbInstance：VPC 类型的 CLB 实例
                           - Nat：NAT 网关
                           - HaVip：高可用虚拟 IP
                           - NetworkInterface：辅助弹性网卡
                           - IpAddress：IP 地址
                           绑定非 ECS 实例时必须显式指定
            mode: 绑定模式，仅当 instance_type="NetworkInterface" 时必填，可选值：
                  - NAT（默认值）：以 NAT 模式和弹性网卡绑定
                  - BINDED：以网卡可见模式和弹性网卡绑定（EIP 将替换网卡的主私网 IP）
                  - MULTI_BINDED：以多 EIP 网卡可见模式和弹性网卡绑定
            private_ip_address: 弹性网卡的私网 IP，绑定 NetworkInterface 且 mode 为
                                BINDED/MULTI_BINDED 时用于指定具体的辅助私网 IP（可选）
            vpc_id: IPv4 网关所属的 VPC ID，instance_type="IpAddress" 时必填（可选）
            instance_region_id: 待绑定实例所在地域 ID，仅当 EIP 加入共享型 GA 实例时需要（可选）
            client_token: 幂等性 Token（可选），未指定时阿里云使用 RequestId 作为 Token
            auto_accumulate: 是否将绑定结果累积到 self.instance_data

        Returns:
            Optional[Dict[str, Any]]: 绑定成功返回本次操作的记录（包含 AllocationId、InstanceId、
                                      InstanceType、Mode、RegionId、RequestId 等字段），失败返回 None

        Note:
            AssociateEipAddress 接口的响应体仅包含 RequestId，因此存入 self.instance_data 的记录
            由请求参数回显 + RequestId 组成，并非 DescribeEipAddresses 那样的完整实例信息。
        """
        allowed_instance_types = [
            "EcsInstance", "SlbInstance", "Nat", "HaVip", "NetworkInterface", "IpAddress"
        ]
        if instance_type not in allowed_instance_types:
            raise ValueError(f"Invalid instance_type value: {instance_type}. Must be one of {allowed_instance_types}.")

        allowed_modes = ["NAT", "BINDED", "MULTI_BINDED"]
        if instance_type == "NetworkInterface" and not mode:
            raise ValueError("mode is required when instance_type is 'NetworkInterface'. Must be one of NAT, BINDED, MULTI_BINDED.")
        if mode is not None and mode not in allowed_modes:
            raise ValueError(f"Invalid mode value: {mode}. Must be one of {allowed_modes}.")
        if instance_type == "IpAddress" and not vpc_id:
            raise ValueError("vpc_id is required when instance_type is 'IpAddress'.")

        client = self._create_vpc_client(region_id)
        req = vpc_models.AssociateEipAddressRequest(
            region_id=region_id,
            allocation_id=allocation_id,
            instance_id=instance_id,
            instance_type=instance_type
        )

        if mode is not None:
            req.mode = mode
        if private_ip_address is not None:
            req.private_ip_address = private_ip_address
        if vpc_id is not None:
            req.vpc_id = vpc_id
        if instance_region_id is not None:
            req.instance_region_id = instance_region_id
        if client_token is not None:
            req.client_token = client_token

        try:
            resp = await client.associate_eip_address_with_options_async(req, self.runtime)
            record = resp.body.to_map()
            # 响应体仅有 RequestId，补充请求参数回显与 RegionId，便于后续追踪绑定关系
            record.update({
                "RegionId": region_id,
                "AllocationId": allocation_id,
                "InstanceId": instance_id,
                "InstanceType": instance_type,
                "Mode": mode,
                "PrivateIpAddress": private_ip_address,
                "VpcId": vpc_id
            })

            if auto_accumulate:
                df = pd.DataFrame([record])
                self.instance_data = pd.concat([self.instance_data, df], axis=0, ignore_index=True)

            return record
        except Exception as e:
            print(f"[Error] Associate EipAddress Failed in {region_id}, AllocationId: {allocation_id}, InstanceId: {instance_id}, Error: {e}")
            return None


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
                    region_id=region_id,  # 必须显式设置 region_id
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
                    region_id=region_id,  # 必须显式设置 region_id
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
            region_id=region_id,  # 必须显式设置 region_id
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
            # print(f"[Info] Extracted bandwidth info for {len(bandwidth_df)} records.")
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
                        all_records.append(eni_df[['Ipv6AddressId', 'AssociatedInstanceId', 'RegionId', 'InstanceId']])

            ecs_df = df_region[df_region["AssociatedInstanceType"] == "EcsInstance"].copy()
            if not ecs_df.empty:
                ecs_df['InstanceId'] = ecs_df['AssociatedInstanceId']  # 直接使用
                all_records.append(ecs_df[['Ipv6AddressId', 'AssociatedInstanceId', 'RegionId', 'InstanceId']])

        # 合并所有记录
        if not all_records:
            print("[Info] No binded instances found.")
            return pd.DataFrame()

        df_result = pd.concat(all_records, ignore_index=True)
        df_result = df_result[df_result['InstanceId'].notna() & (df_result['InstanceId'] != '')]
        # 按 IPv6 唯一 ID 去重（防御性）：每条 IPv6 地址在映射表中只应有一行
        df_result = df_result.drop_duplicates(subset=['Ipv6AddressId'])

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
            # 必须以 Ipv6AddressId（IPv6 唯一 ID）作为合并键：
            # AssociatedInstanceId 不是逐行唯一的——多个 IPv6 地址绑定同一实例时，
            # left merge 会发生笛卡尔扇出，产生重复行
            df_merge = df_result[['Ipv6AddressId', 'InstanceId', 'InstanceName']]
            if input_df is not None:
                # 与输入的 df 合并
                result = pd.merge(
                    input_df,
                    df_merge,
                    on='Ipv6AddressId',
                    how='left'
                )
                return result
            else:
                # 合并到 self.instance_info
                self.instance_info = pd.merge(
                    self.instance_info,
                    df_merge,
                    on='Ipv6AddressId',
                    how='left'
                )
                return self.instance_info

        return df_result