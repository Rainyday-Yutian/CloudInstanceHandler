# coding: utf-8

import asyncio
import datetime
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from cloudinstancehandler.common.funcs import get_time_range
from cloudinstancehandler.volcengine.VolcInstance import VolcInstance
from cloudinstancehandler.volcengine.ECS import ECS, ENI
from cloudinstancehandler.volcengine.CLB import CLB
from cloudinstancehandler.volcengine.NAT import NAT

import volcenginesdkcore
import volcenginesdkvpc
from volcenginesdkcore.rest import ApiException


# ======================================== EIP ======================================== #
class EIP(VolcInstance):
    """
    火山云弹性公网 IP (EIP)

    重构要点（原 volcinsdatahandler.EIP）：
    - getInsInfo → fetch_instance_info（异步，支持批量精确查询与全量分页两种模式）
    - getBindedInsInfo（半成品）→ query_binded_info（补全 NetworkInterface/EcsInstance/ClbInstance 分支）
    - InsInfo → instance_info
    """

    def __init__(self, access_key: str, secret_key: str) -> None:
        super().__init__(access_key, secret_key)
        self.namespace = "VCM_EIP"
        self.sub_namespace = "Instance"

    def _create_api_instance(self, region_id: str) -> volcenginesdkvpc.VPCApi:
        """创建 VPC API 客户端（每请求独立 ApiClient，避免并发下 set_default 全局状态竞争）"""
        configuration = self._create_configuration(region_id)
        return volcenginesdkvpc.VPCApi(volcenginesdkcore.ApiClient(configuration))

    async def fetch_instance_info(
        self,
        region_id: str,
        instance_id_list: Optional[List[str]] = None,
        eip_address_list: Optional[List[str]] = None,
        status: Optional[str] = None,
        page_size: int = 100,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取 EIP 信息（异步）

        支持两种查询模式：
        1. 指定 instance_id_list（allocation_ids）或 eip_address_list 批量精确查询（自动分批并发）
        2. 未指定时全量查询（page_number 分页）

        Args:
            region_id: 地域ID
            instance_id_list: EIP 分配ID列表（allocation_id）
            eip_address_list: EIP 地址列表
            status: EIP 状态过滤（如 Available/InUse 等）
            page_size: 每页/每批数量（最大100）
            auto_accumulate: 是否自动累积到 instance_info

        Returns:
            List[Dict[str, Any]]: 原始记录列表（已注入 region_id 字段）
        """
        try:
            if instance_id_list or eip_address_list:
                raw_records = await self._fetch_by_filter_async(
                    region_id, page_size, instance_id_list, eip_address_list, status
                )
            else:
                raw_records = await self._fetch_all_async(region_id, page_size, status)
        except Exception as e:
            print(f"[Error] Failed to fetch EIP info in {region_id}: {e}")
            raise

        # 接口返回中无 region 字段，手动注入以保证统一规范
        for record in raw_records:
            record['region_id'] = region_id

        if auto_accumulate and raw_records:
            self._accumulate_instance_info(raw_records)

        return raw_records

    async def _fetch_by_filter_async(
        self,
        region_id: str,
        page_size: int,
        instance_id_list: Optional[List[str]] = None,
        eip_address_list: Optional[List[str]] = None,
        status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """按 allocation_ids / eip_addresses 分批并发精确查询（异步）"""
        api_instance = self._create_api_instance(region_id)
        loop = asyncio.get_running_loop()

        async def _batch_fetch(batch_list: List[str], param_name: str, sem: asyncio.Semaphore) -> List[Dict[str, Any]]:
            async with sem:
                def sync_fetch():
                    request = volcenginesdkvpc.DescribeEipAddressesRequest(page_size=page_size)
                    if status:
                        request.status = status
                    setattr(request, param_name, batch_list)
                    response = api_instance.describe_eip_addresses(request)
                    return response.to_dict()

                try:
                    response_dict = await loop.run_in_executor(None, sync_fetch)
                    return response_dict.get('eip_addresses') or []
                except ApiException as e:
                    print(f"[Warn] Failed to fetch EIP batch in {region_id}: {e}")
                    return []

        semaphore = asyncio.Semaphore(10)
        all_tasks = []
        if instance_id_list:
            batches = [instance_id_list[i:i + page_size] for i in range(0, len(instance_id_list), page_size)]
            all_tasks.extend(_batch_fetch(batch, 'allocation_ids', semaphore) for batch in batches)
        if eip_address_list:
            batches = [eip_address_list[i:i + page_size] for i in range(0, len(eip_address_list), page_size)]
            all_tasks.extend(_batch_fetch(batch, 'eip_addresses', semaphore) for batch in batches)

        if not all_tasks:
            return []

        results = await asyncio.gather(*all_tasks, return_exceptions=True)

        records: List[Dict[str, Any]] = []
        for res in results:
            if isinstance(res, list):
                records.extend(res)
            elif isinstance(res, Exception):
                print(f"[Error] Unexpected task error: {res}")
        return records

    async def _fetch_all_async(
        self,
        region_id: str,
        page_size: int,
        status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """全量查询 EIP（page_number 分页，异步）"""
        api_instance = self._create_api_instance(region_id)
        loop = asyncio.get_running_loop()

        request = volcenginesdkvpc.DescribeEipAddressesRequest(page_size=page_size)
        if status:
            request.status = status

        raw_records: List[Dict[str, Any]] = []
        page_number = 1
        has_next_page = True

        while has_next_page:
            request.page_number = page_number
            try:
                def sync_fetch():
                    response = api_instance.describe_eip_addresses(request)
                    return response.to_dict()

                response_dict = await loop.run_in_executor(None, sync_fetch)
                eip_addresses = response_dict.get('eip_addresses') or []
                raw_records.extend(eip_addresses)
                total_count = response_dict.get('total_count') or 0
                if total_count <= page_number * page_size:
                    has_next_page = False
                else:
                    page_number += 1
            except ApiException as e:
                print(f"[Error] Failed to fetch EIP page {page_number} in {region_id}: {e}")
                break
            except Exception as e:
                print(f"[Error] Unexpected error when fetching EIP page {page_number} in {region_id}: {e}")
                break

        return raw_records

    async def fetch_instance_detail(self, region_id: str, allocation_id: str) -> Dict[str, Any]:
        """
        获取单个 EIP 详细信息（DescribeEipAddressAttributes）

        Raises:
            NotImplementedError: 接口尚未适配
        """
        raise NotImplementedError("DescribeEipAddressAttributes 待适配")

    async def query_binded_info(
        self,
        return_raw: bool = False,
        input_df: Optional[pd.DataFrame] = None,
        max_concurrency: int = 15
    ) -> pd.DataFrame:
        """
        查询 EIP 绑定的实例信息（异步并发，补全原半成品 getBindedInsInfo）

        注意：调用前 self.instance_info 或 input_df 必须包含以下字段：
        - instance_id: EIP 绑定的实例ID
        - instance_type: 绑定实例类型（EcsInstance/NetworkInterface/ClbInstance/Nat）
        - region_id: 实例所在地域

        已支持分支：
        - NetworkInterface: 先查 ENI 获取挂载的 ECS，再查 ECS 名称
        - EcsInstance: 直接查询 ECS 名称
        - ClbInstance: 直接查询 CLB 名称
        - Nat: 直接查询 NAT 网关名称

        Args:
            return_raw: 是否返回原始查询数据。默认 False（返回合并后的数据）
            input_df: 可选的输入 DataFrame。如果提供，则只处理该 DataFrame 中的数据；
                     如果为 None（默认），则处理 self.instance_info 中的所有数据
            max_concurrency: 最大并发数（默认 15，可根据 API 限流调整；与 aliyun 签名对齐，当前实现为串行分批）

        Returns:
            pd.DataFrame:
                - return_raw=False: 返回合并绑定信息后的数据（input_df 或 self.instance_info）
                - return_raw=True: 返回原始查询的绑定信息（包含 instance_id, target_instance_id, target_instance_name, instance_type）
        """
        if input_df is not None:
            df = input_df.copy()
        else:
            if self.instance_info.empty:
                print("[Info] No EIP data in instance_info. Returning empty DataFrame.")
                return pd.DataFrame()
            df = self.instance_info.copy()

        required_columns = ['region_id', 'instance_type', 'instance_id']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"[Info] EIP data is missing binding fields {missing_columns}, skipping binding info query.")
            return pd.DataFrame() if return_raw else df

        all_records = []

        for region_id in df['region_id'].dropna().unique():
            df_region = df[df['region_id'] == region_id].copy()

            # ========== 1. NetworkInterface（弹性网卡）==========
            eni_df = df_region[df_region['instance_type'] == 'NetworkInterface'].copy()
            if not eni_df.empty:
                eni_ids = eni_df['instance_id'].dropna().unique().tolist()
                eni_handler = ENI(self.access_key, self.secret_key)
                eni_records = await eni_handler.fetch_instance_info(
                    region_id=region_id,
                    instance_id_list=eni_ids,
                    auto_accumulate=False
                )
                if eni_records:
                    df_eni = pd.DataFrame(eni_records)[['network_interface_id', 'instance_id']].dropna(subset=['instance_id'])
                    df_eni = df_eni[df_eni['instance_id'].astype(str).str.strip() != '']
                    if not df_eni.empty:
                        df_eni = df_eni.rename(columns={
                            'network_interface_id': 'instance_id',   # EIP 绑定的 ENI ID
                            'instance_id': 'target_instance_id'      # ENI 挂载的 ECS ID
                        })
                        eni_df = pd.merge(
                            eni_df[['allocation_id', 'instance_id', 'instance_type', 'region_id']],
                            df_eni,
                            on='instance_id',
                            how='left'
                        )
                        all_records.append(eni_df[['allocation_id', 'instance_id', 'instance_type', 'region_id', 'target_instance_id']])

            # ========== 2. EcsInstance ==========
            ecs_df = df_region[df_region['instance_type'] == 'EcsInstance'].copy()
            if not ecs_df.empty:
                ecs_df['target_instance_id'] = ecs_df['instance_id']
                all_records.append(ecs_df[['allocation_id', 'instance_id', 'instance_type', 'region_id', 'target_instance_id']])

            # ========== 3. ClbInstance ==========
            clb_df = df_region[df_region['instance_type'] == 'ClbInstance'].copy()
            if not clb_df.empty:
                clb_df['target_instance_id'] = clb_df['instance_id']
                all_records.append(clb_df[['allocation_id', 'instance_id', 'instance_type', 'region_id', 'target_instance_id']])

            # ========== 4. Nat（NAT 网关）==========
            nat_df = df_region[df_region['instance_type'] == 'Nat'].copy()
            if not nat_df.empty:
                nat_df['target_instance_id'] = nat_df['instance_id']
                all_records.append(nat_df[['allocation_id', 'instance_id', 'instance_type', 'region_id', 'target_instance_id']])

        # 合并所有记录
        if not all_records:
            print("[Info] No binded instances found.")
            return pd.DataFrame()

        df_result = pd.concat(all_records, ignore_index=True)
        df_result = df_result[df_result['target_instance_id'].notna() & (df_result['target_instance_id'] != '')]
        # 按 EIP 唯一 ID 去重（防御性）：每个 EIP 在映射表中只应有一行
        df_result = df_result.drop_duplicates(subset=['allocation_id'])

        # ========== 统一查询 target_instance_id 的名称 ==========
        name_mapping: Dict[str, str] = {}

        for region_id in df_result['region_id'].dropna().unique():
            region_data = df_result[df_result['region_id'] == region_id]

            # ECS 名称（含 ENI 关联的 ECS 与直接绑定的 ECS）
            ecs_targets = region_data[
                region_data['instance_type'].isin(['NetworkInterface', 'EcsInstance'])
            ]['target_instance_id'].unique().tolist()
            if ecs_targets:
                ecs_handler = ECS(self.access_key, self.secret_key)
                ecs_records = await ecs_handler.fetch_instance_info(
                    region_id=region_id,
                    instance_id_list=ecs_targets,
                    instance_charge_type="all",  # 名称回查不按计费类型过滤，避免漏查 PostPaid 实例
                    auto_accumulate=False
                )
                for item in ecs_records or []:
                    if item.get('instance_id'):
                        name_mapping[item['instance_id']] = item.get('instance_name', '')

            # CLB 名称
            clb_targets = region_data[
                region_data['instance_type'] == 'ClbInstance'
            ]['target_instance_id'].unique().tolist()
            if clb_targets:
                clb_handler = CLB(self.access_key, self.secret_key)
                clb_records = await clb_handler.fetch_instance_info(
                    region_id=region_id,
                    instance_id_list=clb_targets,
                    auto_accumulate=False
                )
                for item in clb_records or []:
                    if item.get('load_balancer_id'):
                        name_mapping[item['load_balancer_id']] = item.get('load_balancer_name', '')

            # NAT 网关名称
            nat_targets = region_data[
                region_data['instance_type'] == 'Nat'
            ]['target_instance_id'].unique().tolist()
            if nat_targets:
                nat_handler = NAT(self.access_key, self.secret_key)
                nat_records = await nat_handler.fetch_instance_info(
                    region_id=region_id,
                    instance_id_list=nat_targets,
                    auto_accumulate=False
                )
                for item in nat_records or []:
                    if item.get('nat_gateway_id'):
                        name_mapping[item['nat_gateway_id']] = item.get('nat_gateway_name', '')

        # 添加名称字段
        df_result['target_instance_name'] = df_result['target_instance_id'].map(name_mapping)

        # 根据 return_raw 和 input_df 参数决定返回方式（对齐 aliyun/hwyun）
        if not return_raw:
            # 必须以 allocation_id（EIP 唯一 ID）作为合并键：
            # 绑定实例键 (instance_id/instance_type/region_id) 不是逐行唯一的——
            # 多个 EIP 绑定同一实例（如同一 NAT 网关）时，left merge 会发生笛卡尔扇出，产生重复行
            df_merge = df_result[['allocation_id', 'target_instance_id', 'target_instance_name']]
            if input_df is not None:
                # 与输入的 df 合并
                result = pd.merge(
                    input_df,
                    df_merge,
                    on='allocation_id',
                    how='left'
                )
                return result
            else:
                # 合并到 self.instance_info
                self.instance_info = pd.merge(
                    self.instance_info,
                    df_merge,
                    on='allocation_id',
                    how='left'
                )
                return self.instance_info
        return df_result


# ======================================== BandwidthPackage ======================================== #
class BandwidthPackage(VolcInstance):
    """
    火山云共享带宽包 (BandwidthPackage)

    重构要点（原 volcinsdatahandler.BandwidthPackage）：
    - getInsInfo → fetch_instance_info（异步）
    - extractEIPFromInsInfo → extract_eip_from_instance_info（累积属性 EIPsFromAllCBWP → eips_from_all_cbwp）
    - extractEIPFromSingleCBWP → extract_eip_from_single_cbwp
    - getEIPBandwidthRank → get_eip_bandwidth_rank（完整重写为异步实现，
      设计范式与 aliyun/VPC.py::EIP、hwyun/VPC.py::GEIP 的同名方法对齐；
      并修复老实现中 df_eips 泄漏自最后一次循环迭代、datetime 未导入两个 bug）
    """

    def __init__(self, access_key: str, secret_key: str) -> None:
        super().__init__(access_key, secret_key)
        self.namespace = "VCM_BandwidthPackage"
        self.sub_namespace = "instance"
        self.eips_from_all_cbwp: pd.DataFrame = pd.DataFrame()

    def _create_api_instance(self, region_id: str) -> volcenginesdkvpc.VPCApi:
        """创建 VPC API 客户端（每请求独立 ApiClient，避免并发下 set_default 全局状态竞争）"""
        configuration = self._create_configuration(region_id)
        return volcenginesdkvpc.VPCApi(volcenginesdkcore.ApiClient(configuration))

    async def fetch_instance_info(
        self,
        region_id: str,
        instance_id_list: Optional[List[str]] = None,
        page_size: int = 100,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取共享带宽包信息（异步，page_number 分页）

        Args:
            region_id: 地域ID
            instance_id_list: 带宽包ID列表（精确查询）
            page_size: 每页返回数量（最大100）
            auto_accumulate: 是否自动累积到 instance_info

        Returns:
            List[Dict[str, Any]]: 原始记录列表（已注入 region_id 字段）
        """
        api_instance = self._create_api_instance(region_id)
        loop = asyncio.get_running_loop()

        describe_bandwidth_packages_request = volcenginesdkvpc.DescribeBandwidthPackagesRequest(
            page_size=page_size
        )
        if instance_id_list:
            describe_bandwidth_packages_request.bandwidth_package_ids = instance_id_list

        raw_records: List[Dict[str, Any]] = []
        page_number = 1
        has_next_page = True

        while has_next_page:
            describe_bandwidth_packages_request.page_number = page_number
            try:
                def sync_fetch():
                    response = api_instance.describe_bandwidth_packages(describe_bandwidth_packages_request)
                    return response.to_dict()

                response_dict = await loop.run_in_executor(None, sync_fetch)
                bandwidth_packages = response_dict.get('bandwidth_packages') or []
                raw_records.extend(bandwidth_packages)
                total_count = response_dict.get('total_count') or 0
                if total_count <= page_number * page_size:
                    has_next_page = False
                else:
                    page_number += 1
            except ApiException as e:
                print(f"Exception when fetching BandwidthPackage in {region_id}: {e}\n")
                break
            except Exception as e:
                print(f"Unexpected error when fetching BandwidthPackage in {region_id}: {e}\n")
                break

        # 接口返回中无 region 字段，手动注入以保证统一规范
        for record in raw_records:
            record['region_id'] = region_id

        if auto_accumulate and raw_records:
            self._accumulate_instance_info(raw_records)

        return raw_records

    def extract_eip_from_instance_info(self) -> pd.DataFrame:
        """
        从 instance_info 中提取所有带宽包绑定的 EIP 信息（降维展开 eip_addresses）

        结果同时存入 self.eips_from_all_cbwp。

        Returns:
            pd.DataFrame: EIP 信息表（含 bandwidth_package_id/region_id/bandwidth(Mbps) 列）
        """
        if self.instance_info.empty:
            print("[Info] No data in instance_info, please call fetch_instance_info first!")
            self.eips_from_all_cbwp = pd.DataFrame()
            return self.eips_from_all_cbwp

        all_dfs = []
        for _, row in self.instance_info.iterrows():
            eip_addresses = row.get('eip_addresses')
            if not isinstance(eip_addresses, list) or not eip_addresses:
                continue
            df_tmp = pd.DataFrame(eip_addresses)
            df_tmp['bandwidth_package_id'] = row.get('bandwidth_package_id')
            df_tmp['region_id'] = row.get('region_id')
            df_tmp['bandwidth(Mbps)'] = row.get('bandwidth')
            all_dfs.append(df_tmp)

        df_data = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
        self.eips_from_all_cbwp = df_data
        return df_data

    def extract_eip_from_single_cbwp(self, eips_address: List[Dict[str, Any]]) -> pd.DataFrame:
        """从单个带宽包的 eip_addresses 列表中提取 EIP 信息表"""
        return pd.DataFrame(eips_address)

    async def get_eip_bandwidth_rank(
        self,
        time_dict: Dict[str, Any],
        direction: str = "out",
        statistics_approach: str = "max",
        display_size: int = 99999,
        period: str = "1m"
    ) -> pd.DataFrame:
        """
        获取带宽包下 EIP 的带宽排名（异步并发，含昨日同比）

        设计范式与 aliyun/VPC.py::EIP.get_eip_bandwidth_rank、
        hwyun/VPC.py::GEIP.get_eip_bandwidth_rank 对齐：
        - 参数命名统一为 direction / statistics_approach / display_size（默认99999表示返回全部）
        - 昨日同比指标列名统一为 previous_day_{metric_name}
        - 派生列统一为 query_bandwidth / previous_day_bandwidth / bandwidth_delta（单位 Mbps）
        - 按原始指标列 {metric_name}_{statistics_approach} 降序排序
        - 排名后统一调用 query_binded_info 补充绑定实例信息
        - 完整排名结果存入 self.instance_data

        火山云差异适配：带宽包展开的 EIP 记录仅含 allocation_id/eip_address，
        排名前先按地域回查 EIP 详情并合并全量字段（name/status/isp/instance_id/
        instance_type 等），与 aliyun/hwyun 的 instance_info 原生自带完整字段对齐，
        排名结果保全所有实例信息，不做字段裁剪。

        重写说明（替代老 getEIPBandwidthRank）：
        - 基于新基类 VolcInstance.get_metric_data 的实际签名（time_dict/display_metric_name/groupby）
        - 修复老实现中 df_eips 泄漏自最后一次循环迭代的 bug（改为一次性构建全量 EIP 表）
        - 修复老实现缺失 datetime 导入的 bug
        - 按地域分组并发拉取当前与昨日同比指标

        Args:
            time_dict: 时间范围字典（使用 get_time_range 生成）
            direction: 流量方向，"out"（OutBPS 上行）或 "in"（InBPS 下行）
            statistics_approach: 二次统计方法（默认 "max"）
            display_size: 显示数量限制（默认99999，表示显示所有）
            period: 指标统计周期（默认 "1m"）

        Returns:
            pd.DataFrame: 排名数据（保全 EIP 详情全量字段 + 指标列 +
                          query_bandwidth/previous_day_bandwidth/bandwidth_delta +
                          绑定实例信息 target_instance_id/target_instance_name）
        """
        if self.instance_info.empty:
            print("[Warning] instance_info is empty. Please fetch instance info first.")
            return pd.DataFrame()

        if direction == "out":
            metric_name = "OutBPS"
        elif direction == "in":
            metric_name = "InBPS"
        else:
            raise ValueError("direction must be 'out' or 'in'")

        # 准备昨日时间（对齐 aliyun/hwyun：end 前移1天，窗口5分钟）
        previous_day_time_dict = get_time_range(
            start_datetime=time_dict['end_datetime'] - datetime.timedelta(days=1, minutes=5),
            end_datetime=time_dict['end_datetime'] - datetime.timedelta(days=1),
            print_time_range=False
        )
        previous_metric_name = f'previous_day_{metric_name}'

        # 一次性构建全量 EIP 表（修复老实现 df_eips 泄漏 bug）
        df_eips = self.extract_eip_from_instance_info()
        if df_eips.empty:
            print("[Warning] No EIP found in bandwidth packages.")
            return pd.DataFrame()

        # 火山云差异适配：带宽包展开的 EIP 记录仅含 allocation_id/eip_address，
        # 先按地域回查全量 EIP 详情并合并所有字段补齐
        # （对齐 aliyun/hwyun 的 instance_info 原生自带完整字段，排名结果保全实例信息，
        #  同时补上 query_binded_info 所需的 instance_id/instance_type 绑定字段）
        eip_handler = EIP(self.access_key, self.secret_key)
        for region in df_eips['region_id'].dropna().unique():
            ids = df_eips[df_eips['region_id'] == region]['allocation_id'].dropna().unique().tolist()
            if ids:
                await eip_handler.fetch_instance_info(
                    region_id=region,
                    instance_id_list=ids,
                    auto_accumulate=True
                )
        if not eip_handler.instance_info.empty:
            df_detail = eip_handler.instance_info.copy()
            merge_keys = ['allocation_id', 'region_id']
            # 排名表中已有的同名重复字段（eip_address/bandwidth_package_id，取值相同）不再合并，避免 _x/_y 后缀
            dup_cols = [c for c in df_detail.columns if c in df_eips.columns and c not in merge_keys]
            df_detail = df_detail.drop(columns=dup_cols)
            df_eips = pd.merge(df_eips, df_detail, on=merge_keys, how='left')

        semaphore = asyncio.Semaphore(10)

        async def _fetch_metric(region: str, ids: List[str], metric_time_dict: Dict[str, Any], display_name: str) -> pd.DataFrame:
            async with semaphore:
                # 每次查询使用独立 handler，避免并发写同一实例的 metric_data
                eip_handler = EIP(self.access_key, self.secret_key)
                return await eip_handler.get_metric_data(
                    instance_id_list=ids,
                    metric_name=metric_name,
                    time_dict=metric_time_dict,
                    period=period,
                    groupby="ResourceID",
                    statistics_approach=[statistics_approach],
                    display_metric_name=display_name,
                    region_id=region
                )

        # 按地域并发拉取当前与昨日同比指标
        tasks = []
        task_meta = []
        for region in df_eips['region_id'].dropna().unique():
            ids = df_eips[df_eips['region_id'] == region]['allocation_id'].dropna().unique().tolist()
            if not ids:
                continue
            tasks.append(_fetch_metric(region, ids, time_dict, metric_name))
            task_meta.append('current')
            tasks.append(_fetch_metric(region, ids, previous_day_time_dict, previous_metric_name))
            task_meta.append('previous')

        if not tasks:
            print("[Warning] No valid EIP ids to query metrics.")
            return pd.DataFrame()

        results = await asyncio.gather(*tasks, return_exceptions=True)

        current_frames = []
        previous_frames = []
        for meta, res in zip(task_meta, results):
            if isinstance(res, pd.DataFrame):
                if not res.empty:
                    (current_frames if meta == 'current' else previous_frames).append(res)
            elif isinstance(res, Exception):
                print(f"[Error] Metric task failed: {res}")

        df_current = pd.concat(current_frames, ignore_index=True) if current_frames else pd.DataFrame()
        df_previous = pd.concat(previous_frames, ignore_index=True) if previous_frames else pd.DataFrame()
        if df_current.empty:
            print("[Warning] No current metric data returned.")
            return pd.DataFrame()

        # 合并指标数据（对齐 aliyun/hwyun 范式）
        if not df_previous.empty:
            df_metrics_merged = pd.merge(df_current, df_previous, on='ResourceID', how='left')
        else:
            df_metrics_merged = df_current
        df_metrics_merged.rename(columns={'ResourceID': 'allocation_id'}, inplace=True)

        # 计算指标
        self.instance_data = pd.merge(df_eips, df_metrics_merged, on=['allocation_id'], how='left')
        self.instance_data['query_bandwidth'] = (self.instance_data[f'{metric_name}_{statistics_approach}'] / (1000**2)).round(2)
        previous_col = f'{previous_metric_name}_{statistics_approach}'
        if previous_col in self.instance_data.columns:
            self.instance_data['previous_day_bandwidth'] = (self.instance_data[previous_col] / (1000**2)).round(2)
        else:
            self.instance_data['previous_day_bandwidth'] = float('nan')
        self.instance_data['bandwidth_delta'] = (self.instance_data['query_bandwidth'] - self.instance_data['previous_day_bandwidth']).round(2)
        self.instance_data.sort_values(by=[f'{metric_name}_{statistics_approach}'], ascending=False, inplace=True)
        self.instance_data.reset_index(drop=True, inplace=True)

        # 查询绑定实例信息（对齐 aliyun/hwyun：排名截取后统一补充绑定信息）
        result_df = await eip_handler.query_binded_info(return_raw=False, input_df=self.instance_data.head(display_size))
        return result_df
