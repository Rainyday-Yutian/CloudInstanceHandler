# coding: utf-8
import pandas as pd
import numpy as np
import asyncio
import aiohttp
from typing import List, Optional, Dict, Any

from cloudinstancehandler.common.funcs import *
from cloudinstancehandler.hwyun.HwyunInstance import HwyunInstance

from huaweicloudsdkcore.auth.credentials import GlobalCredentials, BasicCredentials
from huaweicloudsdkcore.exceptions import exceptions
from huaweicloudsdkvpc.v3.region.vpc_region import VpcRegion
import huaweicloudsdkvpc.v2 as hwvpcv2
from huaweicloudsdkvpc.v3 import *
from huaweicloudsdkelb.v3.region.elb_region import ElbRegion
from huaweicloudsdkelb.v3 import *
from huaweicloudsdkgeip.v3.region.geip_region import GeipRegion
from huaweicloudsdkgeip.v3 import *

from cloudinstancehandler.hwyun.ECS import ECS
from cloudinstancehandler.hwyun.ELB import ELB

from pprint import pprint

# ======================================== GEIP (Global EIP) ======================================== #
class GEIP(HwyunInstance):
    def __init__(self, access_key, secret_key) -> None:
        super().__init__(access_key, secret_key)
        self.namespace = "SYS.GEIP"
        self.DimensionsName = "geip_global_eip_id"

    def fetch_instance_info(
        self,
        id_list: Optional[List[str]] = None,
        name: Optional[str] = None,
        name_like: Optional[str] = None,
        status: Optional[str] = None,
        ip_address: Optional[str] = None,
        ipv6_address: Optional[str] = None,
        isp: Optional[str] = None,
        bandwidth_id_list: Optional[List[str]] = None,
        page_size: int = 2000,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        华为云API文档：https://console.huaweicloud.com/apiexplorer/#/openapi/GEIP/doc?api=ListGlobalEips
        Args:
            id_list: EIP ID列表（精确匹配）
            name: EIP名称（精确匹配）
            name_like: EIP名称模糊查询
            status: EIP状态（idle/inuse/pending_create/pending_update等）
            ip_address: IPv4地址（精确匹配）
            ipv6_address: IPv6地址（精确匹配）
            isp: 运营商（5_telcom/5_union/5_bgp等）
            bandwidth_id_list: 带宽包ID列表（过滤绑定的EIP）
            page_size: 每页数量（范围1-2000，默认2000，最大值2000）
            auto_accumulate: 是否自动累加到 self.instance_info

        Returns:
            List[Dict]: 原始记录列表（完整数据，已处理分页）

        API参数说明：
            - limit: 每页数量（1-2000）
            - offset: 起始偏移量（>=0）
            - marker: 分页标记（上一页最后一条记录的ID）
            - id: 精确匹配的EIP ID（多个ID用逗号分隔）
            - name: 精确匹配的EIP名称
            - name_like: 名称模糊匹配
            - status: 状态过滤
            - ip_address: IPv4地址过滤
            - ipv6_address: IPv6地址过滤
            - isp: 运营商过滤
            - internet_bandwidth_id: 带宽包ID
        """
        # 验证 page_size 范围
        if not 1 <= page_size <= 2000:
            print(f"[WARNING] page_size must be between 1 and 2000, default to {page_size}")
            page_size = 2000

        credentials = GlobalCredentials(self.access_key, self.secret_key)
        client = GeipClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(GeipRegion.value_of("cn-north-4")) \
            .build()

        raw_records = []
        offset = 0
        marker = None
        has_more = True

        try:
            while has_more:
                request = ListGlobalEipsRequest()

                # 设置查询参数
                if id_list:
                    request.id = id_list
                if name:
                    request.name = name
                if name_like:
                    request.name_like = name_like
                if status:
                    request.status = status
                if ip_address:
                    request.ip_address = ip_address
                if ipv6_address:
                    request.ipv6_address = ipv6_address
                if isp:
                    request.isp = isp
                if bandwidth_id_list:
                    request.internet_bandwidth_id = bandwidth_id_list

                request.limit = page_size
                request.offset = offset

                # 如果有 marker，使用 marker 分页（优先级高于 offset）
                if marker:
                    request.marker = marker

                response_json = client.list_global_eips(request).to_json_object()
                current_page = response_json.get('global_eips', [])

                if not current_page:
                    break

                raw_records.extend(current_page)

                # 判断是否还有下一页
                total_count = response_json.get('total_count', 0)
                current_page_count = len(current_page)

                if total_count and (offset + current_page_count) >= total_count:
                    has_more = False
                else:
                    offset += current_page_count
                    # 如果API返回了marker，使用marker分页
                    if 'marker' in response_json:
                        marker = response_json['marker']
                        has_more = marker is not None
                    elif current_page_count < page_size:
                        # 当前页数据不足一页，说明已到最后一页
                        has_more = False

        except exceptions.ClientRequestException as e:
            print(f"[Error] Failed to fetch GEIP info: Request id: {e.request_id}\n"
                  f"Status code: {e.status_code}, error code: {e.error_code}\n"
                  f"Error message: {e.error_msg}")

        if auto_accumulate and raw_records:
            self._accumulate_instance_info(raw_records)

        return raw_records

    def extract_associate_instance_info(self) -> None:
        """
        展平 associate_instance_info 并更新 self.instance_info
        将嵌套的 associate_instance_info 字段展开为平面字段，并统一 IP 地址处理

        处理逻辑：
        1. 展平 associate_instance_info 字典为平面字段（添加 associate_ 前缀）
        2. 将原有的 ip_address (IPv4) 备份到 ipv4_address
        3. 根据 associate_instance_type 统一 IP 地址到 ip_address：
           - IPV6-PORT: 使用 ipv6_address
           - 其他类型: 使用原 ip_address (IPv4)

        注意：此方法直接修改 self.instance_info
        """
        if self.instance_info.empty:
            print("[Info] instance_info is empty. Nothing to extract.")
            return

        # 展平 associate_instance_info
        if "associate_instance_info" in self.instance_info.columns:
            normalized_df = pd.json_normalize(self.instance_info['associate_instance_info'])
            prefix = "associate_"
            normalized_df.columns = [f"{prefix}{col}" for col in normalized_df.columns]
            self.instance_info = pd.concat([self.instance_info, normalized_df], axis=1)

        # 将 ip_address(仅ipv4地址）备份至新的 ipv4_address 字段，然后 ipv4、ipv6地址 统一至 ip_address
        if "ipv4_address" not in self.instance_info.columns and "ip_address" in self.instance_info.columns:
            self.instance_info["ipv4_address"] = self.instance_info["ip_address"]
        if "associate_instance_type" in self.instance_info.columns and "ipv6_address" in self.instance_info.columns:
            self.instance_info["ip_address"] = np.where(
                self.instance_info["associate_instance_type"] == "IPV6-PORT",
                self.instance_info["ipv6_address"],
                self.instance_info["ip_address"]
            )

    def query_binded_info(self, return_raw: bool = False, input_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        查询绑定实例信息（已优化并发）

        注意：调用此方法前，必须先调用 extract_associate_instance_info() 展平数据，
        或确保 input_df 已包含展平后的字段（associate_region, associate_instance_type, associate_instance_id）

        Args:
            return_raw: 是否返回原始查询数据。默认False（返回合并后的数据）
            input_df: 可选的输入 DataFrame。如果提供，则只处理该 DataFrame 中的数据；
                     如果为 None（默认），则处理 self.instance_info 中的所有数据

        Returns:
            pd.DataFrame:
                - return_raw=False: 返回合并绑定信息后的数据（input_df 或 self.instance_info）
                - return_raw=True: 返回原始查询的绑定信息
        """
        # 确定处理的数据源
        if input_df is not None:
            df = input_df
        else:
            if self.instance_info.empty:
                print("[Info] No GEIP data in instance_info. Returning empty DataFrame.")
                return pd.DataFrame()
            df = self.instance_info.copy()

        # 检查必要字段是否存在
        required_columns = ['associate_region', 'associate_instance_type', 'associate_instance_id']
        missing_columns = [col for col in required_columns if col not in df.columns]

        # 如果缺少必要字段，说明没有绑定实例信息
        if missing_columns:
            print(f"[Info] GEIP data is missing binding fields {missing_columns}, skipping binding info query.")
            if return_raw:
                return pd.DataFrame()
            # return_raw=False 时，直接返回原始数据（不添加绑定信息字段）
            return df

        # 初始化结果列表
        all_records = []

        for region_id in df["associate_region"].dropna().unique():
            df_region = df[df["associate_region"] == region_id].copy()

            port_df = df_region[df_region["associate_instance_type"].isin(["PORT", "IPV6-PORT"])].copy()
            if not port_df.empty:
                port_ids = port_df["associate_instance_id"].dropna().unique().tolist()
                port_handler = PORT(self.access_key, self.secret_key)
                df_port_result = port_handler.fetch_instance_info(
                    region_id=region_id,
                    instance_id_list=port_ids,
                    max_concurrency=15,
                    auto_accumulate=False
                )
                if df_port_result:
                    df_port_result = pd.DataFrame(df_port_result)[['id', 'device_id']]
                    df_port_result.columns = ['associate_instance_id', 'target_instance_id']  # device_id -> target_instance_id
                    port_df = pd.merge(
                        port_df[['id', 'associate_instance_id', 'associate_instance_type', 'associate_region']],
                        df_port_result,
                        on='associate_instance_id',
                        how='left'
                    )
                    all_records.append(port_df[['id', 'associate_instance_id', 'target_instance_id', 'associate_instance_type', 'associate_region']])

            ecs_df = df_region[df_region["associate_instance_type"] == "ECS"].copy()
            if not ecs_df.empty:
                ecs_df['target_instance_id'] = ecs_df['associate_instance_id']
                all_records.append(ecs_df[['id', 'associate_instance_id', 'target_instance_id', 'associate_instance_type', 'associate_region']])

            # elb_df = df_region[df_region["associate_instance_type"] == "ELB"].copy()
            elb_df = df_region[df_region["associate_instance_type"].isin(["ELB", "IPV6-ELB"])].copy()
            if not elb_df.empty:
                elb_df['target_instance_id'] = elb_df['associate_instance_id']
                all_records.append(elb_df[['id', 'associate_instance_id', 'target_instance_id', 'associate_instance_type', 'associate_region']])

        # 合并所有记录
        if not all_records:
            print("[Info] No binded instances found.")
            return pd.DataFrame()

        df_result = pd.concat(all_records, ignore_index=True)
        df_result = df_result[df_result['target_instance_id'].notna() & (df_result['target_instance_id'] != '')]
        # 按 GEIP 唯一 ID 去重（防御性）：每个 GEIP 在映射表中只应有一行
        df_result = df_result.drop_duplicates(subset=['id'])

        # ==================== 按地域分组查询名称 ====================
        # 初始化名称映射
        name_mapping = {}

        for region_id in df_result['associate_region'].unique():
            region_result = df_result[df_result['associate_region'] == region_id]

            # 查询 ECS 名称（包括Port关联的ECS和直接的ECS）
            ecs_targets = region_result[
                region_result['associate_instance_type'].isin(['PORT', 'IPV6-PORT', 'ECS'])
            ]['target_instance_id'].unique().tolist()
            if ecs_targets:
                ecs_handler = ECS(self.access_key, self.secret_key)
                ecs_info = ecs_handler.query_ins_info(
                    region_id=region_id,
                    instance_id_list=ecs_targets,
                    auto_accumulate=False
                )
                if ecs_info:
                    for item in ecs_info:
                        name_mapping[item['id']] = item.get('name', '')

            # 查询 ELB 名称
            elb_targets = region_result[
                region_result['associate_instance_type'] == 'ELB'
            ]['target_instance_id'].unique().tolist()
            if elb_targets:
                elb_handler = ELB(self.access_key, self.secret_key)
                elb_info = elb_handler.fetch_instance_info(
                    region_id=region_id,
                    instance_id_list=elb_targets,
                    auto_accumulate=False
                )
                if elb_info:
                    for item in elb_info:
                        name_mapping[item['id']] = item.get('name', '')

        # 添加名称字段
        df_result['target_instance_name'] = df_result['target_instance_id'].map(name_mapping)
        
        # 根据 return_raw 和 input_df 参数决定返回方式
        if not return_raw:
            # 必须以 id（GEIP 唯一 ID）作为合并键：
            # 绑定实例键 (associate_instance_id/associate_instance_type/associate_region) 不是逐行唯一的——
            # 多个 GEIP 绑定同一实例（如同一 ECS/ELB）时，left merge 会发生笛卡尔扇出，产生重复行
            df_merge = df_result[['id', 'target_instance_id', 'target_instance_name']]
            if input_df is not None:
                # 与输入的 df 合并
                result = pd.merge(
                    input_df,
                    df_merge,
                    on='id',
                    how='left'
                )
                return result
            else:
                # 合并到 self.instance_info
                self.instance_info = pd.merge(
                    self.instance_info,
                    df_merge,
                    on='id',
                    how='left'
                )
                return self.instance_info
        return df_result

    def get_eip_bandwidth_rank(
        self,
        time_dict: Dict[str, Any],
        direction: str = "out",
        statistics_approach: str = 'max',
        display_size: int = 99999
    ) -> pd.DataFrame:
        """
        获取带宽排名数据

        Args:
            time_dict: 时间范围字典（参考 get_time_range）
            direction: 流量方向 "out"（上行）或 "in"（下行）
            display_size: 显示数量限制（默认99999，表示显示所有）

        Returns:
            pd.DataFrame: 排名数据
        """
        # print(self.instance_info)
        if self.instance_info.empty:
            print("[Warning] instance_info is empty. Please fetch instance info first.")
            return pd.DataFrame()

        if direction == "out":
            metric_name = "upstream_bandwidth"
        elif direction == "in":
            metric_name = "downstream_bandwidth"
        else:
            raise ValueError("direction must be 'out' or 'in'")

        self.extract_associate_instance_info()

        # 准备昨日时间
        previous_day_time_dict = get_time_range(
            start_datetime=time_dict['end_datetime']-datetime.timedelta(days=1,minutes=5),
            end_datetime=time_dict['end_datetime']-datetime.timedelta(days=1)
        )
        display_metric_name = f'previous_day_{metric_name}'

        # 异步并发获取两个时间段的数据
        async def fetch_both():
            """并发获取当前和昨日数据"""
            async def fetch_current():
                return self.get_metric_data(
                    region_id="cn-north-4",
                    instance_list=self.instance_info['id'].tolist(),
                    metric_name=metric_name,
                    TimeDict=time_dict,
                    period="60",
                    filter="max",
                    field_name="max",
                    statistics_approach=[statistics_approach],
                    group_by=self.DimensionsName,
                    rename_field_name=metric_name
                )

            async def fetch_previous():
                return self.get_metric_data(
                    region_id="cn-north-4",
                    instance_list=self.instance_info['id'].tolist(),
                    metric_name=metric_name,
                    TimeDict=previous_day_time_dict,
                    period="60",
                    filter="max",
                    field_name="max",
                    statistics_approach=[statistics_approach],
                    group_by=self.DimensionsName,
                    rename_field_name=display_metric_name
                )

            return await asyncio.gather(fetch_current(), fetch_previous())
        # 并发执行两个数据查询
        try:
            loop = asyncio.get_running_loop()
            df_data, df_previous = loop.run_until_complete(fetch_both())
        except RuntimeError:
            df_data, df_previous = asyncio.run(fetch_both())
        # 合并指标数据
        df_metrics_merged = pd.merge(df_data, df_previous, on=self.DimensionsName, how='left')
        df_metrics_merged.rename(columns={self.DimensionsName: 'id'}, inplace=True)

        # 计算指标
        self.instance_data = pd.merge(self.instance_info, df_metrics_merged, on=['id'], how='left')
        self.instance_data['query_bandwidth'] = (self.instance_data[f'{metric_name}_{statistics_approach}'] / (1000**2)).round(2)
        self.instance_data['previous_day_bandwidth'] = (self.instance_data[f'{display_metric_name}_{statistics_approach}'] / (1000**2)).round(2)
        self.instance_data['bandwidth_delta'] = (self.instance_data['query_bandwidth'] - self.instance_data['previous_day_bandwidth']).round(2)
        self.instance_data.sort_values(by=[f'{metric_name}_{statistics_approach}'], ascending=False, inplace=True)
        self.instance_data.reset_index(drop=True, inplace=True)

        result_df = self.query_binded_info(return_raw=False, input_df=self.instance_data.head(display_size))
        return result_df


# ======================================== GBWP (Global Bandwidth Package) ======================================== #

class GBWP(HwyunInstance):
    def __init__(self, access_key, secret_key) -> None:
        super().__init__(access_key, secret_key)
        self.namespace = "SYS.GEIP"
        self.DimensionsName = "geip_internet_bandwidth_id"
        self.csp_name = "Huawei"
        self.EIPsFromAllGBWP = None

    def fetch_instance_info(
        self,
        instance_id_list: Optional[List[str]] = None,
        name_like: Optional[str] = None,
        status: Optional[str] = None,
        ext_fields: Optional[List[str]] = None,
        page_size: int = 2000,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取全球带宽包信息（支持完整分页查询）

        华为云API文档：https://console.huaweicloud.com/apiexplorer/#/openapi/GEIP/doc?api=ListInternetBandwidths

        Args:
            instance_id_list: 带宽包ID列表（精确匹配）
            name_like: 带宽包名称模糊查询
            status: 带宽包状态（active/idle/creating/deleting等）
            ext_fields: 额外返回的字段（如：traffic_distribution、is_pre_paid）
            page_size: 每页数量（范围1-2000，默认2000，最大值2000）
            auto_accumulate: 是否自动累加到 self.instance_info

        Returns:
            List[Dict]: 原始记录列表（完整数据，已处理分页）

        API参数说明：
            - limit: 每页数量（1-2000）
            - offset: 起始偏移量（>=0）
            - marker: 分页标记（上一页最后一条记录的ID）
            - id: 精确匹配的带宽包ID（多个ID用逗号分隔）
            - name_like: 名称模糊匹配
            - status: 状态过滤
            - ext-fields: 额外字段（traffic_distribution, is_pre_paid 等）
        """
        # 验证 page_size 范围
        if not 1 <= page_size <= 2000:
            print(f"[WARNING] page_size must be between 1 and 2000, default to {page_size}")
            page_size = 2000

        credentials = GlobalCredentials(self.access_key, self.secret_key)
        client = GeipClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(GeipRegion.value_of("cn-north-4")) \
            .build()

        raw_records = []
        offset = 0
        marker = None
        has_more = True

        try:
            while has_more:
                request = ListInternetBandwidthsRequest()

                # 设置查询参数
                if instance_id_list:
                    request.id = instance_id_list
                if name_like:
                    request.name_like = name_like
                if status:
                    request.status = status
                if ext_fields:
                    request.ext_fields = ext_fields

                request.limit = page_size
                request.offset = offset

                # 如果有 marker，使用 marker 分页（优先级高于 offset）
                if marker:
                    request.marker = marker

                response_json = client.list_internet_bandwidths(request).to_json_object()
                current_page = response_json.get('internet_bandwidths', [])

                if not current_page:
                    break

                raw_records.extend(current_page)

                # 判断是否还有下一页
                # 华为云返回字段：total_count（总数），当前偏移量+当前页数量 < 总数 则还有下一页
                total_count = response_json.get('total_count', 0)
                current_page_count = len(current_page)

                if total_count and (offset + current_page_count) >= total_count:
                    has_more = False
                else:
                    offset += current_page_count
                    # 如果API返回了marker，使用marker分页
                    if 'marker' in response_json:
                        marker = response_json['marker']
                        has_more = marker is not None
                    elif current_page_count < page_size:
                        # 当前页数据不足一页，说明已到最后一页
                        has_more = False

        except exceptions.ClientRequestException as e:
            print(f"[Error] Failed to fetch GBWP info: Request id: {e.request_id}\n"
                  f"Status code: {e.status_code}, error code: {e.error_code}\n"
                  f"Error message: {e.error_msg}")

        if auto_accumulate and raw_records:
            self._accumulate_instance_info(raw_records)

        return raw_records

    def extract_eip_from_ins_info(self, simplify: bool = True) -> pd.DataFrame:
        """
        从 instance_info 中提取绑定的 EIP 信息

        Args:
            simplify: 是否简化字段

        Returns:
            pd.DataFrame: EIP 信息表
        """
        if self.instance_info.empty:
            print(f"[Info] No {self.instance_type} data found in instance_info. Returning empty DataFrame.")
            return pd.DataFrame()

        df = self.instance_info.copy()

        # 展平 EIP 列表
        if "public_ip_info" in df.columns:
            df['EIP_List'] = df['public_ip_info'].apply(
                lambda x: x.get('public_ip_addresses', []) if isinstance(x, dict) else []
            )
        elif "ips" in df.columns:
            df['EIP_List'] = df['ips'].apply(
                lambda x: x if isinstance(x, list) else []
            )
        else:
            print("[Warning] No EIP field found in instance_info.")
            return pd.DataFrame()

        df = df[df['EIP_List'].apply(len) > 0]
        df = df.explode('EIP_List').reset_index(drop=True)

        # 展平 EIP 详情
        eip_details = pd.json_normalize(df['EIP_List'].tolist())
        df_result = pd.concat([df, eip_details], axis=1)

        # 简化字段
        if simplify:
            keep_columns = []
            for col in ['id', 'ip_address', 'ip_version', 'status', 'InternetBandwidthId', 'BandwidthPackageName']:
                if col in df_result.columns:
                    keep_columns.append(col)
            if keep_columns:
                df_result = df_result[keep_columns]

        return df_result


# ======================================== PORT (弹性网卡) ======================================== #

class PORT(HwyunInstance):
    """
    华为云弹性网卡 (PORT)
    注意：此为辅助网卡，主网卡在ECS实例中
    """
    def __init__(self, access_key, secret_key) -> None:
        super().__init__(access_key, secret_key)
        self.csp_name = "Huawei"

    async def fetch_instance_info_async(
        self,
        region_id: str,
        instance_id_list: Optional[List[str]] = None,
        max_concurrency: int = 10,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        异步并发获取弹性网卡信息（使用线程池并发）

        华为云SDK是同步的，使用asyncio.to_thread在独立线程中执行

        Args:
            region_id: 地域ID
            instance_id_list: 网卡ID列表
            max_concurrency: 最大并发数（默认10，可根据API限流调整）
            auto_accumulate: 是否自动累加

        Returns:
            List[Dict]: 原始记录列表
        """
        if not instance_id_list:
            print("[Warning] instance_id_list is empty. Returning empty list.")
            return []

        credentials = BasicCredentials(self.access_key, self.secret_key)
        client = hwvpcv2.VpcClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(VpcRegion.value_of(region_id)) \
            .build()

        semaphore = asyncio.Semaphore(max_concurrency)

        async def fetch_single(port_id: str) -> Optional[Dict]:
            """并发获取单个网卡"""
            async with semaphore:
                try:
                    # 定义一个同步函数，包装SDK调用
                    def sync_fetch():
                        request = hwvpcv2.NeutronShowPortRequest()
                        request.port_id = port_id
                        response = client.neutron_show_port(request)
                        return response.to_json_object()

                    # 在线程池中执行同步调用
                    response_json = await asyncio.to_thread(sync_fetch)
                    port = response_json.get('port')
                    # pprint(response_json)
                    return port
                except exceptions.ClientRequestException as e:
                    print(f"[Warn] Failed to fetch PORT {port_id} in {region_id}: {e}")
                    return None
                except Exception as e:
                    print(f"[Error] Unexpected error fetching PORT {port_id}: {e}")
                    return None

        # 创建所有并发任务
        tasks = [fetch_single(port_id) for port_id in instance_id_list]

        # 并发执行（忽略异常）
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 过滤有效结果
        raw_records = []
        for result in results:
            if isinstance(result, dict):
                raw_records.append(result)
            elif isinstance(result, Exception):
                print(f"[Error] Fetch task failed: {result}")

        if auto_accumulate and raw_records:
            self._accumulate_instance_info(raw_records)

        return raw_records

    def fetch_instance_info(
        self,
        region_id: str,
        instance_id_list: Optional[List[str]] = None,
        private_ip_address_list: Optional[List[str]] = None,
        page_size: int = 50,
        max_concurrency: int = 10,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取弹性网卡信息（支持并发）

        Args:
            region_id: 地域ID
            instance_id_list: 网卡ID列表
            private_ip_address_list: 私有IP列表（未实现）
            page_size: 每页数量（向后兼容参数，实际未使用）
            max_concurrency: 最大并发数（默认10，可根据API限流调整）
            auto_accumulate: 是否自动累加

        Returns:
            List[Dict]: 原始记录列表
        """
        if not instance_id_list:
            print("[Warning] instance_id_list is empty. Returning empty list.")
            return []

        # 运行异步并发方法
        try:
            loop = asyncio.get_running_loop()
            return loop.run_until_complete(
                self.fetch_instance_info_async(region_id, instance_id_list, max_concurrency, auto_accumulate)
            )
        except RuntimeError:
            # 没有运行的事件循环，创建新循环
            return asyncio.run(
                self.fetch_instance_info_async(region_id, instance_id_list, max_concurrency, auto_accumulate)
            )