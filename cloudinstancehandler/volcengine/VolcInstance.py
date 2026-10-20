# coding: utf-8

import asyncio
import warnings
from typing import List, Dict, Any, Optional
from pprint import pprint
import pandas as pd

from cloudinstancehandler.common.funcs import get_time_range
from cloudinstancehandler.common.BasicDataFrame import BasicDataFrame

import volcenginesdkcore
import volcenginesdkvolcobserve
from volcenginesdkcore.rest import ApiException


class VolcInstance(BasicDataFrame):
    """
    火山云实例基类

    参考 AliyunInstance 设计范式统一重构
    """
    def __init__(self, access_key: str, secret_key: str) -> None:
        super().__init__()
        self.instance_type = self.__class__.__name__
        self.access_key = access_key
        self.secret_key = secret_key
        self.namespace: str = None
        self.sub_namespace: str = None
        self.dimensions: str = "ResourceID"
        self.csp_name = "VolcEngine"

    def _create_configuration(self, region_id: str = "cn-shanghai") -> volcenginesdkcore.Configuration:
        """
        创建火山云配置对象
        """
        configuration = volcenginesdkcore.Configuration()
        configuration.ak = self.access_key
        configuration.sk = self.secret_key
        configuration.region = region_id
        return configuration

    async def get_metric_data(
        self,
        instance_id_list: List[str],
        metric_name: str,
        time_dict: Dict,
        period: str = "5m",
        groupby: str = None,
        statistics_approach: List[str] = ['max'],
        display_metric_name: str = None,
        region_id: str = "cn-shanghai",
        page_size: int = 200
    ) -> pd.DataFrame:
        """
        从火山云监控获取指标数据（异步并发）

        Args:
            instance_id_list: 实例ID列表
            metric_name: 指标名称
            time_dict: 时间范围字典（使用 get_time_range 生成）
            period: 统计周期，默认 "5m"
            groupby: 分组字段，支持 'ResourceID' | 'timestamp' | None
            statistics_approach: 统计方法列表，支持 ['last', 'max', 'min', 'avg', 'max_95', 'sum', 'all', 'raw']
            display_metric_name: 显示的指标名称（默认使用 metric_name）
            region_id: 地域ID
            page_size: 每批次查询数量

        Returns:
            pd.DataFrame: 指标数据
        """
        # 参数验证
        dimensions_list = self.dimensions.split(',')
        if groupby == 'ResourceID':
            groupby = self.dimensions
        if groupby not in ['timestamp', None] + dimensions_list:
            raise ValueError(
                f"Invalid groupby value '{groupby}'. "
                f"Allowed values are: {', '.join([self.dimensions, 'timestamp', 'None'])}"
            )

        allowed_stats = ['last', 'max', 'min', 'avg', 'max_95', 'sum', 'all', 'raw']
        if 'all' in statistics_approach:
            statistics_approach = allowed_stats[:-2]
        elif 'raw' in statistics_approach or statistics_approach == [] or statistics_approach is None:
            statistics_approach = None
        elif not set(statistics_approach).issubset(set(allowed_stats)):
            raise ValueError(
                f"Invalid statistics_approach value(s): {set(statistics_approach) - set(allowed_stats)}. "
                f"Allowed values are: {', '.join(allowed_stats)}"
            )

        if display_metric_name is None:
            display_metric_name = metric_name

        # 火山云SDK不支持原生async，使用run_in_executor包装
        loop = asyncio.get_running_loop()

        async def _fetch_batch(batch_ids: List[str], sem: asyncio.Semaphore) -> pd.DataFrame:
            async with sem:
                def sync_fetch():
                    try:
                        instances = []
                        for instance_id in batch_ids:
                            instances.append(
                                volcenginesdkvolcobserve.InstanceForGetMetricDataInput(
                                    dimensions=[
                                        volcenginesdkvolcobserve.DimensionForGetMetricDataInput(
                                            name=self.dimensions,
                                            value=instance_id,
                                        )
                                    ]
                                )
                            )

                        configuration = self._create_configuration(region_id)
                        api_instance = volcenginesdkvolcobserve.VOLCOBSERVEApi(
                            volcenginesdkcore.ApiClient(configuration)
                        )
                        response = api_instance.get_metric_data(
                            volcenginesdkvolcobserve.GetMetricDataRequest(
                                start_time=time_dict['start_timestamp'],
                                end_time=time_dict['end_timestamp'],
                                period=period,
                                namespace=self.namespace,
                                sub_namespace=self.sub_namespace,
                                metric_name=metric_name,
                                instances=instances
                            )
                        )
                        return response.to_dict()
                    except ApiException as e:
                        print(f"\nError: Exception when calling GetMetricData: {e}\n")
                        return None

                response_json = await loop.run_in_executor(None, sync_fetch)
                if not response_json or not response_json.get('data'):
                    return pd.DataFrame()

                df_data = pd.DataFrame()
                for ins_data in response_json['data'].get('metric_data_results', []):
                    df_tmp = pd.DataFrame(ins_data.get('data_points', []))
                    for dimension in ins_data.get('dimensions', []):
                        df_tmp[dimension.get('name')] = dimension.get('value')
                    df_data = pd.concat([df_data, df_tmp])

                return df_data

        # 并发查询
        semaphore = asyncio.Semaphore(10)
        batches = [instance_id_list[i:i+page_size] for i in range(0, len(instance_id_list), page_size)]
        tasks = [_fetch_batch(batch, semaphore) for batch in batches]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        df_combined = pd.DataFrame()
        for result in results:
            if isinstance(result, pd.DataFrame) and not result.empty:
                df_combined = pd.concat([df_combined, result], ignore_index=True)
            elif isinstance(result, Exception):
                print(f"[Error] Unexpected task error: {result}")

        # 空数据保护：无数据时跳过统计（否则 groupby 维度列会 KeyError）
        if df_combined.empty:
            print("[Info] No data found for the specified instances and metric.")
            self.metric_data = df_combined
            return df_combined

        # 统计处理
        statistic = "value"  # 火山云返回的指标值字段名
        aggregation_functions = {}
        if statistics_approach:
            if "last" in statistics_approach:
                aggregation_functions[f'{display_metric_name}_last'] = (f'{statistic}', lambda x: x.iloc[-1])
            if "max" in statistics_approach:
                aggregation_functions[f'{display_metric_name}_max'] = (f'{statistic}', 'max')
            if "min" in statistics_approach:
                aggregation_functions[f'{display_metric_name}_min'] = (f'{statistic}', 'min')
            if "avg" in statistics_approach:
                aggregation_functions[f'{display_metric_name}_avg'] = (f'{statistic}', 'mean')
            if "max_95" in statistics_approach:
                aggregation_functions[f'{display_metric_name}_max_95'] = (f'{statistic}', lambda x: x.quantile(0.95))
            if "sum" in statistics_approach:
                aggregation_functions[f'{display_metric_name}_sum'] = (f'{statistic}', 'sum')

            if groupby:
                df_combined = df_combined.groupby(groupby).agg(**aggregation_functions).reset_index()
            else:
                df_combined = df_combined.agg(**aggregation_functions).reset_index()
        else:
            print('Params statistics_approach contains "raw", returning raw data!')

        self.metric_data = df_combined
        return df_combined
