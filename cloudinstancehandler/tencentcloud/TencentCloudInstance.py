# cloudinstacehandler/tencentcloud/TencentCloudInstance.py
import json, time
import warnings
from typing import List, Dict, Any, Optional
import pandas as pd
import datetime

from cloudinstancehandler.common.funcs import get_time_range
from cloudinstancehandler.common.BasicDataFrame import BasicDataFrame
from cloudinstancehandler.common.deprecated import deprecated_alias

from tencentcloud.common import credential
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.monitor.v20180724 import monitor_client, models


class TencentCloudInstance(BasicDataFrame):
    def __init__(self, access_key: str, secret_key: str) -> None:
        super().__init__()
        self.instance_type = self.__class__.__name__
        # 兼容性保留，后期版本也许会移除 self.credentials
        # 当有实际请求时再传入 self.access_key, self.secret_key 来构建 credentials
        self.credentials = credential.Credential(access_key, secret_key)
        self.access_key = access_key
        self.secret_key = secret_key
        self.namespace: str = None
        self.dimension_name: str = 'instanceId'
        self.csp_name = 'TencentCloud'

    def get_metric_data(
        self,
        instance_list: List[str],
        metric_name: str,
        TimeDict: Dict[str, Any],
        period: str = '300',
        dimensions_df: Optional[pd.DataFrame] = None,
        region_id: str = 'ap-guangzhou',
        page_size: int = 50,
        sleep_time: float = 0,
        field_name: str = 'Maximum',
        statistics_approach: Optional[List[str]] = None,
        group_by: Optional[str] = None,
        rename_field_name: Optional[str] = None
    ) -> pd.DataFrame:
        """
        从腾讯云监控获取指定实例的指标数据，并可选进行二次聚合统计。

        重构说明：
        - 统一方法命名和参数签名，与其他厂商保持一致
        - 支持跳过统计（返回原始数据）、单列统计、多方法聚合等灵活用法
        - 添加完整的异常处理和错误提示
        - 调用统一的 statistic_metric_data() 方法进行统计

        Args:
            instance_list (list): 实例 ID 列表，如 ['ins-abc123', 'ins-def456']
            metric_name (str): 指标名称，如 'CPUUtilization'。请参考官方文档确认可用指标：
                              https://cloud.tencent.com/document/product/248/6143
            TimeDict (dict): 时间范围字典，使用 get_time_range 函数生成。
            period (str): 统计周期（秒），默认 '300'（5分钟）。腾讯云支持 60/300/900 等。
            dimensions_df (pd.DataFrame, optional): 自定义维度数据，在需要获取多维度数据时使用。
                                                与 instance_list 二选一。默认 None。
            region_id (str): 地域 ID，如 'ap-guangzhou'（广州）、'ap-shanghai'（上海）等。默认 'ap-guangzhou'。
            page_size (int): 每批次查询的实例数量（腾讯云限制），默认 50。
            sleep_time (float): 批次间休眠时间（秒），用于避免 API 限流。默认 0。
            field_name (str): 原始数据中用于统计的数值列名，默认 'Maximum'。
                            若该列不存在，函数会自动尝试从 ["Value", "Maximum", "Minimum", "Average", "Sum"] 中匹配第一个存在的列。
                            若为 None 或空字符串，则跳过统计，返回原始数据。
            statistics_approach (list, optional): 统计方法列表。行为如下：
                                                - 未传 → 默认使用 ['max']
                                                - None 或 []（空列表）→ 跳过统计，返回原始数据
                                                - 支持方法: ['last', 'max', 'min', 'avg', 'max_95', 'sum'] 或 ['all']
                                                    （具体支持方法取决于 self.statistic_metric_data 实现）
            group_by (str): 分组字段，如 'instanceId'、'Timestamp' 等。仅在 statistics_approach 非空时生效。
                            若 statistics_approach 为 [] 或 None，此参数被忽略。
            rename_field_name (str, optional): 重命名统计结果列的名称（可选）。
                                                若为 None，使用 metric_name 作为默认值。

        Returns:
            pd.DataFrame:
                - 若 statistics_approach 为 [] → 返回原始监控点（包含 Timestamp, instanceId, Value/Maximum 等列）
                - 若执行统计 → 返回聚合后结果，列包含 group_by 字段 + 统计结果列（如 max_value, avg_value 等）

        腾讯云官方文档：https://cloud.tencent.com/document/product/248/33911
        单次请求最多 50 个实例，时间范围不超过 60 天。

        注意：
        - 指标数据的时间精度可能与 period 不完全一致，取决于指标本身的采集周期
        - 建议在高频调用时设置 sleep_time > 0 避免触发 API 限流
        """
        # ===== 参数验证 =====
        duration_time = TimeDict["end_datetime"] - TimeDict["start_datetime"]
        if duration_time > datetime.timedelta(days=60) or page_size > 50 or page_size < 1:
            warnings.warn(
                f"Duration time {duration_time} exceeds 60 days limit or page_size {page_size} is invalid. "
                f"This may lead to incomplete data or API failure.",
                UserWarning,
                stacklevel=2
            )

        if not self.namespace:
            raise ValueError("Namespace is not set. Please set Namespace in the subclass.")

        # ===== 构造维度数据 =====
        if dimensions_df is not None and not dimensions_df.empty:
            dimensions_list = [
                {
                    "Dimensions": [
                        {"Name": k, "Value": str(v)}
                        for k, v in record.items()
                        if pd.notna(v)
                    ]
                }
                for record in dimensions_df.to_dict(orient='records')
            ]
        elif instance_list is not None:
            dimensions_list = [
                {"Dimensions": [{"Name": self.dimension_name, "Value": InstanceID}]}
                for InstanceID in instance_list
            ]
        else:
            raise ValueError("Either dimensions_df or instance_list must be provided.")

        # ===== 初始化客户端 =====
        httpProfile = HttpProfile()
        httpProfile.endpoint = "monitor.tencentcloudapi.com"
        clientProfile = ClientProfile()
        clientProfile.httpProfile = httpProfile
        client = monitor_client.MonitorClient(self.credentials, region_id, clientProfile)

        # ===== 分批查询 =====
        json_data = []
        for idx in range(0, len(dimensions_list), page_size):
            batch_dimensions = dimensions_list[idx:idx + page_size]

            try:
                req = models.GetMonitorDataRequest()
                params = {
                    "Namespace": self.namespace,
                    "MetricName": metric_name,
                    "Period": period,
                    "StartTime": TimeDict['start_timestring_iso8601'],
                    "EndTime": TimeDict['end_timestring_iso8601'],
                    "Instances": batch_dimensions,
                }
                req.from_json_string(json.dumps(params))

                resp = client.GetMonitorData(req)
                response_json = json.loads(resp.to_json_string())

                # ===== 关键修复：正确解析腾讯云返回的数据结构 =====
                # 腾讯云返回格式：
                # {
                #   "DataPoints": [
                #     {
                #       "Dimensions": [{"Name": "instanceId", "Value": "xxx"}],
                #       "Timestamps": [timestamp1, timestamp2, ...],
                #       "Values": [value1, value2, ...],
                #       "MaxValues": [max1, max2, ...] (可选),
                #       "MinValues": [min1, min2, ...] (可选),
                #       "AvgValues": [avg1, avg2, ...] (可选)
                #     }
                #   ]
                # }
                data_points = response_json.get('DataPoints', [])

                for instance_data in data_points:
                    # 提取维度信息
                    dimensions = instance_data.get('Dimensions', [])
                    dim_dict = {}
                    for dim in dimensions:
                        dim_name = dim.get('Name')
                        dim_value = dim.get('Value')
                        dim_dict[dim_name] = dim_value

                    # 提取时间戳和值数组
                    timestamps = instance_data.get('Timestamps', [])
                    values = instance_data.get('Values', [])

                    # 检查是否有其他统计值
                    max_values = instance_data.get('MaxValues', [])
                    min_values = instance_data.get('MinValues', [])
                    avg_values = instance_data.get('AvgValues', [])

                    # 合并数据
                    for i in range(len(timestamps)):
                        data_point = {
                            'Timestamp': timestamps[i]
                        }
                        # 添加维度信息
                        data_point.update(dim_dict)

                        # 添加值字段
                        if i < len(values):
                            data_point['Value'] = values[i]
                        if max_values and i < len(max_values) and max_values[i] is not None:
                            data_point['Maximum'] = max_values[i]
                        if min_values and i < len(min_values) and min_values[i] is not None:
                            data_point['Minimum'] = min_values[i]
                        if avg_values and i < len(avg_values) and avg_values[i] is not None:
                            data_point['Average'] = avg_values[i]

                        json_data.append(data_point)

                # 批次间休眠
                if sleep_time > 0 and idx + page_size < len(dimensions_list):
                    time.sleep(sleep_time)

            except TencentCloudSDKException as e:
                print(f"[Error] Tencent Cloud SDK Exception at batch {idx//page_size}:")
                print(f"  Request ID: {e.requestId}")
                print(f"  Error Code: {e.code}")
                print(f"  Error Message: {e.message}")
                continue
            except Exception as e:
                print(f"[Error] Unexpected error at batch {idx//page_size}: {e}")
                continue

        # ===== 处理查询结果 =====
        if not json_data:
            print("[Info] No data found for the specified instance and metric.")
            return pd.DataFrame()

        df_data = pd.DataFrame(json_data)

        if 'Timestamp' in df_data.columns:
            df_data.sort_values(by='Timestamp', inplace=True)

        self.metric_data = df_data

        # ===== 二次统计处理 =====
        # 处理 statistics_approach 默认值
        if statistics_approach is None:
            statistics_approach = ['max']

        # 判定是否需要进行统计
        if statistics_approach and field_name and not df_data.empty:
            rename_field_name = rename_field_name or metric_name
            group_by = group_by or self.dimension_name

            # 自动匹配字段名
            Allowed_Field_Name = ["Value", "Maximum", "Minimum", "Average", "Sum"]
            df_data_columns_set = set(df_data.columns)

            if field_name not in df_data_columns_set:
                for stat in Allowed_Field_Name:
                    if stat in df_data_columns_set:
                        field_name = stat
                        break

            # 执行统计
            try:
                df_data = self.statistic_metric_data(
                    field_name=field_name,
                    df_data=df_data,
                    statistics_approach=statistics_approach,
                    group_by=group_by,
                    rename_field_name=rename_field_name
                )
            except Exception as e:
                print(f"[Error] Failed to call statistic_metric_data: {e}")
                raise

        return df_data

    # ==================== 已废弃的 camelCase 兼容别名 ==================== #
    # 老代码通过以下别名调用仍可正常工作，但会触发 DeprecationWarning，
    # 请迁移至对应的 snake_case 方法，别名将在未来版本中移除。

    @deprecated_alias("get_metric_data")
    def getMetricData(self, *args, **kwargs):  # noqa: N802
        """[已废弃别名] 请改用 get_metric_data"""
        return self.get_metric_data(*args, **kwargs)

    # TODO: 子类自行实现适配
    # def getInsInfo(self):
    #     pass
    #
    # def getInsData(self):
    #     pass
