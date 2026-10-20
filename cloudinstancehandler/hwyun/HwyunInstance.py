import json,time

from cloudinstancehandler.common.funcs import *
from cloudinstancehandler.common.BasicDataFrame import *
from cloudinstancehandler.common.deprecated import deprecated_alias

from huaweicloudsdkcore.exceptions import exceptions
from huaweicloudsdkcore.auth.credentials import BasicCredentials
from huaweicloudsdkces.v1.region.ces_region import CesRegion
import huaweicloudsdkces.v1 as CesV1

class HwyunInstance(BasicDataFrame):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__()
        self.instance_type = self.__class__.__name__
        self.credentials = BasicCredentials(access_key, secret_key)
        self.access_key = access_key
        self.secret_key = secret_key
        self.namespace:str = None
        self.DimensionsName:str = "id"
        self.csp_name = "HuaweiCloud"

    def get_metric_data(
        self,
        region_id: str,
        instance_list: list,
        metric_name: str,
        TimeDict: dict,
        period: str = "300",
        Dimensions: list = None,
        filter: str = "max",
        field_name: str = "max",
        statistics_approach: list = None,
        group_by = None,
        rename_field_name: str = None,
        sleep_time: float = 0.5
    ) -> pd.DataFrame:
        """
        获取华为云监控指标数据，并可选进行二次聚合统计

        重构说明：
        - 先使用原生数据结构（List[Dict]）收集所有原始数据点
        - 一次性转换为 DataFrame
        - 调用统一的 statistic_metric_data() 方法进行统计
        - 移除方法内的内联统计逻辑

        Args:
            region_id: 地域ID
            instance_list: 实例ID列表
            metric_name: 指标名称（参考华为云官方文档）
            TimeDict: 时间范围字典（使用 get_time_range 生成）
            period: 统计周期（秒），默认300（5分钟），华为云支持60/300/900等
            Dimensions: 自定义维度（未实现，暂时忽略）
            filter: API原生统计方式（max/min/average/sum/variance）
                - average：聚合周期内指标数据的平均值
                - max：聚合周期内指标数据的最大值
                - min：聚合周期内指标数据的最小值
                - sum：聚合周期内指标数据的求和值
                - variance：聚合周期内指标数据的方差
            field_name: 原始数据中用于二次统计的数值列名（默认"max"）
                - 若该列不存在，会自动从 ['max', 'min', 'average', 'sum', 'variance'] 中匹配第一个存在的列
                - 若为None或空字符串，跳过二次统计，返回原始数据
            statistics_approach: 二次统计方法列表
                - 未传 → 默认使用 ['max']
                - None 或 []（空列表）→ 跳过统计，返回原始数据
                - 支持方法: ['last', 'max', 'min', 'avg', 'max_95', 'sum', 'all']
                    （具体支持方法取决于 statistic_metric_data 实现）
            group_by: 分组字段（如 'id'、'timestamp' 等）
                - 仅在 statistics_approach 非空时生效
                - 若为 None，自动使用除 field_name 和 'timestamp' 外的所有字段
                - 若 statistics_approach 为 [] 或 None，此参数被忽略
            rename_field_name: 重命名统计结果列的名称（可选）
                - 若为 None，使用 metric_name 作为默认值
            sleep_time: 批次间休眠时间（秒），用于避免 API 限流（默认0.5）

        Returns:
            pd.DataFrame:
                - 若 statistics_approach 为 [] → 返回原始监控点（包含 timestamp, id, max/min 等列）
                - 若执行统计 → 返回聚合后结果，列包含 group_by 字段 + 统计结果列

        华为云官方文档：https://support.huaweicloud.com/usermanual-ces/zh-cn_topic_0202622212.html

        注意：
        filter 参数是华为云 API 定义的，用于指定原始数据的聚合方式
        statistics_approach 是本框架定义的，用于对 API 返回的数据进行二次统计
        """
        # 合法性检查
        allowed_filters = ['max', 'min', 'average', 'sum', 'variance']
        if filter not in allowed_filters:
            raise ValueError(f"Invalid filter value '{filter}'. Allowed values are: {', '.join(allowed_filters)}")

        if rename_field_name is None:
            rename_field_name = metric_name

        # 构造维度数据（Dimensions 未实现，暂时使用 instance_list）
        if Dimensions is None:
            metrics = [
                {
                    "namespace": self.namespace,
                    "metric_name": metric_name,
                    "dimensions": [
                        {
                            "name": self.DimensionsName,
                            "value": instance_id
                        }
                    ],
                } for instance_id in instance_list
            ]
        else:
            # TODO: 实现自定义 Dimensions
            metrics = [
                {
                    "namespace": self.namespace,
                    "metric_name": metric_name,
                    "dimensions": dim_list,
                } for dim_list in Dimensions
            ]

        credentials = BasicCredentials(self.access_key, self.secret_key)
        client = CesV1.CesClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(CesRegion.value_of(region_id)) \
            .build()

        # 指标数量 * (to - from) / 监控周期 ≤ 3000
        # (to - from) ≤ 3000 * 监控周期 / 指标数量
        total_metrics = len(metrics)
        if total_metrics > 500:
            raise ValueError("Too many metrics, please reduce the number of metrics or increase the period")

        max_query = int(3000 * int(period) / total_metrics)
        current_start_timestamp = TimeDict['start_timestamp']
        current_query_times = 1

        # 使用原生数据结构收集所有原始数据点
        raw_data_points: List[Dict[str, Any]] = []

        while current_start_timestamp < TimeDict['end_timestamp']:
            if current_query_times % 5 == 0:
                print(f"Wait {sleep_time} seconds ...")
                time.sleep(sleep_time)

            current_end_timestamp = min(current_start_timestamp + max_query, TimeDict['end_timestamp'])

            try:
                request = CesV1.BatchListMetricDataRequest()
                request.body = CesV1.BatchListMetricDataRequestBody(
                    metrics=metrics,
                    period=period,
                    _from=current_start_timestamp * 1000,
                    to=current_end_timestamp * 1000,
                    filter=filter
                )
                response = client.batch_list_metric_data(request)
                response_json = response.to_json_object()
                # pprint(response_json)

                # 遍历每个指标的数据点
                for metric_data in response_json.get("metrics", []):
                    # 提取维度信息
                    dimensions_dict = {}
                    for dimension in metric_data.get('dimensions', []):
                        dimensions_dict[dimension.get("name")] = dimension.get("value")

                    # 提取数据点并补充维度信息
                    for datapoint in metric_data.get("datapoints", []):
                        data_point_with_dims = datapoint.copy()
                        data_point_with_dims.update(dimensions_dict)
                        raw_data_points.append(data_point_with_dims)

            except exceptions.ClientRequestException as e:
                print(f"[Error] Request id: {e.request_id}\n"
                      f"Status code: {e.status_code}, error code: {e.error_code}\n"
                      f"Error message: {e.error_msg}")
                continue

            current_start_timestamp = current_end_timestamp
            current_query_times += 1

        # 一次性转换为 DataFrame
        if raw_data_points:
            df_data = pd.DataFrame(raw_data_points)
            self.metric_data = df_data
        else:
            print("[Info] No data found for the specified instances and metric.")
            return pd.DataFrame()

        # 根据 statistics_approach 决定是否进行二次统计
        if statistics_approach and field_name and not df_data.empty:
            # 自动匹配字段名
            Allowed_Field_Name = ['max', 'min', 'average', 'sum', 'variance']
            df_data_columns_set = set(df_data.columns)

            if field_name not in df_data_columns_set:
                for stat in Allowed_Field_Name:
                    if stat in df_data_columns_set:
                        field_name = stat
                        break

            # 确保按时间排序（用于 last 统计）
            if 'timestamp' in df_data.columns:
                df_data.sort_values(by='timestamp', inplace=True)

            # 自动推断 group_by（若未指定）
            if group_by is None:
                group_by = list(df_data_columns_set - {field_name, 'timestamp'})

            # 调用统一的统计方法
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

