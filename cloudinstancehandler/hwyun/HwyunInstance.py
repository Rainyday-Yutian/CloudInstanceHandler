import json,time

from cloudinstancehandler.common.funcs import *
from cloudinstancehandler.common.BasicDataFrame import *

from huaweicloudsdkcore.exceptions import exceptions
from huaweicloudsdkcore.auth.credentials import BasicCredentials
from huaweicloudsdkces.v1.region.ces_region import CesRegion
import huaweicloudsdkces.v1 as CesV1

class HwyunInstance(BasicDataFrame):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__()
        self.instance_type = self.__class__.__name__
        self.access_key = access_key
        self.secret_key = secret_key
        self.namespace:str = None
        self.DimensionsName:str = None
        self.Prefix = "Huawei"

    def getMetricData(self,region_id:str,instance_list:list,metric_name:str,TimeDict:dict,period:str="300",Dimensions:list=None,filter:str="max",field_name="max",statistics_approach:list=None,group_by=None,rename_field_name=None,sleep_time=0.5):
        """
        filter: max, min, average, sum, varianc
        华为API定义的filter参数，表示请求中指定的period参数为统计周期，以Dimensions为维度分组，对值进行filter指定方式统计
            average：聚合周期内指标数据的平均值。
            max：聚合周期内指标数据的最大值。
            min：聚合周期内指标数据的最小值。
            sum：聚合周期内指标数据的求和值。
            variance：聚合周期内指标数据的方差。
        filter只能是string，因此最终决定不命名为{metric_name}_{filter}，而是直接命名为{metric_name}，防止与自定义的数据列名StatisticsApproach冲突

        GroupBy: id, timestamp, None 或 []
            id: 以id进行分组统计的依据
            timestamp: 以时间戳进行分组统计的依据
            None 或 []: 不分组直接进行统计

        StatisticsApproach: last, max, min, avg, max_95, raw
        本函数定义的StatisticsApproach参数，表示是方法中指定的TimeDict参数为统计周期，以GroupBy进行分组，对值进行StatisticsApproach指定方式统计
            last: TimeDict指定的时间范围内，以GroupBy进行分组统计，取最新的值。
            max、min、avg: TimeDict指定的时间范围内，以GroupBy进行分组统计，取最大值、最小值、平均值。
            max_95: TimeDict指定的时间范围内，以GroupBy进行分组统计，取95%分位数。
            all: TimeDict指定的时间范围内，以GroupBy进行分组统计，统计以上所有值。
            raw/[]/None: 不进行分组统计，直接返回原始数据，此时GroupBy参数无效。

        metric_name:
        华为云官方查询地址 https://support.huaweicloud.com/usermanual-ces/zh-cn_topic_0202622212.html
        """
        
        # 合法性检查
        # if GroupBy not in allowed_GroupBy or not set(GroupBy).issubset(set(allowed_StatisticsApproach)):
        #     raise ValueError(f"Invalid GroupBy value(s): {set(GroupBy) - set(allowed_GroupBy)}. Allowed values are: {', '.join(allowed_GroupBy)}")

        allowed_filters = ['max', 'min', 'average', 'sum', 'variance']
        if filter not in allowed_filters:
            raise ValueError(f"Invalid filter value '{filter}'. Allowed values are: {', '.join(allowed_filters)}")
        
        if rename_field_name is None:
            rename_field_name = metric_name

        # 自定义Dimensions参数是临时支持的，需要重写，构造Dimensions时，可以参考Dataframe，先铺平
        #     metrics = [
        #         {
        #             "namespace": self.namespace,
        #             "metric_name": metric_name,
        #             "dimensions": [
        #                 {
        #                     "name": self.DimensionsName,
        #                     "value": instance_id
        #                 } 
        #             ],
        #         } for instance_id in instance_list
        #     ]
        # else:
        #     metrics = 
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
        # pprint(metrics)

        credentials = BasicCredentials(self.access_key,self.secret_key)
        client = CesV1.CesClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(CesRegion.value_of(region_id)) \
            .build()

        # 指标数量 * (to - from) / 监控周期 ≤ 3000
        # (to - from) ≤ 3000 * 监控周期 / 指标数量
        
        total_metrics = len(metrics)
        if total_metrics > 500:
            raise ValueError("Too many metrics, please reduce the number of metrics or increase the period")
            # 暂不做分批处理
        max_query = int(3000 * int(period) / len(metrics))
        current_start_timestamp = TimeDict['start_timestamp']
        current_query_times = 1
        df_data = []
        while current_start_timestamp < TimeDict['end_timestamp']:
            if current_query_times%5 == 0:
                print(f"Wait {sleep_time} seconds ...")
                time.sleep(sleep_time)
            current_end_timestamp = min(current_start_timestamp + max_query, TimeDict['end_timestamp'])
            # print("Getting metrics data from ",current_start_timestamp, "->" ,current_end_timestamp)
            try:    
                request = CesV1.BatchListMetricDataRequest()
                request.body = CesV1.BatchListMetricDataRequestBody(
                    metrics = metrics,
                    period = period,
                    _from = current_start_timestamp*1000,
                    to = current_end_timestamp*1000,
                    filter = filter
                )
                response = client.batch_list_metric_data(request)
                # pprint(response.to_json_object())
                for metric_data in response.to_json_object()["metrics"]:
                    df_data_clip = pd.DataFrame(metric_data.get("datapoints",[]))
                    # if df_data_clip.empty: continue
                    for dimension in metric_data['dimensions']:
                        df_data_clip[dimension.get("name")] = dimension.get("value")
                    df_data.append(df_data_clip)
            except exceptions.ClientRequestException as e:
                print(f"Request id: {e.request_id}\nStatus code: {e.status_code}, error code: {e.error_code}\nError message: {e.error_msg}")
                continue
            current_start_timestamp = current_end_timestamp
            current_query_times +=1
        if df_data:
            df_data = pd.concat(df_data,ignore_index=True)
            self.MetricData = df_data
        else:
            df_data = pd.DataFrame()
        
        if statistics_approach and field_name and not df_data.empty:
            if rename_field_name is None:
                rename_field_name = metric_name
            Allowed_Field_Name = ['max', 'min', 'average', 'sum', 'variance']
            df_data_columns_set = set(df_data.columns)
            if field_name not in df_data_columns_set:
                for stat in Allowed_Field_Name:
                    if stat in df_data_columns_set:
                        field_name = stat
                        break
            if 'timestamp' in df_data.columns:
                df_data.sort_values(by='timestamp', inplace=True)
            if group_by is None:
                group_by = list(df_data_columns_set - {field_name, 'timestamp'})
                print(group_by)
            try:
                df_data = self.statisticMetricData(field_name=field_name,df_data=df_data,statistics_approach=statistics_approach,group_by=group_by,rename_field_name=rename_field_name)
            except Exception as e:
                print(f"Error occurred while calling statisticMetricData: {e}")
        return df_data

    def getMetricData_old(self,region_id:str,instance_list:list,metric_name:str,TimeDict:dict,period:str="300",filter:str="max",Dimensions:list=None,statistics_approach:list=None,group_by='id',rename_field_name=None,sleep_time=0):
        """
        filter: max, min, average, sum, varianc
        华为API定义的filter参数，表示请求中指定的period参数为统计周期，以Dimensions为维度分组，对值进行filter指定方式统计
            average：聚合周期内指标数据的平均值。
            max：聚合周期内指标数据的最大值。
            min：聚合周期内指标数据的最小值。
            sum：聚合周期内指标数据的求和值。
            variance：聚合周期内指标数据的方差。
        filter只能是string，因此最终决定不命名为{metric_name}_{filter}，而是直接命名为{metric_name}，防止与自定义的数据列名StatisticsApproach冲突

        GroupBy: id, timestamp, None 或 []
            id: 以id进行分组统计的依据
            timestamp: 以时间戳进行分组统计的依据
            None 或 []: 不分组直接进行统计

        StatisticsApproach: last, max, min, avg, max_95, raw
        本函数定义的StatisticsApproach参数，表示是方法中指定的TimeDict参数为统计周期，以GroupBy进行分组，对值进行StatisticsApproach指定方式统计
            last: TimeDict指定的时间范围内，以GroupBy进行分组统计，取最新的值。
            max、min、avg: TimeDict指定的时间范围内，以GroupBy进行分组统计，取最大值、最小值、平均值。
            max_95: TimeDict指定的时间范围内，以GroupBy进行分组统计，取95%分位数。
            all: TimeDict指定的时间范围内，以GroupBy进行分组统计，统计以上所有值。
            raw/[]/None: 不进行分组统计，直接返回原始数据，此时GroupBy参数无效。

        metric_name:
        华为云官方查询地址 https://support.huaweicloud.com/usermanual-ces/zh-cn_topic_0202622212.html
        """
        
        # 合法性检查
        # if GroupBy not in allowed_GroupBy or not set(GroupBy).issubset(set(allowed_StatisticsApproach)):
        #     raise ValueError(f"Invalid GroupBy value(s): {set(GroupBy) - set(allowed_GroupBy)}. Allowed values are: {', '.join(allowed_GroupBy)}")

        allowed_filters = ['max', 'min', 'average', 'sum', 'variance']
        if filter not in allowed_filters:
            raise ValueError(f"Invalid filter value '{filter}'. Allowed values are: {', '.join(allowed_filters)}")
        
        if rename_field_name is None:
            rename_field_name = metric_name

         # 自定义Dimensions参数是临时支持的，需要重写，构造Dimensions时，可以参考Dataframe，先铺平
        # if Dimensions is None:
        #     metrics = [
        #         {
        #             "namespace": self.namespace,
        #             "metric_name": metric_name,
        #             "dimensions": [
        #                 {
        #                     "name": self.DimensionsName,
        #                     "value": instance_id
        #                 } 
        #             ],
        #         } for instance_id in instance_list
        #     ]
        # else:
        #     metrics = 

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
        # pprint(metrics)

        credentials = BasicCredentials(self.AK,self.sk)
        client = CesV1.CesClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(CesRegion.value_of(region_id)) \
            .build()

        df_data = pd.DataFrame()
        # 指标数量 * (to - from) / 监控周期 ≤ 3000
        # (to - from) ≤ 3000 * 监控周期 / 指标数量
        max_query = int(3000 * int(period) / len(instance_list))
        current_start_timestamp = TimeDict['start_timestamp']
        current_query_times = 1
        while current_start_timestamp < TimeDict['end_timestamp']:
            if current_query_times%5 == 0:
                print(f"Wait {sleep_time} seconds ...")
                time.sleep(sleep_time)
            current_end_timestamp = min(current_start_timestamp + max_query, TimeDict['end_timestamp'])
            # print(current_start_timestamp, "->" ,current_end_timestamp)
            try:    
                request = CesV1.BatchListMetricDataRequest()
                request.body = CesV1.BatchListMetricDataRequestBody(
                    metrics = metrics,
                    period = period,
                    _from = current_start_timestamp*1000,
                    to = current_end_timestamp*1000,
                    filter = filter
                )
                response = client.batch_list_metric_data(request)
                # pprint(response.to_json_object())
                df_MetricData = {'id':[],f'{metric_name}':[],"timestamp":[]}
                pprint(response.to_json_object())
                for insdata in response.to_json_object()["metrics"]:
                    instanceId = insdata['dimensions'][0]['value']
                    for datapoint in insdata['datapoints']:
                        df_MetricData["id"].append(instanceId)
                        df_MetricData[f"{metric_name}"].append(datapoint[f'{filter}'])
                        df_MetricData["timestamp"].append(datapoint['timestamp'])
                        # df_MetricData["metric_name"].append(metric_name)
                df_data_tmp = pd.DataFrame(df_MetricData)
                # print(df_data_tmp)
            except exceptions.ClientRequestException as e:
                print(f"Request id: {e.request_id}\nStatus code: {e.status_code}, error code: {e.error_code}\nError message: {e.error_msg}")
                df_data_tmp = pd.DataFrame()
            df_data = pd.concat([df_data,df_data_tmp])
            current_start_timestamp = current_end_timestamp
            current_query_times +=1
        # print(df_data)
        # aggregation_functions = {}
        # if StatisticsApproach:
        #     if "last" in StatisticsApproach:
        #         aggregation_functions[f'{rename_field_name}_last'] = (f'{metric_name}', lambda x: x.iloc[x.index.get_loc(x.idxmax())])
        #     if "max" in StatisticsApproach:
        #         aggregation_functions[f'{rename_field_name}_max'] = (f'{metric_name}', 'max')
        #     if "min" in StatisticsApproach:
        #         aggregation_functions[f'{rename_field_name}_min'] = (f'{metric_name}', 'min')
        #     if "avg" in StatisticsApproach:
        #         aggregation_functions[f'{rename_field_name}_avg'] = (f'{metric_name}', 'mean')
        #     if "max_95" in StatisticsApproach:
        #         aggregation_functions[f'{rename_field_name}_max_95'] = (f'{metric_name}', lambda x: x.quantile(0.95))
        #     if "sum" in StatisticsApproach:
        #         aggregation_functions[f'{rename_field_name}_sum'] = (f'{metric_name}', 'sum')
        #     # if "raw" in StatisticsApproach:
        #     #     aggregation_functions[f'{rename_field_name}'] = (f'{metric_name}', 'list')
        #     df_data = df_data.groupby(GroupBy).agg(**aggregation_functions).reset_index()
        # else:
        #     print('Parmas "StatisticsApproach" has contain "raw", return raw data!')
        return df_data
    
