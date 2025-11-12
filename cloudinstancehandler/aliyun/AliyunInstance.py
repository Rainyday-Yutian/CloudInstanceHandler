import json,time

from cloudinstancehandler.common.funcs import *
from cloudinstancehandler.common.BasicDataFrame import *

from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.auth.credentials import AccessKeyCredential
from aliyunsdkcms.request.v20190101.DescribeMetricDataRequest import DescribeMetricDataRequest
from aliyunsdkcms.request.v20190101.DescribeMetricLastRequest import DescribeMetricLastRequest
from aliyunsdkcms.request.v20190101.DescribeMetricListRequest import DescribeMetricListRequest

class AliyunInstance(BasicDataFrame):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__()
        # 兼容性保留，后期版本也许会移除self.credentials，当有实际请求时再传入self.access_key,self.secret_key来构建credentials。
        # 存疑：在编写华为侧模块时貌似遇到过credential构造后具有有效期的问题。这点再阿里云中待考证。
        self.instance_type = self.__class__.__name__
        self.credentials = AccessKeyCredential(access_key, secret_key)
        self.access_key = access_key
        self.secret_key = secret_key
        self.namespace:str = None
        self.ProductCategory = None
        # 当维度只有1时，应该使用DimensionsName这个变量名，Dimensions应该用于表达多维度，后期修改
        # self.DimesionsName = 'instanceId'    
        self.Dimensions:str = 'instanceId'
        self.prefix = 'Aliyun'

    # TimeDict之后可能会改为TimeRange或者TimeRangeDict
    # getMetricList需要兼容原本的二次统计功能
    def getMetricList(self,instance_list:list,metric_name:str,TimeDict:dict,period:str='300',Dimensions:list=None,region_id:str='cn-hangzhou',page_size:int=50,sleep_time=0,field_name='Maximum',statistics_approach:list=['max'],group_by=None,rename_field_name=None) -> pd.DataFrame:
        """
        从阿里云云监控获取指定实例的指标数据，并可选进行二次聚合统计。
        支持跳过统计（返回原始数据）、单列统计、多方法聚合等灵活用法。

        Args:
            instance_list (list): 实例 ID 列表，如 ['i-abc123', 'i-def456']。若 Dimensions 参数已提供，则此参数仅用于 fallback。
            metric_name (str): 指标名称，如 'CPUUtilization'。请参考官方文档确认可用指标：
                            https://cms.console.aliyun.com/metric-meta/acs_ecs_dashboard/ecs
            TimeDict (dict): 时间范围字典，使用 getTimeDict 函数生成。
            period (str): 统计周期（秒），默认 '300'（5分钟）。阿里云支持 60/300/900 等。
            Dimensions (list, optional): 自定义维度列表，格式如 [{'instanceId': 'i-xxx'}, ...]。
                                        若为 None，则根据 instance_list 和 self.Dimensions 自动生成。
            region_id (str): 地域 ID，默认 'cn-hangzhou'。
            page_size (int): 每批次查询的维度数量（阿里云限制最大 50），默认 50。
            sleep_time (float): 批次间休眠时间（秒），用于避免 API 限流，默认 0。
            field_name (str): 原始数据中用于统计的数值列名，默认 'Maximum'。
                            若该列不存在，函数会自动尝试从 ["Value", "Maximum", "Minimum", "Average", "Sum"] 中匹配第一个存在的列。
                            若为None或空字符串，则跳过统计，返回原始数据。
            statistics_approach (list, optional): 统计方法列表。行为如下：
                                                - 未传 → 默认使用 ['max']
                                                - None 或 []（空列表）→ 跳过统计，返回原始数据。
                                                - 支持方法: ['last', 'max', 'min', 'avg', 'max_95', 'sum'] 或 ['all']
                                                    （具体支持方法取决于 self.statisticMetricData 实现）
            group_by (str): 分组字段，如 'instanceId'、'Timestamp' 等。仅在 statistics_approach 非空时生效。
                            若 statistics_approach 为 [] 或 None，此参数被忽略。
            rename_field_name (str, optional): 重命名统计结果列的名称（可选）。

        Returns:
            pd.DataFrame: 结构取决于是否执行统计：
                        - 若 statistics_approach 为 [] → 返回原始监控点（包含 Timestamp, instanceId, Value/Maximum 等列）
                        - 若执行统计 → 返回聚合后结果，列包含 group_by 字段 + 统计结果列（如 max_value, avg_value 等）
        """
        duration_time = TimeDict["end_datetime"] - TimeDict["start_datetime"]
        if duration_time > datetime.timedelta(days=60) or page_size > 50 or page_size < 1:
            warnings.warn(
                f"Duration time {duration_time} exceeds 60 days limit. This may lead to incomplete data or API failure. ",
                UserWarning,
                stacklevel=2
            )
        if not self.namespace:
            raise ValueError("Namespace is not set. Please set Namespace in the subclass.")

        # 自定义Dimensions参数是临时支持的，需要重写，构造Dimensions时，可以参考Dataframe，先铺平
        if Dimensions is None:
            dimensions_list = [{self.Dimensions: instanceId} for instanceId in instance_list]
        else:
            dimensions_list = Dimensions

        client = AcsClient(region_id=region_id, credential=self.credentials)
        request = DescribeMetricListRequest()
        request.set_accept_format('json')
        request.set_Namespace(self.namespace)
        request.set_MetricName(metric_name)
        request.set_Period(period)
        request.set_StartTime(TimeDict["start_timestring"])
        request.set_EndTime(TimeDict["end_timestring"])
        df_data = []
        # Todo: 需要记录日志或者报错
        for idx in range(0, len(dimensions_list),page_size):
            batch_dimensions_list = dimensions_list[idx:idx+page_size]
            request.set_Dimensions(batch_dimensions_list)    
            response_json = json.loads(client.do_action_with_exception(request))
            df_data_clip = pd.DataFrame(json.loads(response_json.get('Datapoints', '[]')))
            df_data.append(df_data_clip)
            time.sleep(sleep_time)

        if df_data:
            df_data = pd.concat(df_data,ignore_index=True)
            # if not df_data.empty:
            #     df_data.sort_values(by=['Timestamp'],inplace=True)
            self.MetricData = df_data
        else:
            df_data = pd.DataFrame()

        # 根据阿里云的云监控表查询，查看具体支持哪些数据列;目前BasicDataFrame.statisticMetricData()仅支持单个数据列的二次统计，后续可考虑支持多列.
        if statistics_approach and field_name and not df_data.empty:
            if rename_field_name is None:
                rename_field_name = metric_name
            Allowed_Field_Name = ["Value","Maximum","Minimum","Average","Sum"]
            df_data_columns_set = set(df_data.columns)
            if field_name not in df_data_columns_set:
                for stat in Allowed_Field_Name:
                    if stat in df_data_columns_set:
                        field_name = stat
                        break
            if 'Timestamp' in df_data.columns:
                df_data.sort_values(by='Timestamp', inplace=True)
            if group_by is None:
                group_by = self.Dimensions
            try:
                df_data = self.statisticMetricData(field_name=field_name,df_data=df_data,statistics_approach=statistics_approach,group_by=group_by,rename_field_name=rename_field_name)
            except Exception as e:
                print(f"Error occurred while calling statisticMetricData: {e}")
        return df_data
        
    def getMetricData(self,instance_list:list,metric_name:str,TimeDict:dict,period:str="300",Dimensions:list=None,region_id:str="cn-hangzhou",page_size:int=50,sleep_time=0,express=None) -> pd.DataFrame:
        """
        该方法使用的API：DescribeMetricData，单个 API 的调用次数限制为 10 次/秒

        说明：
            与 DescribeMetricList 不同，该接口官方原生支持统计功能（即Dimension={"instanceId": "i-abcdefgh12****"}），将该账号下的所有监控数据进行聚合统计
            介于 CloudInstanceHandler 支持了更丰富的统计功能，所以在非特殊情况下，建议优先使用 getMetricList 方法即可；
            本方法 getMetricData 仅作为补充使用，且不再支持二次统计功能，不过仍然可以通过手动调用 self.statisticMetricData 方法进行二次统计）
        """
        duration_time = TimeDict["end_datetime"] - TimeDict["start_datetime"]
        if duration_time > datetime.timedelta(days=60) or page_size > 50 or page_size < 1:
            warnings.warn(
                f"Duration time {duration_time} exceeds 60 days limit. This may lead to incomplete data or API failure. ",
                UserWarning,
                stacklevel=2
            )
        if not self.namespace:
            raise ValueError("Namespace is not set. Please set Namespace in the subclass.")

        # 自定义Dimensions参数是临时支持的，需要重写，构造Dimensions时，可以参考Dataframe，先铺平
        if Dimensions is None:
            dimensions_list = [{self.Dimensions: instanceId} for instanceId in instance_list]
        else:
            dimensions_list = Dimensions

        client = AcsClient(region_id=region_id, credential=self.credentials)
        request = DescribeMetricDataRequest()
        request.set_accept_format('json')
        request.set_Namespace(self.namespace)
        request.set_MetricName(metric_name)
        request.set_Period(period)
        request.set_StartTime(TimeDict["start_timestring"])
        request.set_EndTime(TimeDict["end_timestring"])
        if express is None:
            request.set_Express(f'{{"groupby":[{self.Dimensions},"timestamp"]}}')
        # request.set_Length('1440')
        # 阿里云接口最新不止支持到1440，后续有需求再编写分页查询，参考 generate_timestamps()
        # print(TimeDict['start_timestamp'])
        df_data = pd.DataFrame()
        for idx in range(0, len(dimensions_list),page_size):
            batch_dimensions_list = dimensions_list[idx:idx+page_size]
            request.set_Dimensions(batch_dimensions_list)    
            response_json = json.loads(client.do_action_with_exception(request))
            df_data_temp = pd.DataFrame(json.loads(response_json.get('Datapoints', '[]')))
            df_data = pd.concat([df_data, df_data_temp])
            time.sleep(sleep_time)
        self.MetricData = df_data
        return df_data
    
    def getMetricLast_NextToken(self,region_id,instance_list,metric_name,Period="60",TimeDict=None,rename_field_name=None,page_size=50) -> pd.DataFrame:
        if rename_field_name is None:
            rename_field_name = metric_name
        client = AcsClient(region_id=region_id, credential=self.credentials)
        # request = DescribeMetricListRequest()
        request = DescribeMetricLastRequest()
        request.set_accept_format('json')
        request.set_MetricName(metric_name)
        request.set_Period(Period)
        request.set_StartTime(TimeDict["start_timestring"])
        request.set_EndTime(TimeDict["end_timestring"])
        request.set_Namespace(self.namespace)
        request.set_Express('{"groupby":["userId","instanceId"]}')
        request.set_Length("1000")
        df_data = pd.DataFrame()
        dimensions_list = [{"instanceId": instanceId} for instanceId in instance_list]
        request.set_Dimensions(dimensions_list)
        response = client.do_action_with_exception(request)
        response_json = json.loads(response)
        if response_json['Success']:
            df_data = pd.concat([df_data, pd.DataFrame(json.loads(response_json['Datapoints']))])
            NextToken = response_json.get("NextToken")
            while(NextToken):
                request.set_NextToken(NextToken)
                df_data = pd.concat([df_data, pd.DataFrame(json.loads(response_json['Datapoints']))])
        else:
            pprint(response)
            print("Failed to obtain the data point!Return a empty dataframe!")
            df_data = pd.DataFrame()
        df_data.rename(columns={col: f'{rename_field_name}_{col}' for col in df_data.columns if col not in ['instanceId','timestamp','userId']}, inplace=True)
        return df_data
    
    # 子类自行实现适配
    # def getInsInfo():
    #     pass

    # def getInsData():
    #     pass

# 待办：需要优化，以最大长度为参数，生成时间戳
# def generate_timestamps(start_timestamp, end_timestamp, period, max_length, instance_count):
#     pass
#     generate_timestamps函数用于生成分页查询的时间戳，以下代码由老通义千问生成，仅作参考，后续会完善
#     total_query_range = end_timestamp - start_timestamp
#     query_range_per_instance = (max_length // instance_count) * period
#     page_count = -(-total_query_range // query_range_per_instance)  # 向上取整

#     timestamps = []
#     current_start = start_timestamp
#     for _ in range(page_count):
#         current_end = min(current_start + query_range_per_instance, end_timestamp)
#         timestamps.append((current_start, current_end))
#         current_start = current_end

#     return timestamps

#     # 示例参数
#     start_timestamp = int(datetime.datetime(2023, 10, 1, 0, 0, 0).timestamp())
#     end_timestamp = int(datetime.datetime(2023, 10, 2, 0, 0, 0).timestamp())
#     period = 300  # 5分钟
#     max_length = 1440
#     instance_count = 50

#     # 生成分页查询的时间戳
#     timestamps = generate_timestamps(start_timestamp, end_timestamp, period, max_length, instance_count)

#     # 打印分页查询的时间戳
#     for i, (start, end) in enumerate(timestamps):
#         print(f"Page {i+1}: Start Timestamp: {start}, End Timestamp: {end}")