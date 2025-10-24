
from cloudinstancehandler.common.funcs import *
from cloudinstancehandler.common.BasicDataFrame import *

import volcenginesdkcore
import volcenginesdkvolcobserve
from volcenginesdkcore.rest import ApiException

class VolcInstance(BasicDataFrame):
    def __init__(self,ak,sk):
        super().__init__()
        self.InsType = self.__class__.__name__
        self.AK = ak
        self.SK = sk
        self.Namespace:str = None
        self.SubNamespace:str = None
        self.Dimensions:str = "ResourceID"
    
    # def getMetricData(self,region_id,instance_id_list:list,metric_name:str,TimeDict:object,period:str="5m",StatisticsApproach:list=['max'],GroupBy:str='id',DisplayMetricName=None) -> pd.DataFrame:
    def getMetricData(self,instance_id_list:list,metric_name:str,TimeDict:object,period:str="5m",groupby:str=None,StatisticsApproach:list=['max'],GroupBy:str="ResourceID",DisplayMetricName:str=None,region_id:str="cn-shanghai",page_size:int=200) -> pd.DataFrame:
        """
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

        namesapce | metric_name | sub_namespace: 
        官方查询地址https://console.volcengine.com/cloud_monitor/docs
        """
        # 参数合法性检查及初始化
        Dimensions_List:list = self.Dimensions.split(',')
        if GroupBy == 'ResourceID':
            GroupBy = self.Dimensions
        if GroupBy not in ['timestamp',None]+Dimensions_List:
            raise ValueError(f"Invalid GroupBy value '{GroupBy}'. Allowed values are: {', '.join([self.Dimensions,'timestamp','obj:None'])}")

        allowed_StatisticsApproach = ['last', 'max', 'min', 'avg', 'max_95','all','raw']
        if 'all' in StatisticsApproach:
            StatisticsApproach = allowed_StatisticsApproach[:-1]
        elif 'raw' in StatisticsApproach or StatisticsApproach == [] or StatisticsApproach is None:
            StatisticsApproach = None
        elif not set(StatisticsApproach).issubset(set(allowed_StatisticsApproach)):
            raise ValueError(f"Invalid StatisticsApproach value(s): {set(StatisticsApproach) - set(allowed_StatisticsApproach)}. Allowed values are: {', '.join(allowed_StatisticsApproach)}")
        
        if DisplayMetricName is None:
            DisplayMetricName = metric_name

        # 请求初始化
        configuration = volcenginesdkcore.Configuration()
        configuration.ak = self.AK
        configuration.sk = self.SK
        configuration.region = region_id

        instances = []
        for instance_id in instance_id_list:
            instances.append(
                volcenginesdkvolcobserve.InstanceForGetMetricDataInput(
                    dimensions=[
                        volcenginesdkvolcobserve.DimensionForGetMetricDataInput(
                            name=self.Dimensions,
                            value=instance_id,
                        )
                    ]
                )
            )
        df_data = pd.DataFrame()
        try:
            api_instance = volcenginesdkvolcobserve.VOLCOBSERVEApi(volcenginesdkcore.ApiClient(configuration))
            response = api_instance.get_metric_data(volcenginesdkvolcobserve.GetMetricDataRequest(
                start_time=TimeDict['start_timestamp'], end_time=TimeDict['end_timestamp'], period=period,
                namespace=self.Namespace, sub_namespace=self.SubNamespace,metric_name=metric_name,
                instances=instances
            ))
            response_json = response.to_dict()
            for ins_data in response_json.get('data').get('metric_data_results'):
                df_tmp = pd.DataFrame(ins_data.get('data_points'))
                for dimension in ins_data.get('dimensions'):
                    df_tmp[dimension.get('name')] = dimension.get('value')
                df_data = pd.concat([df_data, df_tmp])    
        except ApiException as e:
            print("\nError: Exception when calling GetMetricData: %s\n" % e)
            return df_data

        # 沿用阿里云的方法，由于火山云接口没有统计一说，即没有statistics参数，返回数据统一命名为value
        statistic = "value"
        aggregation_functions = {}
        if StatisticsApproach:
            if "last" in StatisticsApproach:
                aggregation_functions[f'{DisplayMetricName}_last'] = (f'{statistic}', lambda x: x.iloc[x.index.get_loc(x.idxmax())])
            if "max" in StatisticsApproach:
                aggregation_functions[f'{DisplayMetricName}_max'] = (f'{statistic}', 'max')
            if "min" in StatisticsApproach:
                aggregation_functions[f'{DisplayMetricName}_min'] = (f'{statistic}', 'min')
            if "avg" in StatisticsApproach:
                aggregation_functions[f'{DisplayMetricName}_avg'] = (f'{statistic}', 'mean')
            if "max_95" in StatisticsApproach:
                aggregation_functions[f'{DisplayMetricName}_max_95'] = (f'{statistic}', lambda x: x.quantile(0.95))
            if "sum" in StatisticsApproach:
                aggregation_functions[f'{DisplayMetricName}_sum'] = (f'{statistic}', 'sum')
            # if "raw" in StatisticsApproach:
            #     aggregation_functions[f'{DisplayMetricName}'] = (f'{metric_name}', 'list')
            if GroupBy:
                df_data = df_data.groupby(GroupBy).agg(**aggregation_functions).reset_index()
            else:
                df_data = df_data.agg(**aggregation_functions).reset_index()
        else:
            print('Parmas StatisticsApproach has contain "raw", return raw data!')
        return df_data

    def getInsInfo(self):
        pass