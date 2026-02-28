# cloudinstacehandler/aliyun/TencentInstance.py
import json,time

from cloudinstancehandler.common.funcs import *
from cloudinstancehandler.common.BasicDataFrame import *

from tencentcloud.common import credential
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.monitor.v20180724 import monitor_client, models



class TencentInstance(BasicDataFrame):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__()
        # 兼容性保留，后期版本也许会移除self.credentials，当有实际请求时再传入self.access_key,self.secret_key来构建credentials。
        # 存疑：在编写华为侧模块时貌似遇到过credential构造后具有有效期的问题。这点再阿里云中待考证。
        self.instance_type = self.__class__.__name__
        self.credentials = credential.Credential(access_key, secret_key)
        self.access_key = access_key
        self.secret_key = secret_key
        self.namespace:str = None
        self.ProductCategory = None
        # 仅用于单一维度查询，如实例ID
        # self.DimesionsName = 'instanceId'
        self.Dimensions:str = 'instanceId'
        self.prefix = 'Aliyun'
        
    def getMetricsData(self,metric_name:str,TimeDict:dict,period:str='300',dimensions_df:pd.DataFrame=None,instance_list:list=None,region_id:str='ap-guangzhou',page_size:int=50,sleep_time=0,field_name='Maximum',statistics_approach:list=['max'],group_by=None,rename_field_name=None) -> pd.DataFrame:
        duration_time = TimeDict["end_datetime"] - TimeDict["start_datetime"]
        if duration_time > datetime.timedelta(days=60) or page_size > 50 or page_size < 1:
            warnings.warn(
                f"Duration time {duration_time} exceeds 60 days limit. This may lead to incomplete data or API failure. ",
                UserWarning,
                stacklevel=2
            )
        if not self.namespace:
            raise ValueError("Namespace is not set. Please set Namespace in the subclass.")
        
        if dimensions_df is not None and not dimensions_df.empty:
            # dimensions_list = []
            # for _, row in dimensions_df.iterrows():
            #     dims = []
            #     for col, value in row.items():
            #         dims.append({
            #             "Name": col,
            #             "Value": value
            #         })
            # dimensions_list.append({"Dimensions": dims})
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
            pprint(dimensions_list)
        elif instance_list is not None:
            dimensions_list = [{"Dimensions": [{"Name": self.Dimensions, "Value": InstanceID}]} for InstanceID in instance_list]
        # elif self.InsInfo:
        #     dimensions_list = self.InsInfo[[ID_Field_Name]].rename({ID_Field_Name: self.Dimensions}).to_dict(orient='records')
        else:
            raise ValueError("Either dimensions_df or instance_list must be provided.")
        
        httpProfile = HttpProfile()
        httpProfile.endpoint = "monitor.tencentcloudapi.com"
        clientProfile = ClientProfile()
        clientProfile.httpProfile = httpProfile
        client = monitor_client.MonitorClient(self.credentials,region_id, clientProfile)

        # 实例化一个请求对象,每个接口都会对应一个request对象
        req = models.GetMonitorDataRequest()
        params = {
            "Namespace": self.namespace,
            "MetricName": metric_name,
            "Period": period,
            "StartTime": TimeDict['start_timestring_iso8601'],
            "EndTime": TimeDict['end_timestring_iso8601'],
            "Instances": dimensions_list,
        }
        
        req.from_json_string(json.dumps(params))
        resp_str = client.GetMonitorData(req).to_json_string()
        response_json = json.loads(resp_str)
        pprint(response_json)
        # df = genDF_newest(response_json)
        # df['Value'] = df['Value'] * 1e6
        # except TencentCloudSDKException as err:
        #     print(err)
        #     df = pd.DataFrame()
        # return df