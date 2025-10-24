import json,requests,time
from urllib.parse import quote
import hashlib,hmac

from cloudinstancehandler.volcengine.VolcInstance import *

import volcenginesdkecs,volcenginesdkredis,volcenginesdkrdsmysqlv2,volcenginesdkclb,volcenginesdkvpc
from volcenginesdkcore.rest import ApiException

# 抽象出来的通用请求类，用于未适配SDK的接口，后续代码过长时考虑拆分
class VCMServiceCommonRequest():
    def __init__(self, ak, sk, region, host, method, service, action, version, content_type="application/x-www-form-urlencoded",debug=False):
        self.AK = ak
        self.SK = sk
        self.Region = region
        self.Host = host
        self.Method = method
        self.ContentType = content_type
        self.Service = service
        self.Action = action
        self.Version = version
        self.Query = {}
        self.Headers = {}
        self.Body = {}
        self.Debug = debug
    
    # def set_query_param(self,key,value):
    #     self.RequestParams[key] = value

    def norm_query(self,params):
        query = ""
        for key in sorted(params.keys()):
            if type(params[key]) == list:
                for k in params[key]:
                    query = (
                            query + quote(key, safe="-_.~") + "=" + quote(k, safe="-_.~") + "&"
                    )
            else:
                query = (query + quote(key, safe="-_.~") + "=" + quote(params[key], safe="-_.~") + "&")
        # print(query)
        query = query[:-1]
        return query.replace("+", "%20")


    # 第一步：准备辅助函数。
    # sha256 非对称加密
    def hmac_sha256(self,key: bytes, content: str):
        return hmac.new(key, content.encode("utf-8"), hashlib.sha256).digest()
    
    # sha256 hash算法
    def hash_sha256(self,content: str):
        return hashlib.sha256(content.encode("utf-8")).hexdigest()
    
    # 第二步：签名请求函数
    def request(self):
        # 第三步：创建身份证明。其中的 Service 和 Region 字段是固定的。ak 和 sk 分别代表
        # AccessKeyID 和 SecretAccessKey。同时需要初始化签名结构体。一些签名计算时需要的属性也在这里处理。
        # 初始化身份证明结构体
        credential = {
            "access_key_id": self.AK,
            "secret_access_key": self.SK,
            "service": self.Service,
            "region": self.Region,
        }
        # 初始化签名结构体
        request_param = {
            "body": self.Body,
            "host": self.Host,
            "path": "/",
            "method": self.Method,
            "content_type": self.ContentType,
            "date": datetime.datetime.utcnow(),
            "query": {"Action": self.Action, "Version": self.Version, **self.Query},
        }
        if self.Body is None:
            request_param["body"] = ""
        # 第四步：接下来开始计算签名。在计算签名前，先准备好用于接收签算结果的 signResult 变量，并设置一些参数。
        # 初始化签名结果的结构体
        x_date = request_param["date"].strftime("%Y%m%dT%H%M%SZ")
        short_x_date = x_date[:8]
        x_content_sha256 = self.hash_sha256(request_param["body"])
        sign_result = {
            "Host": request_param["host"],
            "X-Content-Sha256": x_content_sha256,
            "X-Date": x_date,
            "Content-Type": request_param["content_type"],
        }
        # 第五步：计算 Signature 签名。
        signed_headers_str = ";".join(
            ["content-type", "host", "x-content-sha256", "x-date"]
        )
        # signed_headers_str = signed_headers_str + ";x-security-token"
        canonical_request_str = "\n".join(
            [request_param["method"].upper(),
            request_param["path"],
            self.norm_query(request_param["query"]),
            "\n".join(
                [
                    "content-type:" + request_param["content_type"],
                    "host:" + request_param["host"],
                    "x-content-sha256:" + x_content_sha256,
                    "x-date:" + x_date,
                ]
            ),
            "",
            signed_headers_str,
            x_content_sha256,
            ]
        )

        hashed_canonical_request = self.hash_sha256(canonical_request_str)

        credential_scope = "/".join([short_x_date, credential["region"], credential["service"], "request"])
        string_to_sign = "\n".join(["HMAC-SHA256", x_date, credential_scope, hashed_canonical_request])

        if self.Debug:
            # 打印正规化的请求用于调试比对
            print("正规化的请求:",canonical_request_str)
            # 打印hash值用于调试比对
            print("hash值:",hashed_canonical_request)
            # 打印最终计算的签名字符串用于调试比对
            print("最终计算的签名字符串",string_to_sign)

        k_date = self.hmac_sha256(credential["secret_access_key"].encode("utf-8"), short_x_date)
        k_region = self.hmac_sha256(k_date, credential["region"])
        k_service = self.hmac_sha256(k_region, credential["service"])
        k_signing = self.hmac_sha256(k_service, "request")
        signature = self.hmac_sha256(k_signing, string_to_sign).hex()

        sign_result["Authorization"] = "HMAC-SHA256 Credential={}, SignedHeaders={}, Signature={}".format(
            credential["access_key_id"] + "/" + credential_scope,
            signed_headers_str,
            signature,
        )
        header = {**self.Headers, **sign_result}
        # header = {**header, **{"X-Security-Token": SessionToken}}
        # 第六步：将 Signature 签名写入 HTTP Header 中，并发送 HTTP 请求。
        r = requests.request(method=self.Method,
                            url="https://{}{}".format(request_param["host"], request_param["path"]),
                            headers=header,
                            params=request_param["query"],
                            data=request_param["body"],
                            )
        return r.json()

# -------------------------------------- 具体类 ---------------------------------------- #
# 具体请求类实现
# 对应文档中的 SearchResources 接口，用于搜索资源（获取所有资源）
class VCMSearchResourcesRequest(VCMServiceCommonRequest):
    def __init__(self,ak,sk,filter=None,sort_by=None,sort_order=None,max_results=100,next_token=None,action="SearchResources",version="2023-06-01"):
        super().__init__(ak,sk,region="cn-north-1",host="open.volcengineapi.com",method="POST",service="resourcecenter",action=action,version=version,content_type="application/json")
        self.MaxResults = max_results
        self.Filter = filter
        self.SortBy = sort_by
        self.SortOrder = sort_order
        self.NextToken = next_token
        self.Response = None

    def do_request(self):
        params_dict = {
            'MaxResults': self.MaxResults,
            'Filter': self.Filter,
            'SortBy': self.SortBy,
            'SortOrder': self.SortOrder,
            'NextToken': self.NextToken
        }
        
        params_dict = {k: v for k, v in params_dict.items() if v is not None}
        if self.Debug:
            print("请求参数：")
            pprint(self.Body)
        self.Body = json.dumps(params_dict)
        self.Response = self.request()
        if self.Debug:
            print("完整响应：")
            pprint(self.Response)
        return self.Response.get("Result", None)
    
# 对应文档中的 GetResourceCounts 接口，用于获取现存所有资源类型
class VCMGetResourceCountsRequest(VCMServiceCommonRequest):
    def __init__(self,ak,sk,filter=None,group_by_key=None,action="GetResourceCounts",version="2023-06-01"):
        super().__init__(ak,sk,region="cn-north-1",host="open.volcengineapi.com",method="POST",service="resourcecenter",action=action,version=version,content_type="application/json")
        self.Filter = filter
        self.GroupByKey = group_by_key
    
    def do_request(self):
        if self.GroupByKey not in ["ResourceType","Region"]:
            raise ValueError("GroupByKey must be one of ['ResourceType','Region']")
        params_dict = {
            'Filter': self.Filter,
            'GroupByKey': self.GroupByKey
        }
        
        params_dict = {k: v for k, v in params_dict.items() if v is not None}
        if self.Debug:
            print("请求参数：")
            pprint(self.Body)
        self.Body = json.dumps(params_dict)
        self.Response = self.request()
        if self.Debug:
            print("完整响应：")
            pprint(self.Response)
        return self.Response.get("Result", None)

# -------------------------------------- 实例类  ---------------------------------------- #
class ECS(VolcInstance):
    def __init__(self,ak,sk):
        super().__init__(ak,sk)
        self.Namespace = "VCM_ECS"
        self.SubNamespace = "Instance"
        self.SpecInfo:pd.DataFrame = pd.DataFrame()

    def getInsInfo(self,region,instance_id_list:list=None,instance_name=None,eip_addresses_list:list=None,primary_ip_address:str=None,instance_charge_type:str="PrePaid",page_size=100):
        configuration = volcenginesdkcore.Configuration()
        configuration.ak = self.AK
        configuration.sk = self.SK
        configuration.region = region
        volcenginesdkcore.Configuration.set_default(configuration)
        api_instance = volcenginesdkecs.ECSApi()
        describe_instances_request = volcenginesdkecs.DescribeInstancesRequest(
            max_results=page_size,
        )

        if instance_id_list:
            describe_instances_request.instance_ids = instance_id_list
        if instance_name:
            describe_instances_request.instance_name = instance_name
        if instance_charge_type:
            if instance_charge_type not in ["PrePaid","PostPaid"]:
                raise ValueError("instance_charge_type must be 'PrePaid' or 'PostPaid'")
            describe_instances_request.instance_charge_type = instance_charge_type
        if eip_addresses_list:
            describe_instances_request.eip_addresses = eip_addresses_list

        next_token = True
        df_data = pd.DataFrame()
        while next_token:
            try:
                describe_instances_request.next_token = next_token
                response = api_instance.describe_instances(describe_instances_request)
                df_data_tmp = pd.DataFrame(response.to_dict().get('instances'))
                next_token = response.next_token
                df_data = pd.concat([df_data,df_data_tmp],ignore_index=True)
                # if not next_token:
                #     break
            except ApiException as e:
                print("Exception occurred while fetching instances: %s\n" % e)
                break   
        # 返回的数据没有region_id，手动添加
        df_data['region_id'] = region
        self.InsInfo = pd.concat([self.InsInfo,df_data],axis=0,ignore_index=True)
        return df_data

    def getSpecInfo(self,region="cn-beijing",instance_type_ids_list:list=None,merge=True):
        configuration = volcenginesdkcore.Configuration()
        configuration.ak = self.AK
        configuration.sk = self.SK
        configuration.region = region
        volcenginesdkcore.Configuration.set_default(configuration)
        api_instance = volcenginesdkecs.ECSApi()
        describe_instance_types_request = volcenginesdkecs.DescribeInstanceTypesRequest()

        # 由于文档说明不够详细，因此当查询的instance_type_ids长度小于等于100时，直接查询，大于100时，直接查询所有规格信息
        # 后续再优化成自行分页来优先保证可行性，初步设计是再套一层for在while上，用page_size在instance_type_ids中滚动查询；
        # 如果self.InsInfo为空，instance_type_ids只有一个None元素用于获取所有规格信息
        # 如果self.InsInfo不为空，优先查InsInfo的规格信息，此时instance_type_ids_list即使指定有值，也不会生效
        if not self.InsInfo.empty:
            if len(self.InsInfo['instance_type_id'].unique().tolist()) <=100:
                describe_instance_types_request.max_results = 100
                describe_instance_types_request.instance_type_ids = self.InsInfo['instance_type_id'].unique().tolist()
            else:
                describe_instance_types_request.max_results = 1000
        elif instance_type_ids_list:
            if len(instance_type_ids_list) <=100:
                describe_instance_types_request.max_results = 100
                describe_instance_types_request.instance_type_ids = self.InsInfo['instance_type_id'].unique().tolist()
            else:
                raise Exception('instance_type_ids_list length must be less than 100')
        else:
            raise Exception('One of InsInfo or instance_type_ids_list must be not empty')
        next_token = True
        df_data = pd.DataFrame()
        while next_token:
            try:
                describe_instance_types_request.next_token = next_token
                response = api_instance.describe_instance_types(describe_instance_types_request)
                next_token = response.next_token
                df_tmp = pd.DataFrame(response.to_dict().get('instance_types'))
                df_data = pd.concat([df_data,df_tmp])
            except ApiException as e:
                print("Exception when calling api: %s\n" % e)
                pass
        self.SpecInfo = df_data
        normalized_df = pd.json_normalize(self.SpecInfo['network'])
        prefix = "network_"
        normalized_df.columns = [f"{prefix}{col}" for col in normalized_df.columns]
        self.SpecInfo = self.SpecInfo.join(normalized_df)
        if merge and not self.InsInfo.empty:
            self.InsInfo = pd.merge(self.InsInfo,self.SpecInfo,on='instance_type_id',how='left')
        return df_data

class RDS(VolcInstance):
    def __init__(self,ak,sk):
        super().__init__(ak,sk)
        self.Namespace = "VCM_RDS_MySQL"
        self.SubNamespace = "resource_monitor"

    def getRegions(self,raw_data=False)-> pd.DataFrame:
        configuration = volcenginesdkcore.Configuration()
        configuration.ak = self.AK
        configuration.sk = self.SK
        configuration.region = "cn-beijing"
        volcenginesdkcore.Configuration.set_default(configuration)
        api_instance = volcenginesdkrdsmysqlv2.RDSMYSQLV2Api()
        describe_regions_request = volcenginesdkrdsmysqlv2.DescribeRegionsRequest(
        )
        regions_list_default_data = [{'region_id': 'cn-beijing', 'region_name': '华北2（北京）'},
        {'region_id': 'cn-shanghai', 'region_name': '华东2（上海）'},
        {'region_id': 'cn-guangzhou', 'region_name': '华南1（广州）'},
        {'region_id': 'cn-hongkong', 'region_name': '中国香港'},
        {'region_id': 'ap-southeast-1', 'region_name': '亚太东南（柔佛）'}]
        try:
            response = api_instance.describe_regions(describe_regions_request)
            regions_list_data  = response.to_dict().get('regions', regions_list_default_data)
        except ApiException as e:
            print("Exception when calling api: %s\n" % e)
            regions_list_data  = regions_list_default_data
        if raw_data:
            return regions_list_data
        return pd.DataFrame(regions_list_data)

    def getInsInfo(self,region,page_size=100):
        configuration = volcenginesdkcore.Configuration()
        configuration.ak = self.AK
        configuration.sk = self.SK
        configuration.region = region
        volcenginesdkcore.Configuration.set_default(configuration)
        api_instance = volcenginesdkrdsmysqlv2.RDSMYSQLV2Api()
        describe_db_instances_request = volcenginesdkrdsmysqlv2.DescribeDBInstancesRequest(
            page_size=page_size
        )
        page_number = 1
        has_next_page = True
        df_data = pd.DataFrame()
        while has_next_page:
            describe_db_instances_request.page_number = page_number
            try:
                response = api_instance.describe_db_instances(describe_db_instances_request)
                df_data_tmp = pd.DataFrame(response.to_dict().get("instances"))
                df_data = pd.concat([df_data,df_data_tmp])
                if response.total <= page_number * describe_db_instances_request.page_size:
                    has_next_page = False
                else:
                    page_number += 1
            except ApiException as e:
                print("\nException when calling api: %s\n" % e)
                break
            except Exception as e:
                print(f"\nAn unexpected error occurred: {e}\n")
                break
        self.InsInfo = pd.concat([self.InsInfo,df_data],axis=0,ignore_index=True)
        return df_data
    
    # 原设计兼容手动指定实例ID，和自动获取InsInfo中实例ID，后取消此设计
    # def getInsDetailsInfo(self,region,instance_id_list=None):
    #     if not self.InsInfo.empty:
    #                 instance_id_list = self.InsInfo['InstanceId'].unique().tolist()
    #             elif instance_id_list:
    #                 pass
    #             else:
    #                 raise ValueError("One of InsInfo or instance_id_list must be not empty.")
    # 新版还未写完详细信息的解析部分
    def getInsDetailsInfo(self,merge=True):
        if self.InsInfo.empty:
            raise ValueError("InsInfo is empty.Please call getInsInfo first.")
        # df_data = pd.DataFrame()
        list_data = []
        for region in self.InsInfo['region_id'].unique().tolist():
            configuration = volcenginesdkcore.Configuration()
            configuration.ak = self.AK
            configuration.sk = self.SK
            configuration.region = region
            volcenginesdkcore.Configuration.set_default(configuration)
            api_instance = volcenginesdkrdsmysqlv2.RDSMYSQLV2Api()
            try:
                for instance_id in self.InsInfo[self.InsInfo['region_id'] == region]['instance_id'].unique().tolist():
                    describe_db_instance_detail_request = volcenginesdkrdsmysqlv2.DescribeDBInstanceDetailRequest(instance_id=instance_id)
                    response = api_instance.describe_db_instance_detail(describe_db_instance_detail_request)
                    response_json = response.to_dict()
                    response_json['instance_id'] = response_json['basic_info']['instance_id']
                    list_data.append(response_json)
            except ApiException as e:
                print("Exception when calling api: %s\n" % e)
        df_data = pd.DataFrame(list_data)
        if merge:
            self.InsInfo = pd.merge(self.InsInfo, df_data, on='instance_id', how='left')
        return df_data
    
class Redis(VolcInstance):
    def __init__(self,ak,sk):
        super().__init__(ak,sk)
        self.Namespace = "VCM_Redis"
        self.SubNamespace = "aggregated_server"

    def getInsInfo(self,region,page_size=100):
        configuration = volcenginesdkcore.Configuration()
        configuration.ak = self.AK
        configuration.sk = self.SK
        configuration.region = region
        volcenginesdkcore.Configuration.set_default(configuration)
        api_instance = volcenginesdkredis.REDISApi()

        page_number = 1
        has_next_page = True
        describe_db_instances_request = volcenginesdkredis.DescribeDBInstancesRequest(
            page_number = page_number,
            region_id = region,
            page_size = page_size
        )
        df_data = pd.DataFrame()
        while has_next_page:
            describe_db_instances_request.page_number = page_number
            try:
                response = api_instance.describe_db_instances(describe_db_instances_request)
                df_data_tmp = pd.DataFrame(response.to_dict().get("instances"))
                df_data = pd.concat([df_data,df_data_tmp])
                if response.total_instances_num <= page_number * describe_db_instances_request.page_size:
                    has_next_page = False
                else:
                    page_number += 1
            except ApiException as e:
                print("\nException when calling api: %s\n" % e)
                break
            except Exception as e:
                print(f"\nAn unexpected error occurred: {e}\n")
                break
        self.InsInfo = pd.concat([self.InsInfo,df_data],axis=0,ignore_index=True)
        return df_data
        
class CLB(VolcInstance):
    def __init__(self,ak,sk):
        super().__init__(ak,sk)
        self.Namespace = "VCM_CLB"
        self.SubNamespace = "loadbalancer"
        self.billing_type_map = {
            1: "包年包月",
            2: "按量计费"
        }
    
    def getInsInfo(self,region,instance_id_list:list=None,instance_name:str=None,eip_address:str=None,page_size=100):
        configuration = volcenginesdkcore.Configuration()
        configuration.ak = self.AK
        configuration.sk = self.SK
        configuration.region = region
        volcenginesdkcore.Configuration.set_default(configuration)
        api_instance = volcenginesdkclb.CLBApi()
        describe_load_balancers_request = volcenginesdkclb.DescribeLoadBalancersRequest(
            page_size=page_size
        )
        if instance_id_list:
            describe_load_balancers_request.load_balancer_ids = instance_id_list
        if instance_name:
            describe_load_balancers_request.load_balancer_name = instance_name
        if eip_address:
            describe_load_balancers_request.eip_address = eip_address

        page_number = 1
        has_next_page = True        
        df_data = pd.DataFrame()
        while has_next_page:
            describe_load_balancers_request.page_number = page_number
            try:
                response = api_instance.describe_load_balancers(describe_load_balancers_request)
                df_data_tmp = pd.DataFrame(response.to_dict().get("load_balancers"))
                df_data = pd.concat([df_data,df_data_tmp])
                # if response.total_count <= (page_number - 1) * describe_load_balancers_request.page_size + len(response.load_balancers):
                if response.total_count <= page_number * describe_load_balancers_request.page_size:
                    has_next_page = False
                else:
                    page_number += 1
            except ApiException as e:
                print("\nException when calling api: %s\n" % e)
                break
            except Exception as e:
                print(f"\nAn unexpected error occurred: {e}\n")
                break 
        if not df_data.empty:
            df_data['billing_type_text'] = df_data['load_balancer_billing_type'].map(self.billing_type_map)
            df_data['region_id'] = region
        self.InsInfo = pd.concat([self.InsInfo,df_data],axis=0,ignore_index=True)
        return df_data
    
class BandwidthPackage(VolcInstance):
    def __init__(self,AK,SK):
        super().__init__(AK,SK)
        self.Namespace = "VCM_BandwidthPackage"
        self.SubNamespace = "instance"
        self.EIPsFromAllCBWP = pd.DataFrame()
        
    def getInsInfo(self,region,instance_id_list=None,page_size=100):
        configuration = volcenginesdkcore.Configuration()
        configuration.ak = self.AK
        configuration.sk = self.SK
        configuration.region = region
        volcenginesdkcore.Configuration.set_default(configuration)
        api_instance = volcenginesdkvpc.VPCApi()
        describe_bandwidth_packages_request = volcenginesdkvpc.DescribeBandwidthPackagesRequest(
            page_size = page_size
        )

        if instance_id_list:
            describe_bandwidth_packages_request.bandwidth_package_ids = instance_id_list

        page_number = 1
        has_next_page = True        
        df_data = pd.DataFrame()
        while has_next_page:
            describe_bandwidth_packages_request.page_number = page_number
            try:
                response = api_instance.describe_bandwidth_packages(describe_bandwidth_packages_request)
                df_data_tmp = pd.DataFrame(response.to_dict().get("bandwidth_packages"))
                df_data = pd.concat([df_data,df_data_tmp])
                if response.total_count <= page_number * describe_bandwidth_packages_request.page_size:
                    has_next_page = False
                else:
                    page_number += 1
            except ApiException as e:
                print("\nException when calling api: %s\n" % e)
                break
            except Exception as e:
                print(f"\nAn unexpected error occurred: {e}\n")
                break
        if not df_data.empty:
            df_data['region_id'] = region
        self.InsInfo = pd.concat([self.InsInfo,df_data],axis=0,ignore_index=True)
        return df_data

    def extractEIPFromInsInfo(self):
        if self.InsInfo.empty:
            print("No data in InsInfo,Please call getBandwidthPackages first!")
            return None
        df_data = pd.DataFrame()
        for index, row in self.InsInfo.iterrows():
            df_data_tmp = pd.DataFrame(row["eip_addresses"])
            df_data_tmp['bandwidth_package_id'] = row['bandwidth_package_id']
            df_data_tmp['region_id'] = row['region_id']
            df_data_tmp['bandwidth(Mbps)'] = row['bandwidth']
            df_data = pd.concat([df_data,df_data_tmp])
        self.EIPsFromAllCBWP = df_data
        return df_data
    
    def extractEIPFromSingleCBWP(self,eips_address)->pd.DataFrame:
        df_data = pd.DataFrame(eips_address)
        return df_data
    
    def getEIPBandwidthRank(self,TimeDict,flow_direction='out',more_info=True,display_size=30):
        if flow_direction == "out":
            metric_name = "OutBPS"
        elif flow_direction == "in":
            metric_name = "InBPS"
        EIPForBwRank = EIP(self.AK,self.SK)
        df_data = pd.DataFrame()
        for index,row in self.InsInfo.iterrows():
            df_eips = pd.DataFrame(row["eip_addresses"])
            df_eips['region_id'] = row['region_id']
            df_eips['bandwidth(Mbps)'] = row['bandwidth']
            instance_list = df_eips['allocation_id'].tolist()
            df_data_tmp = EIPForBwRank.getMetricData(instance_id_list=instance_list, metric_name=metric_name,TimeDict=TimeDict,period='1m',DisplayMetricName="查时速率(bps)")
            df_offset_tmp = EIPForBwRank.getMetricData(instance_id_list=instance_list, metric_name=metric_name,TimeDict=getTimeDict(start_datetime=TimeDict['start_datetime']-datetime.timedelta(days=1,minutes=5),end_datetime=TimeDict['end_datetime']-datetime.timedelta(days=1)),period='1m',DisplayMetricName="同比速率(bps)")
            df_data = pd.concat([df_data,pd.merge(df_data_tmp,df_offset_tmp,on=['ResourceID'],how='left')])
        df_data.rename(columns={'ResourceID':'allocation_id'},inplace=True)
        df_data = pd.merge(df_eips,df_data,how='left',on='allocation_id')
        df_data['查时(Mbps)'] = df_data['查时速率(bps)_max'].div(1000**2).round(2)
        df_data["同比(Mbps)"] = df_data["同比速率(bps)_max"].div(1000**2).round(2)
        df_data['差值(Mbps)'] = df_data['查时(Mbps)'] - df_data['同比(Mbps)']
        df_data['占比(%)'] = ((df_data['查时(Mbps)'] / df_data['bandwidth(Mbps)'])*100).round(2)
        df_data = df_data.sort_values(by=['查时(Mbps)'],ascending=False).reset_index(drop=True)
        # if more_info:
            # EIPForBwRank.getBindedInfo()
        return df_data
    
class EIP(VolcInstance):
    def __init__(self,AK,SK):
        super().__init__(AK,SK)
        self.Namespace = "VCM_EIP"
        self.SubNamespace = "Instance"

    def getInsInfo(self,region,instance_id_list=None,eip_address_list=None,status:str=None,page_size=100):
        configuration = volcenginesdkcore.Configuration()
        configuration.ak = self.AK
        configuration.sk = self.SK
        configuration.region = region
        volcenginesdkcore.Configuration.set_default(configuration)
        api_instance = volcenginesdkvpc.VPCApi()
        describe_eip_addresses_request = volcenginesdkvpc.DescribeEipAddressesRequest(
            page_size = page_size,
        )

        if status:
            describe_eip_addresses_request.status = status

        df_data = pd.DataFrame()
        # 区分指定查询和全部查询
        # Handle specific queries with instance_id_list or eip_address_list
        if instance_id_list or eip_address_list:
            items_to_query = instance_id_list if instance_id_list else eip_address_list
            request_attr = 'allocation_ids' if instance_id_list else 'eip_addresses'

            for idx in range(0, len(items_to_query), page_size):
                try:
                    batch_list = items_to_query[idx:idx + page_size]
                    setattr(describe_eip_addresses_request, request_attr, batch_list)
                    response = api_instance.describe_eip_addresses(describe_eip_addresses_request)
                    df_data_tmp = pd.DataFrame(response.to_dict().get("eip_addresses", []))
                    df_data = pd.concat([df_data, df_data_tmp], ignore_index=True)
                except ApiException as e:
                    print(f"\nException when calling API: {e}\n")
                    break
                except Exception as e:
                    print(f"\nAn unexpected error occurred: {e}\n")
                    break
        else:
            page_number = 1
            has_next_page = True        
            while has_next_page:
                describe_eip_addresses_request.page_number = page_number
                try:
                    response = api_instance.describe_eip_addresses(describe_eip_addresses_request)
                    df_data_tmp = pd.DataFrame(response.to_dict().get("eip_addresses"))
                    df_data = pd.concat([df_data,df_data_tmp])
                    if response.total_count <= page_number * describe_eip_addresses_request.page_size:
                        has_next_page = False
                    else:
                        page_number += 1
                except ApiException as e:
                    print("\nException when calling api: %s\n" % e)
                    break
                except Exception as e:
                    print(f"\nAn unexpected error occurred: {e}\n")
                    break
        if not df_data.empty:
            df_data['region_id'] = region
        self.InsInfo = pd.concat([self.InsInfo,df_data],axis=0,ignore_index=True)
        return df_data
    
    def getInsDetail(self,region):
        # DescribeEipAddressAttributes
        pass 
    
    def getBindedInsInfo(self,merge=True):
        # NetworkInterface
        for region in self.InsInfo['region_id'].unique():
            df_region_InsInfo = self.InsInfo[self.InsInfo['region_id']==region]
            instance_list = df_region_InsInfo[df_region_InsInfo["instance_type"] == "NetworkInterface"]["instance_id"].to_list()
            print(instance_list)
            ForQueryECS = ECS(self.AK,self.SK)
            ForQueryECS.getInsInfo(region=region,eip_addresses_list=instance_list[:10])
            print(ForQueryECS.InsInfo)
            
        # ClbInstance
        # EcsInstance
        # Nat
        # df_ins_all = pd.DataFrame()
        # if self.InsInfo.empty:
        #     print("No EIP Info!Pls get info frist!")
        # else:
        #     for region in self.InsInfo['self_RegionId'].unique():
        #         df_region_InsInfo = self.InsInfo[self.InsInfo['self_RegionId']==region]
        #         df_ins = pd.DataFrame({"InstanceId":[], "InstanceName":[]})
        #         instance_list = df_region_InsInfo[df_region_InsInfo["InstanceType"] == "NetworkInterface"]["InstanceId"].to_list()
        #         if len(instance_list) > 0:
        #             MoreInfo = ENI(self.Credentials)
        #             df_ENI = MoreInfo.getENIInfo(region,instance_list)[['NetworkInterfaceId','InstanceId']].dropna(subset=['InstanceId'])
        #             if len(df_ENI) > 0:
        #                 MoreInfo = ECS(self.Credentials)
        #                 df_ENI_ECS = MoreInfo.getECSInfo(region,df_ENI['InstanceId'].drop_duplicates().to_list())[['InstanceId','InstanceName']]
        #                 df_ENI_ECS = pd.merge(df_ENI,df_ENI_ECS,how="left",on='InstanceId').drop(['InstanceId'],axis=1)
        #                 df_ENI_ECS.rename(columns={'NetworkInterfaceId':'InstanceId'},inplace=True)
        #                 df_ins = pd.concat([df_ins,df_ENI_ECS],axis=0,ignore_index=True)
        #         instance_list = df_region_InsInfo[df_region_InsInfo["InstanceType"] == "EcsInstance"]["InstanceId"].to_list()
        #         if len(instance_list) > 0:
        #             MoreInfo = ECS(self.Credentials)
        #             df_ECS = MoreInfo.getECSInfo(region,instance_list)[['InstanceId','InstanceName']]
        #             df_ins = pd.concat([df_ins,df_ECS],axis=0,ignore_index=True)
        #         instance_list = df_region_InsInfo[df_region_InsInfo["InstanceType"] == "SlbInstance"]["InstanceId"].drop_duplicates().to_list()
        #         if len(instance_list) > 0:
        #             MoreInfo = SLB(self.Credentials)
        #             df_SLB = MoreInfo.getSLBInfo(region,instance_list)[['LoadBalancerId','LoadBalancerName']]
        #             df_SLB.rename(columns={'LoadBalancerId':'InstanceId','LoadBalancerName':'InstanceName'},inplace=True)
        #             df_ins = pd.concat([df_ins,df_SLB],axis=0,ignore_index=True)
        #         instance_list = df_region_InsInfo[df_region_InsInfo["InstanceType"] == "Nat"]["InstanceId"].drop_duplicates().to_list()
        #         if len(instance_list) > 0:
        #             MoreInfo = NGW(self.Credentials)
        #             df_NGW = MoreInfo.getNGWInfo(region,instance_list)[['NatGatewayId','Name']]
        #             df_NGW.rename(columns={'NatGatewayId':'InstanceId','Name':'InstanceName'},inplace=True)
        #             df_ins = pd.concat([df_ins,df_NGW],axis=0,ignore_index=True)
        #         df_ins_all = pd.concat([df_ins_all,df_ins],axis=0,ignore_index=True)
        #     if merge:
        #         self.InsInfo = pd.merge(self.InsInfo,df_ins_all,how="left",on='InstanceId')
        # return df_ins_all
        

class OBServer(VolcInstance):
    def __init__(self,AK,SK):
        super().__init__(AK,SK)

    def getRules(self,rule_name:str=None,page_size=100):
        configuration = volcenginesdkcore.Configuration()
        configuration.ak = self.AK
        configuration.sk = self.SK
        configuration.region = "cn-beijing"
        # set default configuration
        volcenginesdkcore.Configuration.set_default(configuration)

        # use global default configuration
        api_instance = volcenginesdkvolcobserve.VOLCOBSERVEApi()
        list_rules_request = volcenginesdkvolcobserve.ListRulesRequest()
        list_rules_request.page_size = page_size
        if rule_name:
            list_rules_request.rule_name = rule_name

        page_number = 1
        has_next_page = True        
        df_data = pd.DataFrame()
        while has_next_page:
            list_rules_request.page_number = page_number
            try:
                response = api_instance.list_rules(list_rules_request)
                df_data_tmp = pd.DataFrame(response.to_dict().get("data"))
                df_data = pd.concat([df_data,df_data_tmp])
                if response.total_count <= page_number * list_rules_request.page_size:
                    has_next_page = False
                else:
                    page_number += 1
            except ApiException as e:
                print("Exception when calling api: %s\n" % e)
                break
            except Exception as e:
                print("Exception when calling api: %s\n" % e)
                break
        self.InsInfo = pd.concat([df_data, self.InsInfo],axis=0,ignore_index=True)
        return df_data

    def expandInsInfo(self):
        self.InsInfo['original_dimensions'] = self.InsInfo['original_dimensions'].apply(lambda x: {} if pd.isna(x) else x)
        # print(self.InsInfo[self.InsInfo['original_dimensions'].isna()])
        self.InsInfo['ResourceID'] = self.InsInfo['original_dimensions'].apply(lambda x: x.get('ResourceID', []))
        df_exploded = self.InsInfo.explode('ResourceID').reset_index(drop=True)
        self.InsData = df_exploded
        return df_exploded
    
class Resource(BasicDataFrame):
    def __init__(self,AK,SK):
        self.AK = AK
        self.SK = SK

    def getInsInfo(self,page_size=100,sleep_time=2):
        SearchResources = VCMSearchResourcesRequest(ak=self.AK, sk=self.SK,max_results=page_size)
        # SearchResources.Debug = True

        df_data = pd.DataFrame()
        next_token = None        
        while True:
            response_json = SearchResources.do_request()
            resources = response_json.get("Resources", [])
            df_tmp = pd.DataFrame(resources)
            df_data = pd.concat([df_data, df_tmp], ignore_index=True)
            next_token = response_json.get("NextToken")
            if not next_token:
                break
            SearchResources.NextToken = next_token
            pprint(df_tmp)
            time.sleep(sleep_time)
        self.InsInfo = df_data
        return df_data
        