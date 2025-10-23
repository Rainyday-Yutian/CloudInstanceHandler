# coding: utf-8
import pandas as pd
# import requests
import numpy as np

from huaweicloudsdkcore.auth.credentials import GlobalCredentials
from huaweicloudsdkgeip.v3.region.geip_region import GeipRegion
from huaweicloudsdkgeip.v3 import *
from huaweicloudsdkces.v2.region.ces_region import CesRegion
from huaweicloudsdkces.v2 import *
from huaweicloudsdkrms.v1.region.rms_region import RmsRegion
from huaweicloudsdkrms.v1 import *
from huaweicloudsdkecs.v2.region.ecs_region import EcsRegion
from huaweicloudsdkecs.v2 import *
from huaweicloudsdkvpc.v3.region.vpc_region import VpcRegion
import huaweicloudsdkvpc.v2 as hwvpcv2
from huaweicloudsdkvpc.v3 import *
from huaweicloudsdkevs.v2.region.evs_region import EvsRegion
from huaweicloudsdkevs.v2 import *
from huaweicloudsdkiam.v3.region.iam_region import IamRegion
from huaweicloudsdkiam.v3 import *
from huaweicloudsdkelb.v3.region.elb_region import ElbRegion
from huaweicloudsdkelb.v3 import *

# from huaweicloudsdkiam.v3 import IamClient, ListRegionsRequest
# from huaweicloudsdkces.v1.region.ces_region import CesRegion
# from huaweicloudsdkces.v1 import *

from cloudinstancehandler.hwyun.HwyunInstance import *
    
# ----------------------------------------  全资源 ---------------------------------------------- #
class Resources(BasicDataFrame):
    def __init__(self,access_key,secret_key) -> None:
        super().__init__()
        self.access_key = access_key
        self.secret_key = secret_key
    
    # def getInsInfo(self):
    # def getResourcesInfo(self):
    # def getAllResourcesSummary(self):
    def get_all_resources_summary(self):
        credentials = GlobalCredentials(self.access_key,self.secret_key)
        client = RmsClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(RmsRegion.value_of("cn-north-4")) \
            .build()
        try:
            request = CollectAllResourcesSummaryRequest()
            response_json = client.collect_all_resources_summary(request).to_json_object()
            df_summary = pd.DataFrame()
            for provider in response_json:
                for types in provider['types']:
                    df_types_region_count = pd.DataFrame(types['regions'])
                    df_types_region_count['provider'] = provider['provider']
                    df_types_region_count['type'] = types['type']
                    df_summary = pd.concat([df_summary,df_types_region_count],ignore_index=True)
        except exceptions.ClientRequestException as e:
            print(f"Request id: {e.request_id}\nStatus code: {e.status_code}, error code: {e.error_code}\nError message: {e.error_msg}")
        # if 'provider' in df_summary.columns and 'type' in df_summary.columns:
        df_summary['provider_type'] = df_summary['provider'] + '.' + df_summary['type'] 
        self.InsInfo = df_summary
        return df_summary

    # def get_resources_by_type(self,provider,type,limit=200,next_marker=None):
    def get_resources_by_type(self,provider,type,limit=200):
        credentials = GlobalCredentials(self.access_key,self.secret_key)
        if limit>200:
            limit = 200
        client = RmsClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(RmsRegion.value_of("cn-north-4")) \
            .build()
        try:
            request = ListResourcesRequest()
            request.provider = provider
            request.type = type
            request.limit = limit
            response_json = client.list_resources(request).to_json_object()
            df_data = pd.DataFrame(response_json['resources'])
            # next_marker = response_json['page_info']['next_marker']
            next_marker = response_json.get('page_info', {}).get('next_marker', None)
            while next_marker:
                request.marker = next_marker
                response_json = client.list_resources(request).to_json_object()
                next_marker = response_json['page_info']['next_marker']
                # df_temp = pd.DataFrame(response_json['resources'])
                df_data = pd.concat([df_data,pd.DataFrame(response_json['resources'])],ignore_index=True)
            # 递归法，个人不太喜欢用递归，pass
            # if response_json['page_info']['next_marker']:
                # df_data = df_data.append(self.get_resources_by_type(provider,type,limit,next_marker=response_json['page_info']['next_marker']),ignore_index=True)    
            # print(df_data)
        except exceptions.ClientRequestException as e:
            print(f"Request id: {e.request_id}\nStatus code: {e.status_code}, error code: {e.error_code}\nError message: {e.error_msg}")
            df_data = pd.DataFrame()
        self.InsData = pd.concat([self.InsData,df_data],ignore_index=True)
        return df_data

    # 通过指定的provider和type查出资源类型的数据取资源信息，可以筛选不要的provider和type，合并给self.InsData
    def get_resources_data(self,provider_type_list:list=[],is_ignore_list=False):
        if len(provider_type_list) != 0:
            if is_ignore_list:
                df_resource = self.InsInfo[~self.InsInfo['provider_type'].isin(provider_type_list)]
            else:
                df_resource = self.InsInfo[self.InsInfo['provider_type'].isin(provider_type_list)]   
        else:
            df_resource = self.InsInfo
        df_provider_type_unique = df_resource.drop_duplicates(subset=['provider', 'type'])
        params = df_provider_type_unique[['provider', 'type']].to_dict(orient='records')
        for param in params:
            df_data = self.get_resources_by_type(param['provider'],param['type'])
            print(df_data)

    # 未写完，通过此接口，无需指定provider和type，直接查询所有资源类型的数据，赋给self.InsData，这里会直接覆写self.InsData
    def get_all_resources(self,limit=20):
        credentials = GlobalCredentials(self.access_key,self.secret_key)
        client = RmsClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(RmsRegion.value_of("cn-north-4")) \
            .build()
        try:
            request = ListAllResourcesRequest()
            request.limit = limit
            response_json = client.list_all_resources(request).to_json_object()
            # while response_json['meta_data']['marker'] != "":
            #     request.start = response_json['meta_data']['marker']
            pprint(response_json)
        except exceptions.ClientRequestException as e:
            print(f"Request id: {e.request_id}\nStatus code: {e.status_code}, error code: {e.error_code}\nError message: {e.error_msg}")
        

class CES(BasicDataFrame):
    def __init__(self,access_key,secret_key) -> None:
        super().__init__()
        self.access_key = access_key
        self.secret_key = secret_key

    # 获取地域所有告警规则
    def get_alarm_rules(self,region_id,limit=100):
        credentials = BasicCredentials(self.access_key,self.secret_key)
        if limit > 100:
            limit = 100
        client = CesClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(CesRegion.value_of(region_id)) \
            .build()

        try:
            request = ListAlarmRulesRequest()
            request.limit = limit
            response_json = client.list_alarm_rules(request).to_json_object()
            total_count = response_json.get('count', 0)
            data = response_json.get('alarms', [])
            if total_count > limit:
                for idx in range(limit, total_count, limit):
                    request.offset = idx
                    response_json = client.list_alarm_rules(request).to_json_object()
                    data.extend(response_json.get('alarms', []))
        except exceptions.ClientRequestException as e:
            print(f"Request id: {e.request_id}\nStatus code: {e.status_code}, error code: {e.error_code}\nError message: {e.error_msg}")
        df_data = pd.DataFrame(data)
        df_data['alarm_region_id'] = region_id
        df_data.rename(columns={'name': 'alarm_name','type':'alarm_app_type'}, inplace=True)
        self.InsInfo = pd.concat([self.InsInfo, df_data], ignore_index=True)
        return df_data

    # 返回单个告警规则关联的资源
    def list_alarms_rule_resources(self,region_id,alarm_id,limit=100):
        credentials = BasicCredentials(self.access_key,self.secret_key)
        if limit > 100:
            limit = 100
        client = CesClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(CesRegion.value_of(region_id)) \
            .build()
        try:
            request = ListAlarmRuleResourcesRequest()
            request.alarm_id = alarm_id
            request.limit = limit
            response_json = client.list_alarm_rule_resources(request).to_json_object()
            total_count = response_json.get('count', 0)
            data = response_json.get('resources', [])
            if total_count > limit:
                for idx in range(limit, total_count, limit):
                    request.offset = idx
                    response_json = client.list_alarm_rule_resources(request).to_json_object()
                    data.extend(response_json.get('resources', []))
            if data:
                data = [item for sublist in data for item in sublist]
                df = pd.DataFrame(data)
                df['alarm_id'] = alarm_id
                df.rename(columns={'name': 'type_name'}, inplace=True)
            else:
                df = pd.DataFrame()
        except exceptions.ClientRequestException as e:
            print(f"request id: {e.request_id}\nStatus code: {e.status_code}, error code: {e.error_code}\nerror message: {e.error_msg}")
        return df
    
    def get_alarms_associated_resources(self):
        df_rules_resources = pd.DataFrame()
        for region in self.InsInfo['alarm_region_id'].unique():
            for alarm_id in self.InsInfo[self.InsInfo['alarm_region_id']== region]['alarm_id'].unique():
                df_tmp = self.list_alarms_rule_resources(region_id=region,alarm_id=alarm_id)
                df_rules_resources = pd.concat([df_rules_resources,df_tmp],ignore_index=True)
                print(df_tmp)
        df_rules_resources.rename(columns={'value':'id'},inplace=True)        
        print(df_rules_resources)
        self.InsData = pd.merge(df_rules_resources,self.InsInfo,how='left',on='alarm_id')
        return df_rules_resources

    # 资源维度告警规则
    def get_resources_alarms_old(self,region_id_list):
        df_all_rules_resources = pd.DataFrame()
        
        # for region in ["cn-north-4"]:
        for region in region_id_list:
            df_region_rules_resources = pd.DataFrame()
            df_rules = self.get_alarm_rules(region)
            for index,alarm in df_rules.iterrows():
                df_rules_resources = self.list_alarms_rule_resources(region_id=region,alarm_id=alarm['alarm_id'])
                # df_rules_resources['alarm_name'] = alarm['name']+f"[{alarm['enabled']}]"
                df_rules_resources['alarm_name'] = alarm['name']
                df_rules_resources['alarm_id'] = alarm['alarm_id']
                df_rules_resources['enabled'] = alarm['enabled']
                df_region_rules_resources = pd.concat([df_region_rules_resources,df_rules_resources],ignore_index=True)
                print(df_region_rules_resources)
                # 减轻接口压力用
                if index % 20 == 0:
                    print("减轻接口压力，暂停一会儿")
                    time.sleep(2)
            df_region_rules_resources['region'] = region
            df_all_rules_resources = pd.concat([df_all_rules_resources,df_region_rules_resources],ignore_index=True)
            df_all_rules_resources.rename(columns={'value':'id'},inplace=True)
        return df_all_rules_resources
    
    def scan_all_resources_alarms(self):
        pass
        scan = Resource(self.access_key,self.secret_key)

# ------------------------------------------ 云产品实例 -------------------------------------------------- #
class IAM(BasicDataFrame):
    def __init__(self,access_key,secret_key) -> None:
        super().__init__()
        self.access_key = access_key
        self.secret_key = secret_key

    def getInsInfo(self):
        credentials = GlobalCredentials(self.access_key, self.secret_key)
        client = IamClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(IamRegion.value_of("cn-north-4")) \
            .build()
        try:
            request = KeystoneListRegionsRequest()
            response_json = client.keystone_list_regions(request).to_json_object()
            df_data = pd.DataFrame(response_json['regions'])
        except exceptions.ClientRequestException as e:
            print(f"Request id: {e.request_id}\nStatus code: {e.status_code}, error code: {e.error_code}\nError message: {e.error_msg}")
        return df_data

class ECS(HwyunInstance):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__(access_key,secret_key)
        self.namespace = "SYS.ECS"
        self.DimensionsName = "instance_id"
        self.SpecificationData = pd.DataFrame
    # 注意：ECS类的queryInsInfo()和getInsInfo()是基于两个不同的接口，返回的数据结构也不同，两者都会去写self.InsInfo，如果同时调用，会导致数据结构混乱。
    # 后续再进行优化，请人工区分调用。

    # 该支持全地域资源获取、且是分页获取，但目前仅用做查询获取，且该接口获取到的信息较少。
    def queryInsInfo(self,region_id,instance_id_list,page_size=100):
        credentials = BasicCredentials(self.access_key, self.secret_key)
        client = EcsClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(EcsRegion.value_of(region_id)) \
            .build()

        df_data = pd.DataFrame()
        try:
            request = ListCloudServersRequest()
            request.limit = page_size
            for idx in range(0, len(instance_id_list), page_size):
                batch_list_str = ",".join(instance_id_list[idx:idx + page_size])
                request.id = batch_list_str
                response_json = client.list_cloud_servers(request).to_json_object()
                # pprint(response_json)
                df_data_tmp = pd.DataFrame(response_json['servers'])
                df_data = pd.concat([df_data, df_data_tmp])
        except exceptions.ClientRequestException as e:
            print(f"Request id: {e.request_id}\nStatus code: {e.status_code}, error code: {e.error_code}\nError message: {e.error_msg}")
        self.InsInfo = pd.concat([self.InsInfo,df_data],ignore_index=True)
        return df_data
        
    # def getInsInfo
    # novalist 未写完，比如分页（超过1000条）
    def getInsInfo(self,region_id,limit=None,ip=None,not_tags="CCE-Dynamic-Provisioning-Node"):
        credentials = BasicCredentials(self.access_key, self.secret_key)

        client = EcsClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(EcsRegion.value_of(region_id)) \
            .build()
        try:
            request = NovaListServersDetailsRequest()
            # if limit:
            #     request.limit = limit
            if not_tags:
                request.not_tags = not_tags
            if ip:
                request.ip = ip
            response_json = client.nova_list_servers_details(request).to_json_object()
            df_data = pd.DataFrame(response_json['servers'])
        except exceptions.ClientRequestException as e:
            print(f"Request id: {e.request_id}\nStatus code: {e.status_code}, error code: {e.error_code}\nError message: {e.error_msg}")
            df_data = pd.DataFrame()
        print(df_data)
        if not df_data.empty:
            df_data['region_id'] = region_id
            df_data['spec_id'] = df_data['flavor'].apply(lambda x: x.get('id'))
        self.InsInfo = pd.concat([self.InsInfo,df_data],ignore_index=True)
        return df_data

    def getSpecData(self):
        credentials = BasicCredentials(self.access_key, self.secret_key)
        client = EcsClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(EcsRegion.value_of("cn-south-1")) \
            .build()
        try:
            request = ListFlavorsRequest()
            response_json = client.list_flavors(request).to_json_object()
            df_data = pd.DataFrame(response_json.get('flavors'))
            spec_df = pd.json_normalize(df_data['os_extra_specs'])
            df_data = pd.concat([df_data, spec_df], axis=1)
        except exceptions.ClientRequestException as e:
            print(f"Request id: {e.request_id}\nStatus code: {e.status_code}, error code: {e.error_code}\nError message: {e.error_msg}")
        self.SpecificationData = df_data
        return df_data

    def getECSSpec(self,field_list=None):
        if self.SpecificationData.empty:
            self.getSpecData()
        field_list.append('id')
        if field_list is None:
            df_data = self.SpecificationData
        else:
            df_data = self.SpecificationData[field_list]
        df_data = df_data.add_prefix('spec_')
        self.InsInfo = pd.merge(self.InsInfo,df_data,how='left',on='spec_id')

# 弹性网卡
class PORT(HwyunInstance):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__(access_key,secret_key)
    
    # def getInssInfo():
    #     credentials = BasicCredentials(access_key, secret_key)

    #     client = VpcClient.new_builder() \
    #         .with_credentials(credentials) \
    #         .with_region(VpcRegion.value_of("cn-north-4")) \
    #         .build()

    #     try:
    #         request = ListPortsRequest()
    #         request.id = "testetset"
    #         response = client.list_ports(request)
    #         print(response)
    #     except exceptions.ClientRequestException as e:
    #         print(e.status_code)
    #         print(e.request_id)
    #         print(e.error_code)
    #         print(e.error_msg)

    def getInsInfo(self,region_id,instance_id_list:list=None,private_ip_address_list:list=None,page_size=2000):
        query_limit = 50
        credentials = BasicCredentials(self.access_key, self.secret_key)

        client = hwvpcv2.VpcClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(VpcRegion.value_of(region_id)) \
            .build()
        df_data = []
        try:
            # if len(instance_id_list) <= query_limit:
            request = hwvpcv2.NeutronShowPortRequest()
            for instance_id in instance_id_list:
                request.port_id = instance_id
                response_json = client.neutron_show_port(request).to_json_object()
                df_data.append(response_json['port'])
                # pprint(response_json)
            df_data = pd.DataFrame(df_data)
            # else:
                # print("Exceed single query limit, gets all instances of the region")
                # raise ValueError("Exceed single query limit")
        except exceptions.ClientRequestException as e:
            print(f"Request id: {e.request_id}\nStatus code: {e.status_code}, error code: {e.error_code}\nError message: {e.error_msg}")
        self.InsInfo = pd.concat([self.InsInfo,df_data], ignore_index=True)
        return df_data

# 辅助网卡 未写完
class SNI(HwyunInstance):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__(access_key,secret_key)
    
    def getInsInfo(self,region_id,instance_id_list:list=None,private_ip_address_list:list=None,page_size=2000):
        credentials = BasicCredentials(self.access_key, self.secret_key)
        client = VpcClient.new_builder() \
        .with_credentials(credentials) \
        .with_region(VpcRegion.value_of(region_id)) \
        .build()
        df_data = pd.DataFrame()
        
        try:
            request = ListSubNetworkInterfacesRequest()
            request.limit = page_size
            if instance_id_list:
                request.id = instance_id_list
            if private_ip_address_list:
                request.private_ip_address = private_ip_address_list
            response_json = client.list_sub_network_interfaces(request).to_json_object()
            df_data_tmp = pd.DataFrame(response_json['sub_network_interfaces'])
            df_data = pd.concat([df_data,df_data_tmp])
        except exceptions.ClientRequestException as e:
            print(f"Request id: {e.request_id}\nStatus code: {e.status_code}, error code: {e.error_code}\nError message: {e.error_msg}")
        self.InsInfo = pd.concat([self.InsInfo,df_data])
        return df_data

class EVS(HwyunInstance):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__(access_key,secret_key)
        self.RegionList = []

    def getAvailableZones(self,region_id=None):
        credentials = BasicCredentials(self.access_key, self.secret_key)
        client = EvsClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(EvsRegion.value_of("cn-north-4")) \
            .build()

        df_data = pd.DataFrame()
        try:
            request = CinderListAvailabilityZonesRequest()
            response_json = client.cinder_list_availability_zones(request).to_json_object()
            df_data = pd.DataFrame(response_json['availabilityZoneInfo'])
        except exceptions.ClientRequestException as e:
            print(f"Request id: {e.request_id}\nStatus code: {e.status_code}, error code: {e.error_code}\nError message: {e.error_msg}")
        self.InsInfo = pd.concat([self.InsInfo,df_data],axis=0,ignore_index=True)
        return df_data

    def getInsInfo(self,region_id,limit=1000,sort_key=None,status=None):
        # if sort_key not in ["status","id","created_at","size"]:
        #     raise ValueError("sort_key must be one of 'status','id','created_at','size'")
        credentials = BasicCredentials(self.access_key, self.secret_key)
        client = EvsClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(EvsRegion.value_of(region_id)) \
            .build()
        
        df_data = pd.DataFrame()
        offset = 0
        total_count = None
        request = ListVolumesRequest()
        request.limit = limit
        if sort_key:
            request.sort_key = sort_key
        if status:
            request.status = status
        
        while True:
            try:
                request.offset = offset
                response_json = client.list_volumes(request).to_json_object()
                volumes = response_json.get('volumes', [])
                if not volumes:
                    break 
                df_data_tmp = pd.DataFrame(volumes)
                df_data = pd.concat([df_data, df_data_tmp], ignore_index=True)
                if 'count' in response_json and total_count is None:
                    total_count = response_json['count']
                if len(df_data) >= total_count:
                    break 
                offset += limit  # 更新偏移量
            except exceptions.ClientRequestException as e:
                print(f"Request id: {e.request_id}\nStatus code: {e.status_code}, error code: {e.error_code}\nError message: {e.error_msg}")
                break  # 遇到错误则停止分页查询
        self.InsInfo = pd.concat([self.InsInfo, df_data], ignore_index=True)
        return df_data
        
class ELB(HwyunInstance):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__(access_key,secret_key)
        self.namespace = "SYS.ELB"
        self.DimensionsName ="lbaas_instance_id"
    
    def getInsInfo(self,region_id,instance_id_list:list=None,page_size=100):
        credentials = BasicCredentials(self.access_key, self.secret_key)

        client = ElbClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(ElbRegion.value_of(region_id)) \
            .build()

        df_data = pd.DataFrame()
        try:
            request = ListLoadBalancersRequest()
            request.limit = page_size
            for idx in range(0, len(instance_id_list), page_size):
                batch_list = instance_id_list[idx:idx + page_size]
                request.id = batch_list
                response_json = client.list_load_balancers(request).to_json_object()
                # pprint(response_json)
                df_data_tmp = pd.DataFrame(response_json['loadbalancers'])
                df_data = pd.concat([df_data, df_data_tmp])
        except exceptions.ClientRequestException as e:
            print(f"Request id: {e.request_id}\nStatus code: {e.status_code}, error code: {e.error_code}\nError message: {e.error_msg}")
        self.InsInfo = pd.concat([self.InsInfo,df_data],ignore_index=True)
        return df_data

class GEIP(HwyunInstance):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__(access_key,secret_key)
        self.namespace = "SYS.GEIP"
        self.DimensionsName ="geip_global_eip_id"

    def getEipInfo(self,banwidth_id_list=None,status:list=None,raw=False):
        # status = ["idle", "inuse", "pending_create", "pending_update"]
        credentials = GlobalCredentials(self.access_key,self.secret_key)
        client = GeipClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(GeipRegion.value_of("cn-north-4")) \
            .build()
        try:
            request = ListGlobalEipsRequest()
            if banwidth_id_list is not None:
                request.internet_bandwidth_id = banwidth_id_list
            if status:
                request.status = status
            response_json = client.list_global_eips(request).to_json_object()
            df = pd.DataFrame(response_json['global_eips'])
        except exceptions.ClientRequestException as e:
            print(f"Request id: {e.request_id}\nStatus code: {e.status_code}, error code: {e.error_code}\nError message: {e.error_msg}")
            df = pd.DataFrame()
        if not df.empty and not raw :
            normalized_df = pd.json_normalize(df['associate_instance_info'])
            prefix = "associate_"
            normalized_df.columns = [f"{prefix}{col}" for col in normalized_df.columns]
            df = df.join(normalized_df)
            df["ipv4_address"] = df["ip_address"]
            df["ip_address"] = np.where(
                df["associate_instance_type"] == "IPV6-PORT", df["ipv6_address"], df["ip_address"]
            )
        self.InsInfo = pd.concat([self.InsInfo,df],axis=0,ignore_index=True)
        return df
    
    # Bury a mine in this method,heihei
    def getBindedInsInfo(self,merge=True):
        # if "associate_region" not in self.InsInfo.columns:
        for region_id in self.InsInfo["associate_region"].unique():
            df_region_ins = self.InsInfo[self.InsInfo["associate_region"]==region_id]
            df_data = pd.DataFrame()
            # PORT
            df_region_port = df_region_ins[df_region_ins["associate_instance_type"].isin(["PORT", "IPV6-PORT"])][['associate_instance_id']]
            if not df_region_port.empty:
                MoreInfo = PORT(self.access_key,self.secret_key)
                df_PortBindedInfo = MoreInfo.getInsInfo(region_id=region_id,instance_id_list=df_region_port["associate_instance_id"].tolist())[['id','device_id']]
                df_PortBindedInfo.rename(columns={"id":"associate_instance_id","device_id":"id"},inplace=True)
                # df_PortBindedInfo.rename(columns={"id":"associate_instance_id","device_id":"ecs_instance_id"},inplace=True)
                # df_PortBindedInfo.rename(columns={"id":"associate_instance_id"},inplace=True)
                df_data = pd.concat([df_data,df_PortBindedInfo])
            
            # ECS
            df_region_ecs = df_region_ins[df_region_ins["associate_instance_type"]=="ECS"][['associate_instance_id']]
            if not df_region_ecs.empty:
                # df_region_ecs['ecs_instance_id'] = df_region_ecs['associate_instance_id']
                df_region_ecs['id'] = df_region_ecs['associate_instance_id']
                df_data = pd.concat([df_data,df_region_ecs])
            if not df_data.empty:
                MoreInfo = ECS(self.access_key,self.secret_key)
                df_moreinfo = MoreInfo.queryInsInfo(region_id=region_id,instance_id_list=df_data[(df_data['id'].notnull()) & (df_data['id'] !="")]['id'].tolist())[['id','name']]
                df_data = pd.merge(df_data,df_moreinfo,how="left",on="id")
            # ELB
            df_region_lb = df_region_ins[df_region_ins["associate_instance_type"]=="ELB"]
            if not df_region_lb.empty:
                MoreInfo = ELB(self.access_key,self.secret_key)
                df_moreinfo = MoreInfo.getInsInfo(region_id=region_id,instance_id_list=df_region_lb["associate_instance_id"].tolist())[['id','name']]
                df_moreinfo['associate_instance_id'] = df_moreinfo['id']
                df_data = pd.concat([df_data,df_moreinfo],ignore_index=True)
            if merge and not df_data.empty:
                df_data.rename(columns={'id':'device_id','name':'device_name'},inplace=True)
                self.InsInfo = pd.merge(self.InsInfo,df_data,how="left",on="associate_instance_id")
            return df_data
            
    def getEIPBandwidthRank(self,TimeDict,direction="out",display_size=30):
        if self.InsInfo is None:
            return None
        
        if direction == "out":
            metric_name = "upstream_bandwidth"
        elif direction == "in":
            metric_name = "downstream_bandwidth"
        else:
            raise ValueError("direction must be in or out")
        
        df_data = self.getMetricData("cn-north-4",instance_list=self.InsInfo['id'].to_list(),metric_name=metric_name,period="60",TimeDict=TimeDict,)
    
        DisplayMetricName = f'yestorday_{metric_name}'
        Yestorday_TimeDict = getTimeDict(start_datetime=TimeDict['end_datetime']-datetime.timedelta(days=1,minutes=5),end_datetime=TimeDict['end_datetime']-datetime.timedelta(days=1))
        df_data_tmp = self.getMetricData("cn-north-4",instance_list=self.InsInfo['id'].to_list(),metric_name=metric_name,period="60",TimeDict=Yestorday_TimeDict,DisplayMetricName=DisplayMetricName)
        df_data = pd.merge(df_data,df_data_tmp,on=['id'],how='left')
        self.getBindedInsInfo()
        df_data['当前(Mbps)'] = df_data[f'{metric_name}_max'].div(1000**2).round(2)
        df_data['同比(Mbps)'] = df_data[f'{DisplayMetricName}_max'].div(1000**2).round(2)
        df_data['差值'] = df_data['当前(Mbps)'] - df_data['同比(Mbps)']
        df_data = pd.merge(self.InsInfo,df_data,on=['id'],how='left')
        df_data.sort_values(by=[f'{metric_name}_max'],ascending=False,inplace=True)
        # self.InsData = pd.concat([self.InsData,df_data],axis=0,ignore_index=True)
        return df_data
    
class GBWP(HwyunInstance):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__(access_key,secret_key)
        self.namespace = "SYS.GEIP"
        self.DimensionsName ="geip_internet_bandwidth_id"

    def getInsInfo(self,instance_id_list:list=None,instance_name:str=None,page_size=100):
        credentials = GlobalCredentials(self.access_key, self.secret_key)
        client = GeipClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(GeipRegion.value_of("cn-north-4")) \
            .build()

        df_data = pd.DataFrame()
        try:
            request = ListInternetBandwidthsRequest()
            if instance_name:
                request.name = instance_name
            if instance_id_list:
                request.id = instance_id_list
            # request.offset = 0
            request.limit = page_size
            response_json = client.list_internet_bandwidths(request).to_json_object()
            df_data = pd.concat([df_data, pd.DataFrame(response_json['internet_bandwidths'])],axis=0,ignore_index=True)
        except exceptions.ClientRequestException as e:
            print(f"Request id: {e.request_id}\nStatus code: {e.status_code}, error code: {e.error_code}\nError message: {e.error_msg}")
        self.InsInfo = pd.concat([self.InsInfo, df_data],axis=0,ignore_index=True)
        return df_data

# v1.0.6 和 1.0.7两个版本上新较多，很多类功能并不完善，比如实例分页查询这块，后期需要补全优化
