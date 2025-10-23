# coding: utf-8
# import pandas as pd
# import numpy as np
import requests

from cloudinstancehandler.aliyun.AliyunInstance import *

# from aliyunsdkcore.client import AcsClient
# from aliyunsdkcore.auth.credentials import AccessKeyCredential

# from aliyunsdkcore.acs_exception.exceptions import ClientException
# from aliyunsdkcore.acs_exception.exceptions import ServerException
# from aliyunsdkcore.auth.credentials import StsTokenCredential

from aliyunsdkecs.request.v20140526.DescribeInstancesRequest import DescribeInstancesRequest
from aliyunsdkvpc.request.v20160428.DescribeEipAddressesRequest import DescribeEipAddressesRequest
from aliyunsdkecs.request.v20140526.DescribeNetworkInterfacesRequest import DescribeNetworkInterfacesRequest
from aliyunsdkecs.request.v20140526.DescribeDisksRequest import DescribeDisksRequest
from aliyunsdkecs.request.v20140526.ModifyInstanceAttributeRequest import ModifyInstanceAttributeRequest
from aliyunsdkslb.request.v20140515.DescribeLoadBalancersRequest import DescribeLoadBalancersRequest
from aliyunsdkvpc.request.v20160428.DescribeIpv6AddressesRequest import DescribeIpv6AddressesRequest
from aliyunsdkr_kvstore.request.v20150101.DescribeInstancesOverviewRequest import DescribeInstancesOverviewRequest
from aliyunsdkr_kvstore.request.v20150101.DescribeInstancesRequest import DescribeInstancesRequest as DescribeRedisInsRequest
from aliyunsdkvpc.request.v20160428.DescribeCommonBandwidthPackagesRequest import DescribeCommonBandwidthPackagesRequest
from aliyunsdkvpc.request.v20160428.DescribeNatGatewaysRequest import DescribeNatGatewaysRequest
from aliyunsdkrds.request.v20140815.DescribeDBInstancesRequest import DescribeDBInstancesRequest
from aliyunsdkdds.request.v20151201.DescribeDBInstancesRequest import DescribeDBInstancesRequest
from aliyunsdkcdn.request.v20180510.DescribeUserDomainsRequest import DescribeUserDomainsRequest
from aliyunsdkcdn.request.v20180510.DescribeDomainBpsDataRequest import DescribeDomainBpsDataRequest
from aliyunsdkecs.request.v20140526.CreateSnapshotRequest import CreateSnapshotRequest
from aliyunsdkecs.request.v20140526.DescribeRegionsRequest import DescribeRegionsRequest

from aliyunsdkcore.request import CommonRequest

try:
    import oss2
    from oss2.credentials import StaticCredentialsProvider
except:
    print("导入oss2失败，相关功能将不可用!\n")


########################################################################################################
# 重要：AliyunInstance.getMetricList、getMetricData 已重写，以下代码需要重新修改调用参数，请尽快重新适配 #
########################################################################################################

class ECS(AliyunInstance):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__(access_key,secret_key)
        self.namespace = "acs_ecs_dashboard"
        self.ProductCategory = "ecs"
        self.ENIsFromAllECS = None
        self.Regions = None

    def getRegions(self) -> pd.DataFrame:
        client = AcsClient(region_id='cn-hangzhou', credential=self.credentials)
        request = DescribeRegionsRequest()
        request.set_accept_format('json')
        response_json = json.loads(client.do_action_with_exception(request))
        self.Regions = pd.DataFrame(response_json.get('Regions').get('Region'))
        return self.Regions

    def getInsInfo(self,region_id:str,instance_list=None,instance_name=None,page_size=100) -> pd.DataFrame:
        if page_size > 100:
            print("page_size must be less than or equal to 100, set default 100")
            page_size = 100
        client = AcsClient(region_id=region_id, credential=self.credentials)
        request = DescribeInstancesRequest()
        request.set_accept_format('json')
        request.set_MaxResults(page_size)
        if instance_name is not None:
            request.set_InstanceName(instance_name)
        if instance_list is not None:
            df = pd.DataFrame()
            for idx in range(0, len(instance_list),page_size):
                batch_inslist = instance_list[idx:idx+page_size]
                request.set_InstanceIds(batch_inslist)
                response_json = json.loads(client.do_action_with_exception(request))
                df_clip = pd.DataFrame(response_json['Instances']['Instance'])
                df = pd.concat([df, df_clip],ignore_index=True)
        else:
            response_json = json.loads(client.do_action_with_exception(request))
            pprint(response_json)
            df = pd.DataFrame(response_json['Instances']['Instance'])
            while(response_json.get('NextToken') is not None and response_json.get('NextToken') != ''):
                request.set_NextToken(response_json['NextToken'])    
                response_json = json.loads(client.do_action_with_exception(request))
                df_clip = pd.DataFrame(response_json['Instances']['Instance'])
                df = pd.concat([df, df_clip],ignore_index=True)
        if not df.empty:
            # getInsInfo统一规范：当接口中无RegionId字段时，需要手动添加
            # df['self_RegionId'] = region_id
            # df['RegionId'] = region_id
            self.InsInfo = pd.concat([self.InsInfo,df],axis=0,ignore_index=True)
        return df
    
    def modifyInstanceAttribute(self,region_id:str,instance_id:str,instance_name:str):
        client = AcsClient(region_id=region_id, credential=self.credentials)
        request = client.describe_instances()
        response_json = json.loads(client.do_action_with_exception(request))
        pprint(response_json)

    def extractEIPFromInsInfo(self):
        df_ENI = pd.DataFrame()
        if self.InsInfo.empty:
            print(f'No {self.instance_type} Info Data Found...')
        else:
            df = self.InsInfo
            df['NetworkInterfaces'] = df['NetworkInterfaces'].apply(lambda x: x['NetworkInterface'])
            for index,row in df.iterrows():
                df_temp = pd.DataFrame(row['NetworkInterfaces'])
                df_temp['InstanceName'] = row['InstanceName']
                df_temp['InstanceId'] = row['InstanceId']
                df_temp['RegionId'] = row['RegionId']
                df_ENI = pd.concat([df_ENI, df_temp],ignore_index=True)
            # print(f'Extracted EIP from {self.instance_type} Data...')
        self.ENIsFromAllECS = df_ENI
        return df_ENI
    

class Disks(AliyunInstance):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__(access_key,secret_key)
        self.namespace = "acs_ecs_dashboard"
        self.ProductCategory = "ecs"

    def getInsInfo(self,region_id,disk_ids_list=None,instance_id:str=None,instance_name=None,disk_type="all",status="All",page_size=100) -> pd.DataFrame:
        if disk_type not in ['all','system','data']:
            raise ValueError('disk_type must be "all","system","data"')
        if status not in ['All','In_use','Available','Creating','ReIniting','ReInitingFailed','Attaching','Detaching','UnPlugging','UnPlugged']:
            raise ValueError('status must be "All","In_use","Available","Creating","ReIniting","ReInitingFailed","Attaching","Detaching","UnPlugging","UnPlugged"')
            # In_use：使用中。Available：待挂载。Attaching：挂载中 。Detaching：卸载中。Creating：创建中。ReIniting：初始化中。All：所有状态。
        client = AcsClient(region_id=region_id, credential=self.credentials)
        request = DescribeDisksRequest()
        request.set_accept_format('json')
        request.set_DiskType(disk_type)
        request.set_PageSize(page_size)
        request.set_PageNumber(1)
        request.set_Status(status)
        if instance_id:
            request.set_InstanceId(instance_id)
        if disk_ids_list:
            request.set_DiskIds(disk_ids_list)
        response_json = json.loads(client.do_action_with_exception(request))
        df = pd.DataFrame(response_json['Disks']['Disk'])
        page_total = response_json['TotalCount'] // page_size + 1
        for page_number in range(2,page_total+1):
            request.set_PageNumber(page_number)
            response_json = json.loads(client.do_action_with_exception(request)) 
            df_clip = pd.DataFrame(response_json['Disks']['Disk'])
            df = pd.concat([df, df_clip],ignore_index=True)
        self.InsInfo = pd.concat([self.InsInfo,df],axis=0,ignore_index=True)
        return df
        # 2025.2.27 阿里云这个接口的新版分页方式好像还未支持好，先弃用新版分页方式
        # request.set_MaxResults(page_size)
        # response_json = json.loads(client.do_action_with_exception(request))
        # df = pd.DataFrame(response_json['Disks']['Disk'])
        # while(response_json.get('NextToken') is not None):
        #     request.set_NextToken(response_json['NextToken'])
        #     response_json = json.loads(client.do_action_with_exception(request))
        #     pprint(response_json)
        #     time.sleep(1)
        #     df_clip = pd.DataFrame(response_json['Disks']['Disk'])
        #     df = pd.concat([df, df_clip],ignore_index=True)
        # self.InsInfo = pd.concat([self.InsInfo,df],axis=0,ignore_index=True)
        # return df

    def createSnapshot(self,region_id,disk_id:str,retention_days:int=30,snapshot_name:str="created_by_CIH",description:str="CIH Snapshot") -> None:
        client = AcsClient(region_id=region_id, credential=self.credentials)
        request = CreateSnapshotRequest()
        request.set_accept_format('json')
        request.set_DiskId(disk_id)
        request.set_SnapshotName(snapshot_name)
        request.set_Description(description)
        request.set_RetentionDays(retention_days)
        try:
            response_json = json.loads(client.do_action_with_exception(request))
            df_data = pd.DataFrame([response_json])
        except Exception as e:
            print(f"Create Snapshot Failed, DiskId: {disk_id} ,Error Message: {e}")
            df_data = pd.DataFrame()
        return df_data
    
    # def batchCreateSnapshot(self):
    #     for index,row in self.InsInfo.iterrows():

            
        

class ENI(AliyunInstance):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__(access_key,secret_key)
        
    # def getENIInfo(self,region_id,instance_list=None,page_size=100):
    #     client = AcsClient(region_id=region_id, credential=self.credentials)
    #     request = DescribeNetworkInterfacesRequest()
    #     request.set_accept_format('json')
    #     request.set_PageSize(page_size)
    #     request.set_PageNumber(1)
    #     df = pd.DataFrame()
    #     if instance_list:
    #         for idx in range(0, len(instance_list),page_size):
    #             request.set_NetworkInterfaceIds(instance_list[idx:idx+page_size])
    #             response_json = json.loads(client.do_action_with_exception(request))
    #             df_clip = pd.DataFrame(response_json['NetworkInterfaceSets']['NetworkInterfaceSet'])
    #             df = pd.concat([df, df_clip],ignore_index=True)
    #     else:
    #         request.set_PageNumber(1)
    #         response_json = json.loads(client.do_action_with_exception(request))
    #         df = pd.DataFrame(response_json['NetworkInterfaceSets']['NetworkInterfaceSet']) 
    #         page_total = response_json['TotalCount'] // page_size + 1
    #         for page_number in range(2,page_total+1):
    #             request.set_PageNumber(page_number)
    #             response_json = json.loads(client.do_action_with_exception(request))
    #             df_clip = pd.DataFrame(response_json['NetworkInterfaceSets']['NetworkInterfaceSet'])
    #             df = pd.concat([df, df_clip],ignore_index=True) 
    #     df['self_RegionId'] = region_id
    #     self.InsInfo = pd.concat([self.InsInfo,df],axis=0,ignore_index=True)
    #     return df

    def getENIInfo(self,region_id,instance_list=None,page_size=500):
        client = AcsClient(region_id=region_id, credential=self.credentials)
        request = DescribeNetworkInterfacesRequest()
        request.set_accept_format('json')
        request.set_MaxResults(page_size)
        if instance_list is not None:
            request.set_NetworkInterfaceIds(instance_list)
        response_json = json.loads(client.do_action_with_exception(request))
        df = pd.DataFrame(response_json['NetworkInterfaceSets']['NetworkInterfaceSet'])
        while(response_json.get('NextToken') is not None):
            request.set_NextToken(response_json['NextToken'])    
            response_json = json.loads(client.do_action_with_exception(request))
            df_clip = pd.DataFrame(response_json['NetworkInterfaceSets']['NetworkInterfaceSet'])
            df = pd.concat([df, df_clip],ignore_index=True)
        df['self_RegionId'] = region_id
        self.InsInfo = pd.concat([self.InsInfo,df],axis=0,ignore_index=True)
        return df
    
class RDS(AliyunInstance):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__(access_key,secret_key)
        self.namespace = "acs_rds_dashboard"
        self.ProductCategory = "rds"

    def getRDSInfo(self,region_id,instance_list=None,page_size=100):
        client = AcsClient(region_id=region_id, credential=self.credentials)
        request = DescribeDBInstancesRequest()
        request.set_accept_format('json')
        request.set_MaxResults(page_size)
        if instance_list is not None:
            request.set_NetworkInterfaceIds(instance_list)
        response_json = json.loads(client.do_action_with_exception(request))
        df = pd.DataFrame(response_json['Items']['DBInstance'])
        while(response_json.get('NextToken') is not None or response_json.get('NextToken') !=''):
            pprint(response_json)
            pprint(response_json['NextToken'])
            request.set_NextToken(response_json['NextToken'])    
            response_json = json.loads(client.do_action_with_exception(request))
            df_clip = pd.DataFrame(response_json['Items']['DBInstance'])
            df = pd.concat([df, df_clip],ignore_index=True)
        df['self_RegionId'] = region_id
        self.InsInfo = pd.concat([self.InsInfo,df],axis=0,ignore_index=True)
        return df

class SLB(AliyunInstance):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__(access_key,secret_key)
        self.namespace = "acs_slb_dashboard"
        self.ProductCategory = "slb"
    
    # 未更新
    def getSLBInfo(self,region_id,inslist):
        client = AcsClient(region_id=region_id, credential=self.credentials)
        request = DescribeLoadBalancersRequest()
        request.set_accept_format('json')
        request.set_LoadBalancerId(','.join(inslist))
        page_size = 10      # PageSize 最大值100,但是精准查询上限10个
        request.set_PageSize(page_size)
        request.set_PageNumber(1)
        df = pd.DataFrame()
        for idx in range(0, len(inslist),page_size):
            batch_inslist = inslist[idx:idx+page_size]
            request.set_LoadBalancerId(','.join(batch_inslist))
            response_json = json.loads(client.do_action_with_exception(request))
            df_clip = pd.DataFrame(response_json['LoadBalancers']['LoadBalancer'])
            df = pd.concat([df, df_clip],ignore_index=True)
        df['self_RegionId'] = region_id
        self.InsInfo = pd.concat([self.InsInfo,df],axis=0,ignore_index=True)
        return df

class NGW(AliyunInstance):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__(access_key,secret_key)
        self.namespace = "acs_nat_gateway"
        self.ProductCategory = "nat_gateway"

    def getNGWInfo(self,region_id,instance_list=None,page_size=50):
        client = AcsClient(region_id=region_id, credential=self.credentials)
        request = DescribeNatGatewaysRequest()
        request.set_accept_format('json')
        request.set_PageSize(page_size)
        request.set_PageNumber(1)
        df = pd.DataFrame()
        if instance_list:
            page_size = 1
            for instance_str in instance_list:
                request.set_NatGatewayId(instance_str)
                response_json = json.loads(client.do_action_with_exception(request))
                df_clip = pd.DataFrame(response_json["NatGateways"]["NatGateway"])
                df = pd.concat([df, df_clip],ignore_index=True)
        else:
            request.set_PageNumber(1)
            response_json = json.loads(client.do_action_with_exception(request))
            df = pd.DataFrame(response_json["NatGateways"]["NatGateway"]) 
            page_total = response_json['TotalCount'] // page_size + 1
            for page_number in range(2,page_total+1):
                request.set_PageNumber(page_number)
                response_json = json.loads(client.do_action_with_exception(request))
                df_clip = pd.DataFrame(response_json["NatGateways"]["NatGateway"])
                df = pd.concat([df, df_clip],ignore_index=True)            
        df['self_RegionId'] = region_id
        self.InsInfo = pd.concat([self.InsInfo,df],axis=0,ignore_index=True)
        return df

class IPv6(AliyunInstance):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__(access_key,secret_key)
        self.namespace = "acs_ipv6_bandwidth"
        self.ProductCategory = "ipv6gateway"

    # 老版查询代码，未更新，可能需要修改
    def getIPv6Info(self,region_id,page_size=50) -> pd.DataFrame:
        if page_size > 50:
            # print("传参错误")
            return None

        client = AcsClient(region_id=region_id, credential=self.credentials) 
        request = DescribeIpv6AddressesRequest()
        request.set_accept_format('json')
        request.set_PageSize(1)
        request.set_PageNumber(1)
        response_json = json.loads(client.do_action_with_exception(request))
        page_total = response_json['TotalCount'] // page_size + 1
        df = pd.DataFrame()
        request.set_PageSize(page_size)
        for page_number in range(1,page_total+1):
            # time.sleep(sleep_time)
            request.set_PageNumber(page_number)
            response_json = json.loads(client.do_action_with_exception(request))
            df_temp = pd.DataFrame(response_json["Ipv6Addresses"]["Ipv6Address"]) 
            df = pd.concat([df, df_temp],axis=0,ignore_index=True)
        df['self_RegionId'] = region_id
        self.InsInfo = pd.concat([self.InsInfo,df],axis=0,ignore_index=True)
        return df

    def extendInternetBwInfo(self,merge=True) -> pd.DataFrame:
        """
        此函数将InsInfo中的Ipv6InternetBandwidth字段信息降维，并添加到InsInfo中
        """
        if "Ipv6InternetBandwidth" in self.InsInfo.columns:
            normalized_df = pd.json_normalize(self.InsInfo['Ipv6InternetBandwidth'])
            prefix = "InternetBw_"
            normalized_df.columns = [f"{prefix}{col}" for col in normalized_df.columns]
            if merge:
                self.InsInfo = self.InsInfo.join(normalized_df)
        else:
            normalized_df = pd.DataFrame()
        return normalized_df
    
    def getIPv6Data(self,RegionList,MetricList,TimeDict=None,DisplayNameDict=None) -> None:
        """
        MetricList = ["Ipv6Address.RatePercentageOutToInternet","Ipv6Address.RatePercentageInFromInternet"]
        RegionList = ["cn-chengdu"]
        DisplayNameDict = {"Ipv6Address.RatePercentageOutToInternet":"OutToInternet","Ipv6Address.RatePercentageInFromInternet":"InFromInternet"}
        """
        ENI_ForMoreInfo = ENI(self.credentials)
        ECS_ForMoreInfo = ECS(self.credentials)
        print(f'Obtaining {self.instance_type} Data in {RegionList}...')
        for region in RegionList:
            df = self.getIPv6Info(region)
            print(df)
            if not df.empty:
                # For getting more eni info
                eni_list = df[df["AssociatedInstanceType"]=="NetworkInterface"]["AssociatedInstanceId"].to_list()
                df_ENI = ENI_ForMoreInfo.getENIInfo(region,instance_list=eni_list)[["NetworkInterfaceId","InstanceId"]]
                df_ENI.rename(columns={"NetworkInterfaceId":"AssociatedInstanceId"},inplace=True)
                df = pd.merge(df,df_ENI,on="AssociatedInstanceId",how="left")
                # For getting more ecs info
                df.loc[df["AssociatedInstanceType"]=="EcsInstance","InstanceId"] = df.loc[df["AssociatedInstanceType"]=="EcsInstance", "AssociatedInstanceId"]
                df_ECS = ECS_ForMoreInfo.getECSInfo(region, df[df["InstanceId"].notna()]["InstanceId"].to_list())
                df = pd.merge(df,df_ECS[["InstanceId","InstanceName"]],on="InstanceId",how="left")
                # For getting more ipv6 info
                df = pd.concat([df, pd.json_normalize(df['Ipv6InternetBandwidth'])], axis=1)
                # Simplify the Info # 后续由self.drop
                df = df[["Ipv6AddressId","Ipv6Isp","NetworkType","InternetChargeType","Bandwidth","InstanceId","InstanceName"]]

                for metric in MetricList:
                    print(metric)
                    df_data = self.getMetricData(region,df["Ipv6AddressId"].to_list(),metric,TimeDict=TimeDict,rename_field_name=DisplayNameDict.get(metric))
                    print(df_data)
                    df_data.rename(columns={"instanceId":"Ipv6AddressId"},inplace=True)
                    df = pd.merge(df,df_data,on="Ipv6AddressId",how="left")
                self.InsData = pd.concat([self.InsData,df],axis=0,ignore_index=True)
                print(f"The {self.instance_type} data of {region} is obtained!")
            else:
                print(f"No {self.instance_type} in {region}!")
    
class Redis(AliyunInstance):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__(access_key,secret_key)
        self.namespace = "acs_kvstore"
        self.ProductCategory = ["kvstore_standard","kvstore_splitrw","kvstore_sharding"]

    def getReidsOverview(self,region_id) -> pd.DataFrame:
        client = AcsClient(region_id=region_id, credential=self.credentials)
        request = DescribeInstancesOverviewRequest()
        request.set_accept_format('json')
        response_json = json.loads(client.do_action_with_exception(request))
        df = pd.DataFrame(response_json["Instances"]) 
        return df

    def getRedisInfo(self,region_id,instance_list=None,page_size=50) -> pd.DataFrame:
        client = AcsClient(region_id=region_id, credential=self.credentials) 
        request = DescribeRedisInsRequest()
        request.set_accept_format('json')
        request.set_PageSize(page_size)
        request.set_PageNumber(1)
        if instance_list is None:
            response_json = json.loads(client.do_action_with_exception(request))
            df = pd.DataFrame(response_json["Instances"]["KVStoreInstance"]) 
            page_total = response_json['TotalCount'] // page_size + 1
            for page_number in range(2,page_total+1):
                request.set_PageNumber(page_number)
                response_json = json.loads(client.do_action_with_exception(request))
                df_clip = pd.DataFrame(response_json["Instances"]["KVStoreInstance"])
                df = pd.concat([df, df_clip],ignore_index=True)
        else:
            df = pd.DataFrame()
            page_size = 30
            request.set_PageSize(page_size)
            for idx in range(0, len(instance_list),page_size):
                batch_inslist_str = ",".join(instance_list[idx:idx+page_size])
                request.set_InstanceIds(batch_inslist_str)
                response_json = json.loads(client.do_action_with_exception(request))
                df_clip = pd.DataFrame(response_json["Instances"]["KVStoreInstance"])
                df = pd.concat([df, df_clip],ignore_index=True)
        df['self_RegionId'] = region_id
        self.InsInfo = pd.concat([self.InsInfo,df],axis=0,ignore_index=True)
        return df
        
    def getRedisData(self,RegionList,MetricList,TimeDict,instance_list=None)-> None:
        print(f'Obtaining {self.instance_type} Data in {RegionList}...')
        for region in RegionList:
            if instance_list and len(RegionList) == 1:
                df = self.getRedisInfo(region,instance_list)
            else:
                df = self.getRedisInfo(region)
            # self.InsInfo[region] = self.getRedisInfo(region)
            # df format
            print(df)
            if not df.empty:
                cluster_list = df[df['ArchitectureType']=="cluster"]["InstanceId"].to_list()
                standard_list = df[df['ArchitectureType']=="standard"]["InstanceId"].to_list()
                rwsplit_list = df[df['ArchitectureType']=="rwsplit"]["InstanceId"].to_list()
                for metric_name in MetricList:
                    print(metric_name)
                    df_data = pd.concat(
                        [
                            self.getMetricData(region, cluster_list, "Sharding"+metric_name, TimeDict=TimeDict, rename_field_name=metric_name),
                            self.getMetricData(region, standard_list, "Standard"+metric_name, TimeDict=TimeDict, rename_field_name=metric_name),
                            self.getMetricData(region, rwsplit_list, "Splitrw"+metric_name, TimeDict=TimeDict, rename_field_name=metric_name)
                        ],
                        ignore_index=True
                    )
                    df = pd.merge(df, df_data.rename(columns={"instanceId":"InstanceId"}), on='InstanceId')
                self.InsData = pd.concat([self.InsData,df],ignore_index=True)
                print(f"The {self.instance_type} data of {region} is obtained!")
            else:
                print(f"No {self.instance_type} in {region}!")

# MongoDB
class DDS(AliyunInstance):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__(access_key,secret_key)
        self.namespace = "acs_mongodb"
        self.ProductCategory = "mongodb_replicaset"
        self.instance_type = "replicate"

    def getInsInfo(self,region_id,instance_list=None,page_size=100):
        client = AcsClient(region_id=region_id, credential=self.credentials)
        request = DescribeDBInstancesRequest()
        request.set_accept_format('json')
        request.set_PageSize(page_size)
        request.set_DBInstanceType(self.instance_type)
        response_json = json.loads(client.do_action_with_exception(request))
        df_data = pd.DataFrame(response_json["DBInstances"]["DBInstance"]) 
        page_total = response_json['TotalCount'] // page_size + 1
        for page_number in range(2,page_total+1):
            request.set_PageNumber(page_number)
            response_json = json.loads(client.do_action_with_exception(request))
            df_data_tmp = pd.DataFrame(response_json["DBInstances"]["DBInstance"])
            df_data = pd.concat([df_data, df_data_tmp],ignore_index=True)            
        df_data['self_RegionId'] = region_id
        self.InsInfo = pd.concat([self.InsInfo,df_data],axis=0,ignore_index=True)
        return df_data

class EIP(AliyunInstance):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__(access_key,secret_key)
        self.namespace = "acs_vpc_eip"
        self.ProductCategory = "eip"

    def getEIPInfo(self,region_id,instance_list=None,ip_address_list=None,associate_instance_type=None,associate_instance_id=None,page_size=50) -> pd.DataFrame:
        client = AcsClient(region_id=region_id, credential=self.credentials) 
        request = DescribeEipAddressesRequest()
        request.set_accept_format('json')
        request.set_PageSize(page_size)
        df = pd.DataFrame()
        # 先这样，后面考虑动态调用
        if associate_instance_type:
            request.set_AssociatedInstanceType(associate_instance_type)
        if instance_list:
            for idx in range(0, len(instance_list),page_size):
                batch_inslist_str = ",".join(instance_list[idx:idx+page_size])
                request.set_AllocationId(batch_inslist_str)
                response_json = json.loads(client.do_action_with_exception(request))
                df_clip = pd.DataFrame(response_json["EipAddresses"]["EipAddress"])
                df = pd.concat([df, df_clip],ignore_index=True)
        elif ip_address_list:
            for idx in range(0, len(ip_address_list),page_size):
                batch_ip_str = ",".join(ip_address_list[idx:idx+page_size])
                request.set_EipAddress(batch_ip_str)
                response_json = json.loads(client.do_action_with_exception(request))
                df_clip = pd.DataFrame(response_json["EipAddresses"]["EipAddress"])
                df = pd.concat([df, df_clip],ignore_index=True)
        # elif associate_instance_id:
        #     for idx in range(0, len(associate_instance_id),page_size):
        #         batch_inslist_str = ",".join(associate_instance_id[idx:idx+page_size])
        #         request.set_AssociatedInstanceId(batch_inslist_str)
        #         response_json = json.loads(client.do_action_with_exception(request))
        #         df_clip = pd.DataFrame(response_json["EipAddresses"]["EipAddress"])
        #         df = pd.concat([df, df_clip],ignore_index=True)
        else:
            request.set_PageNumber(1)
            response_json = json.loads(client.do_action_with_exception(request))
            df = pd.DataFrame(response_json["EipAddresses"]["EipAddress"]) 
            page_total = response_json['TotalCount'] // page_size + 1
            for page_number in range(2,page_total+1):
                request.set_PageNumber(page_number)
                response_json = json.loads(client.do_action_with_exception(request))
                df_clip = pd.DataFrame(response_json["EipAddresses"]["EipAddress"])
                df = pd.concat([df, df_clip],ignore_index=True)            
        df['self_RegionId'] = region_id
        self.InsInfo = pd.concat([self.InsInfo,df],axis=0,ignore_index=True)
        return df
    
    # 没写完，尽量再保留一列AssociatedInstanceId
    def getBindedInsInfo(self,merge=True):
        df_ins_all = pd.DataFrame()
        if self.InsInfo.empty:
            print("No EIP Info!Pls get info frist!")
        else:
            for region in self.InsInfo['self_RegionId'].unique():
                df_region_InsInfo = self.InsInfo[self.InsInfo['self_RegionId']==region]
                df_ins = pd.DataFrame({"InstanceId":[], "InstanceName":[]})
                instance_list = df_region_InsInfo[df_region_InsInfo["InstanceType"] == "NetworkInterface"]["InstanceId"].to_list()
                if len(instance_list) > 0:
                    MoreInfo = ENI(self.credentials)
                    df_ENI = MoreInfo.getENIInfo(region,instance_list)[['NetworkInterfaceId','InstanceId']].dropna(subset=['InstanceId'])
                    if len(df_ENI) > 0:
                        MoreInfo = ECS(self.credentials)
                        df_ENI_ECS = MoreInfo.getECSInfo(region,df_ENI['InstanceId'].drop_duplicates().to_list())[['InstanceId','InstanceName']]
                        df_ENI_ECS = pd.merge(df_ENI,df_ENI_ECS,how="left",on='InstanceId').drop(['InstanceId'],axis=1)
                        df_ENI_ECS.rename(columns={'NetworkInterfaceId':'InstanceId'},inplace=True)
                        df_ins = pd.concat([df_ins,df_ENI_ECS],axis=0,ignore_index=True)
                instance_list = df_region_InsInfo[df_region_InsInfo["InstanceType"] == "EcsInstance"]["InstanceId"].to_list()
                if len(instance_list) > 0:
                    MoreInfo = ECS(self.credentials)
                    df_ECS = MoreInfo.getECSInfo(region,instance_list)[['InstanceId','InstanceName']]
                    df_ins = pd.concat([df_ins,df_ECS],axis=0,ignore_index=True)
                instance_list = df_region_InsInfo[df_region_InsInfo["InstanceType"] == "SlbInstance"]["InstanceId"].drop_duplicates().to_list()
                if len(instance_list) > 0:
                    MoreInfo = SLB(self.credentials)
                    df_SLB = MoreInfo.getSLBInfo(region,instance_list)[['LoadBalancerId','LoadBalancerName']]
                    df_SLB.rename(columns={'LoadBalancerId':'InstanceId','LoadBalancerName':'InstanceName'},inplace=True)
                    df_ins = pd.concat([df_ins,df_SLB],axis=0,ignore_index=True)
                instance_list = df_region_InsInfo[df_region_InsInfo["InstanceType"] == "Nat"]["InstanceId"].drop_duplicates().to_list()
                if len(instance_list) > 0:
                    MoreInfo = NGW(self.credentials)
                    df_NGW = MoreInfo.getNGWInfo(region,instance_list)[['NatGatewayId','Name']]
                    df_NGW.rename(columns={'NatGatewayId':'InstanceId','Name':'InstanceName'},inplace=True)
                    df_ins = pd.concat([df_ins,df_NGW],axis=0,ignore_index=True)
                df_ins_all = pd.concat([df_ins_all,df_ins],axis=0,ignore_index=True)
            if merge:
                self.InsInfo = pd.merge(self.InsInfo,df_ins_all,how="left",on='InstanceId')
        return df_ins_all

    # def getStatementData(YoY=True,)

    def getInsData(self,MetricList,TimeDict,RegionList=None,DisplayNameDict=None) -> None:
        if self.InsInfo.empty:
            # if RegionList is None:
                # RegionList = describeavialableregions().regions
            print(f'Obtaining {self.instance_type} Info in {RegionList}...')
            for RegionId in RegionList:
                self.getEIPInfo(RegionId)
            self.getBindedInsInfo()
        else:
            print("检索到InsInfo已有数据，将直接查询InsInfo内各实例的指标数据，RegionList参数无效！")
        df = self.InsInfo.copy()
        RegionList = df['RegionId'].unique().tolist()
        # for region in RegionList:
        # 不再循环设置regionid，所有云监控数据从cn-hangzhou获取
        for metric in MetricList:
            print(f'Obtaining {metric} Data of {self.instance_type} ...')
            df_data = self.getMetricData("cn-hangzhou",df["BandwidthPackageId"].to_list(),metric,TimeDict=TimeDict)
            df_data.rename(columns={"instanceId":"BandwidthPackageId"},inplace=True)
            df = pd.merge(df,df_data,on="BandwidthPackageId",how="left")
        self.InsData = pd.concat([self.InsData,df],axis=0,ignore_index=True)
        print(f"The {self.instance_type} data is obtained!")
        

    # 未写完
    # def getEIPBwRank(self,instance_list,metric_name,query_datetime,TimeDict,region_id="cn-hangzhou"):
    #     df_temp = self.getMetricData(region_id,instance_list=instance_list,metric_name=metric_name,TimeDict=TimeDict,Period='60',rename_field_name="当前速率(bps)",KeepTimestamp=True)
    #     df_offset_temp = self.getMetricData(region_id,instance_list=instance_list,metric_name=metric_name,TimeDict=getTimeDict(end_datetime=TimeDict['end_datetime']-datetime.timedelta(days=1)),Period='60',rename_field_name="昨日速率(bps)",KeepTimestamp=True)
    #     df_data = pd.merge(df_temp,df_offset_temp,on=['instanceId'],how='left')
    #     df_data.drop(['timestamp_y','当前速率(bps)_95','昨日速率(bps)_95'],inplace=True,axis=1)
    #     df_data['timestamp_x'] = pd.to_datetime(df_data['timestamp_x'],unit="ms",origin='1970-01-01 08:00:00').dt.strftime('%H:%M')
    #     df_data['当前速率(bps)'] = df_data['当前速率(bps)'].div(1024**2).round(2)
    #     df_data["昨日速率(bps)"] = df_data["昨日速率(bps)"].div(1024**2).round(2)
    #     df_data.rename(columns={'instanceId':'AllocationId','timestamp_x':'时间','当前速率(bps)':'当前速率(Mbps)','昨日速率(bps)':'昨日速率(Mbps)'},inplace=True)
    #     df_data['差值'] = df_data['当前速率(Mbps)'] - df_data['昨日速率(Mbps)']
    #     df_data.sort_values(by=['当前速率(Mbps)'],ascending=False,inplace=True,)
    #     return df_data
                
class CBWP(AliyunInstance):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__(access_key,secret_key)
        self.namespace = "acs_bandwidth_package"
        self.ProductCategory = "sharebandwidthpackages"
        self.EIPsFromAllCBWP = None

    def getCBWPInfo(self,region_id,instance_list=None,page_size=50) -> pd.DataFrame:
        client = AcsClient(region_id=region_id, credential=self.credentials)
        request = DescribeCommonBandwidthPackagesRequest()
        request.set_accept_format('json')
        request.set_PageSize(page_size)
        request.set_PageNumber(1)
        if instance_list is None:
            response_json = json.loads(client.do_action_with_exception(request))
            df = pd.DataFrame(response_json['CommonBandwidthPackages']['CommonBandwidthPackage'])
            page_total = response_json['TotalCount'] // page_size + 1
            for page_number in range(2,page_total+1):
                request.set_PageNumber(page_number)
                response_json = json.loads(client.do_action_with_exception(request))
                df_clip = pd.DataFrame(response_json['CommonBandwidthPackages']['CommonBandwidthPackage'])
                df = pd.concat([df, df_clip],ignore_index=True)
        else:
            # Only one cbwpid can be queried, not real batch query :D
            df = pd.DataFrame()
            page_size = 1
            request.set_PageSize(page_size)
            for cbwpid in instance_list:
                request.set_BandwidthPackageId(cbwpid)
                response_json = json.loads(client.do_action_with_exception(request))
                df_clip = pd.DataFrame(response_json['CommonBandwidthPackages']['CommonBandwidthPackage'])
                df = pd.concat([df, df_clip],ignore_index=True)
        df['self_RegionId'] = region_id
        self.InsInfo = pd.concat([self.InsInfo,df],axis=0,ignore_index=True)
        return df
    
    # 可能需要优化
    def extractEIPFromInsInfo(self):
        df_EIP = pd.DataFrame()
        if self.InsInfo.empty:
            print(f'No {self.instance_type} Data Found...')
        else:
            df = self.InsInfo
            df['PublicIpAddresses'] = df['PublicIpAddresses'].apply(lambda x: x['PublicIpAddresse'])
            for index,row in df.iterrows():
                df_temp = pd.DataFrame(row['PublicIpAddresses'])
                df_temp['BandwidthPackageId'] = row['BandwidthPackageId']
                df_temp['BandwidthPackageName'] = row['Name']
                df_temp['RegionId'] = row['RegionId']
                df_EIP = pd.concat([df_EIP, df_temp],ignore_index=True)
            # print(f'Extracted EIP from {self.instance_type} Data...')
        self.EIPsFromAllCBWP = df_EIP
        return df_EIP
    
    def extractEIPFromSingleCBWP(self,PublicIpAddresses)->pd.DataFrame:
        df_data = pd.DataFrame(PublicIpAddresses['PublicIpAddresse'])
        return df_data
        
    def getEIPBandwidthRank(self,TimeDict,flow_direction='out',display_size=None):
        if flow_direction == "out":
            metric_name = "net_tx.rate"
        elif flow_direction == "in":
            metric_name = "net_rx.rate"
        else:
            raise ValueError("flow_direction must be 'out' or 'in'")
        EIPForBwRank = EIP(self.credentials)
        df_data = pd.DataFrame()
        for index,row in self.InsInfo.iterrows():
            df_cbwp_eip = self.extractEIPFromSingleCBWP(row['PublicIpAddresses']) 
            instance_list = df_cbwp_eip['AllocationId'].tolist()
            df_temp = EIPForBwRank.getMetricData(instance_list=instance_list,metric_name=metric_name,TimeDict=TimeDict,period='60',rename_field_name="查时速率(bps)")
            df_offset_temp = EIPForBwRank.getMetricData(instance_list=instance_list,metric_name=metric_name,TimeDict=getTimeDict(end_offset_days=1,end_datetime=TimeDict['end_datetime']),period='60',rename_field_name="同比速率(bps)")
            df_data = pd.concat([df_data,pd.merge(df_temp,df_offset_temp,on=['instanceId'],how='left')],ignore_index=True)
            EIPForBwRank.getEIPInfo(row['RegionId'],instance_list=instance_list)
        # formatting
        df_data.rename(columns={'instanceId':'AllocationId'},inplace=True)
        EIPForBwRank.InsInfo = pd.merge(df_data,EIPForBwRank.InsInfo,on=['AllocationId'],how='left')
        EIPForBwRank.InsInfo['当前(Mbps)'] = EIPForBwRank.InsInfo['查时速率(bps)_max'].div(1000**2).round(2)
        EIPForBwRank.InsInfo["同比(Mbps)"] = EIPForBwRank.InsInfo["同比速率(bps)_max"].div(1000**2).round(2)
        EIPForBwRank.InsInfo['差值(Mbps)'] = EIPForBwRank.InsInfo['当前(Mbps)'] - EIPForBwRank.InsInfo['同比(Mbps)']
        EIPForBwRank.InsInfo['BandwidthPackageBandwidth'] = EIPForBwRank.InsInfo['BandwidthPackageBandwidth'].astype(int)
        EIPForBwRank.InsInfo['占比(%)'] = ((EIPForBwRank.InsInfo['当前(Mbps)'] / EIPForBwRank.InsInfo['BandwidthPackageBandwidth'])*100).round(2)
        EIPForBwRank.InsInfo = EIPForBwRank.InsInfo.sort_values(by=['当前(Mbps)'],ascending=False).reset_index(drop=True)
        if display_size:
            EIPForBwRank.InsInfo = EIPForBwRank.InsInfo.head(display_size)
        EIPForBwRank.getBindedInsInfo()
        return EIPForBwRank.InsInfo
    
    # v1.0.6 ，如果self.InsInfo有数据，则直接使用，没有则获取各地域下的所有资源，正式更名为getInsData
    def getInsData(self,MetricList,TimeDict,RegionList=None,DisplayNameDict=None) -> None:
        if self.InsInfo.empty:
            # if RegionList is None:
                # RegionList = describeavialableregions().regions
            print(f'Obtaining {self.instance_type} Info in {RegionList}...')
            for RegionId in RegionList:
                self.getCBWPInfo(RegionId)
        else:
            print("检索到InsInfo已有数据，将直接查询InsInfo内各实例的指标数据，RegionList参数无效！")
        df = self.InsInfo.copy()
        RegionList = df['RegionId'].unique().tolist()
        # for region in RegionList:
        # 不再循环设置regionid，所有云监控数据从cn-hangzhou获取
        for metric in MetricList:
            print(f'Obtaining {metric} Data of {self.instance_type} ...')
            df_data = self.getMetricData("cn-hangzhou",df["BandwidthPackageId"].to_list(),metric,TimeDict=TimeDict)
            df_data.rename(columns={"instanceId":"BandwidthPackageId"},inplace=True)
            df = pd.merge(df,df_data,on="BandwidthPackageId",how="left")
        self.InsData = pd.concat([self.InsData,df],axis=0,ignore_index=True)
        print(f"The {self.instance_type} data is obtained!")

class AntiDDoS():
    def __init__(self,Credentials)-> None:
        self.instance_type = self.__class__.__name__
        self.credentials:dict = Credentials
        self.InsInfo = pd.DataFrame({"InternetIp":[],"InstanceId":[],"InstanceName":[],"InstanceType":[],"Region":[]})
    
    def getIPRegion(self,ip:str=None)-> None:
        if ip:
            client = AcsClient(region_id='cn-hangzhou', credential=self.credentials)
            request = CommonRequest()
            request.set_accept_format('json')
            request.set_domain('antiddos.aliyuncs.com')
            request.set_method('POST')
            request.set_protocol_type('https') # https | http
            request.set_version('2017-05-18')
            request.set_action_name('DescribeIpLocationService')
            request.add_query_param('InternetIp', ip)
            response_json = json.loads(client.do_action(request))
            response = response_json.get('Instance')
            if response:
                df = pd.DataFrame([response])
                self.InsInfo = pd.concat([self.InsInfo,df],axis=0,ignore_index=True)
            else:
                df = pd.DataFrame()
        else:
            df = pd.DataFrame()
        return df
            
    def getIPInfo(self):
        df = self.InsInfo[self.InsInfo['InstanceType']=="eip"]
        if not df.empty:
            IP_MoreInfo = EIP(self.credentials)
            for region in self.InsInfo['Region'].unique():
                instance_list = df[df['Region']==region]["InstanceId"].to_list()
                IP_MoreInfo.getEIPInfo(region_id=region,instance_list=instance_list)
            IP_MoreInfo.getBindedInsInfo()
            ipinfo = IP_MoreInfo.InsInfo[['AllocationId','InstanceId','InstanceName','BandwidthPackageId']].rename(columns={'InstanceId':'AssociatedInstanceId','AllocationId':'InstanceId'})
            self.InsInfo = self.InsInfo.merge(ipinfo, on='InstanceId', how='left', suffixes=('', '_B'))
            self.InsInfo['InstanceName'] = self.InsInfo['InstanceName'].fillna(self.InsInfo['InstanceName_B'])
            self.InsInfo.drop(columns=['InstanceName_B'], inplace=True)

class OSS(AliyunInstance):
    def __init__(self, Credentials):
        super().__init__(Credentials)
        self.auth = oss2.ProviderAuth(StaticCredentialsProvider(self.credentials.access_key_id, self.credentials.access_key_secret))

    def getOSSInfo(self,max_retries=3):
        service = oss2.Service(self.auth, 'https://oss-cn-hangzhou.aliyuncs.com')
        oss_list = {"Name":[],"Region":[],"StorageClass":[],"CreationDate":[],'IntranetEndpoint':[],'ExtranetEndpoint':[],"OwnerId":[],"ACLGrant":[],"DataRedundancyType":[],"AccessMonitor":[],"StorageSizeInBytes":[],"Tag":[]}
        retries = 0
        for object in oss2.BucketIterator(service):
            # oss_list['Name'].append(object.name)
            while retries < max_retries:
                try:
                    print(object.name,end="...")
                    bucket = oss2.Bucket(self.auth, 'https://oss-cn-hangzhou.aliyuncs.com', object.name)
                    oss_region = bucket.get_bucket_location().location
                    bucket_info = bucket.get_bucket_info()
                    bucket = oss2.Bucket(self.auth,bucket_info.extranet_endpoint,object.name)
                    bucket_stat = bucket.get_bucket_stat()
                    bucket_tagging = bucket.get_bucket_tagging()
                    # 先请求到所有数据再插入，简单保证原子性
                    oss_list['Region'].append(oss_region)
                    oss_list['Name'].append(bucket_info.name)
                    oss_list['StorageClass'].append(bucket_info.storage_class)
                    oss_list['CreationDate'].append(bucket_info.creation_date)
                    oss_list['IntranetEndpoint'].append(bucket_info.intranet_endpoint)
                    oss_list['ExtranetEndpoint'].append(bucket_info.extranet_endpoint)
                    oss_list['OwnerId'].append(bucket_info.owner.id)
                    oss_list['ACLGrant'].append(bucket_info.acl.grant)
                    oss_list['DataRedundancyType'].append(bucket_info.data_redundancy_type)
                    oss_list['AccessMonitor'].append(bucket_info.access_monitor)
                    # oss_list.setdefault('StorageSizeInBytes', []).append(bucket_stat.storage_size_in_bytes)
                    oss_list['StorageSizeInBytes'].append(bucket_stat.storage_size_in_bytes)
                    oss_list['Tag'].append(bucket_tagging.tag_set.tagging_rule)
                    print("获取成功")
                    break
                except requests.exceptions.ConnectTimeout:
                    retries += 1
                    time.sleep(1)
                    if retries == max_retries:
                        print(f"\nFailed to get info for bucket {object.name} after {max_retries} attempts.")
                    else:
                        print(f"Retrying({retries})...",end="")
                # OSS获取非国内数据时因线路问题可能导致超时，非代码异常，当前方案加入单线程重试，后续也可考虑跳过所有超时后，再返回重试将空缺部分回填

class CenBWP(AliyunInstance):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__(access_key,secret_key)
        self.namespace = "acs_cen"
        self.ProductCategory = "cen_area"
        self.Dimensions = 'bandwidthPackageId'

class CenRegion(AliyunInstance):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__(access_key,secret_key)
        self.namespace = "acs_cen"
        self.ProductCategory = "cen_region"
        self.Dimensions = 'cenId'

class DDoSDip(AliyunInstance):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__(access_key,secret_key)
        self.namespace = "acs_ddosdip"
        self.ProductCategory = "ddosdip"
        
class CDN(AliyunInstance):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__(access_key,secret_key)
        self.namespace = "acs_cdn"
        self.ProductCategory = "cdn"
        self.Dimensions = 'domain'

    def getInsInfo(self,region_id='cn-hangzhou',domain_name:str=None,page_size=500):
        client = AcsClient(region_id=region_id, credential=self.credentials)
        request = DescribeUserDomainsRequest()
        request.set_accept_format('json')
        request.set_PageSize(page_size)
        request.set_PageNumber(1)
        if domain_name:
            request.set_DomainName(domain_name)
        response_json = json.loads(client.do_action_with_exception(request))
        df = pd.DataFrame(response_json['Domains']['PageData'])
        page_total = response_json['TotalCount'] // page_size + 1
        for page_number in range(2,page_total+1):
            request.set_PageNumber(page_number)
            response_json = json.loads(client.do_action_with_exception(request))
            df_clip = pd.DataFrame(response_json['Domains']['PageData'])
            df = pd.concat([df, df_clip],ignore_index=True)
        self.InsInfo = pd.concat([self.InsInfo,df],axis=0,ignore_index=True)
        return df

    def getDomainBpsData(self,domain_list:list,TimeDict:dict,interval:str="300",page_size:int=500):
        # interval_list = ['300', '3600', '86400']
        # if interval not in interval_list:
        #     raise ValueError(f"Invalid interval. Please choose from {interval_list}")
        client = AcsClient(region_id='cn-hangzhou', credential=self.credentials)
        request = DescribeDomainBpsDataRequest()
        request.set_accept_format('json')
        df = pd.DataFrame()
        for idx in range(0,len(domain_list),page_size):
            batch_domain_list = domain_list[idx:idx+page_size]
            domain_str = ",".join(batch_domain_list)
            request.set_DomainName(domain_str)
            request.set_StartTime(TimeDict['start_timestring_iso8601'])
            request.set_EndTime(TimeDict['end_timestring_iso8601'])
            response_json = json.loads(client.do_action_with_exception(request))
            pprint(response_json)
            df_clip = pd.DataFrame(response_json['DomainBpsDataPerInterval']['DataModule'])
            df = pd.concat([df, df_clip], ignore_index=True)
        self.InsData = pd.concat([self.InsData, df], axis=0, ignore_index=True)
        return df