
from cloudinstancehandler.tencentcloud.TencentInstance import *
import os
import json
import types
from tencentcloud.common import credential
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.vpc.v20170312 import vpc_client, models
from time import sleep
import pandas


class ENI(TencentInstance):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__(access_key,secret_key)
        
    def CreateENI(self,region_id,vpc_id,subnet_id,eni_name):
        try:
            httpProfile = HttpProfile()
            httpProfile.endpoint = "vpc.tencentcloudapi.com"
            clientProfile = ClientProfile()
            clientProfile.httpProfile = httpProfile
            client = vpc_client.VpcClient(self.credentials, region_id, clientProfile)

            req = models.CreateNetworkInterfaceRequest()
            params = {
                "VpcId": vpc_id,
                "SubnetId": subnet_id,
                "NetworkInterfaceName": eni_name
            }
            req.from_json_string(json.dumps(params))
            resp = client.CreateNetworkInterface(req)
            return json.loads(resp.to_json_string())
        except TencentCloudSDKException as err:
            print(err)
            return False

    def BatchCreateENI(self,df_data_params):
        resp_list = []
        # 需要细化
        for index, row in df_data.iterrows():
            resp_list.append(self.CreateENI(row['RegionId'],row['VpcId'],row['SubnetId'],row['NetworkInterfaceName'])['NetworkInterface'])
            sleep(1)
        df_data = pd.DataFrame(resp_list)
        self.InsInfo = self.InsInfo.append(df_data,ignore_index=True)
        return df_data
    
    def AssociateSecurityGroups(self,region,eni_id_list,sg_id_list):
        # 上限为100，需要增加逻辑
        try:
            httpProfile = HttpProfile()
            httpProfile.endpoint = "vpc.tencentcloudapi.com"

            # 实例化一个client选项，可选的，没有特殊需求可以跳过
            clientProfile = ClientProfile()
            clientProfile.httpProfile = httpProfile
            # 实例化要请求产品的client对象,clientProfile是可选的
            client = vpc_client.VpcClient(self.credentials, region, clientProfile)

            # 实例化一个请求对象,每个接口都会对应一个request对象
            req = models.AssociateNetworkInterfaceSecurityGroupsRequest()
            params = {
                "NetworkInterfaceIds": eni_id_list,
                "SecurityGroupIds": sg_id_list
            }
            req.from_json_string(json.dumps(params))
            resp = client.AssociateNetworkInterfaceSecurityGroups(req)
            return json.loads(resp.to_json_string())
        except TencentCloudSDKException as err:
            print(err)

    def AssociateInstances(self,region,eni_id_list,instance_id_list):
        # try:
        pass

    
# {
#     "NetworkInterface": {
#         "NetworkInterfaceId": "eni-2klulv1n",
#         "NetworkInterfaceName": "test_create",
#         "NetworkInterfaceDescription": "",
#         "SubnetId": "subnet-dgpr6104",
#         "VpcId": "vpc-oq27mvlt",
#         "GroupSet": [],
#         "Primary": false,
#         "MacAddress": "20:90:6F:11:48:61",
#         "State": "PENDING",
#         "NetworkInterfaceState": "PENDING",
#         "PrivateIpAddressSet": [
#             {
#                 "PrivateIpAddress": "192.168.20.119",
#                 "Primary": true,
#                 "PublicIpAddress": "",
#                 "AddressId": "",
#                 "Description": "",
#                 "IsWanIpBlocked": false,
#                 "State": "PENDING",
#                 "QosLevel": null
#             }
#         ],
#         "Attachment": null,
#         "Zone": "",
#         "CreatedTime": "",
#         "Ipv6AddressSet": [],
#         "TagSet": [],
#         "EniType": 0,
#         "Business": "cvm",
#         "CdcId": "",
#         "AttachType": 0,
#         "ResourceId": null,
#         "QosLevel": null
#     },
#     "RequestId": "858416f4-942a-498d-80b0-6987d3197a6c"
# }