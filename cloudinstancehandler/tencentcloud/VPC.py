
from cloudinstancehandler.tencentcloud.TencentCloudInstance import TencentCloudInstance

import os
import json
import types
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.vpc.v20170312 import vpc_client, models
from pprint import pprint

class BWP(TencentCloudInstance):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__(access_key,secret_key)
        self.namespace = "QCE/BWP"
        self.dimension_name = "bandwidthPackageId"
        
        

class EIP(TencentCloudInstance):
    def __init__(self,access_key,secret_key)-> None:
        super().__init__(access_key,secret_key)
        
    def get_instance_info():
        pass
        
    def AssociateEIP(self,region,address_id:str,instance_id:str=None,network_interface_id:str=None,private_ip_address:str=None):
        try:
            httpProfile = HttpProfile()
            httpProfile.endpoint = "vpc.tencentcloudapi.com"

            clientProfile = ClientProfile()
            clientProfile.httpProfile = httpProfile
            client = vpc_client.VpcClient(self.credentials, region, clientProfile)

            req = models.AssociateAddressRequest()
            params = {
                "AddressId": address_id,
                # "InstanceId": cvm_id,
                # "NetworkInterfaceId": eni_id
            }
            if instance_id:
                params["InstanceId"] = instance_id
            elif network_interface_id:
                if not private_ip_address:
                    raise Exception("Please provide private_ip when binding to ENI")
                params.update({
                    "NetworkInterfaceId": network_interface_id,
                    "PrivateIpAddress": private_ip_address
                })
            
            req.from_json_string(json.dumps(params))
            resp = client.AssociateAddress(req)
            return resp.to_json_string()

        except TencentCloudSDKException as err:
            pprint(err)

class ENI(TencentCloudInstance):
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
            # return False

    # def BatchCreateENI(self,df_data_params):
    #     resp_list = []
    #     # 需要细化
    #     for index, row in df_data.iterrows():
    #         resp_list.append(self.CreateENI(row['RegionId'],row['VpcId'],row['SubnetId'],row['NetworkInterfaceName'])['NetworkInterface'])
    #         sleep(1)
    #     df_data = pd.DataFrame(resp_list)
    #     self.InsInfo = self.InsInfo.append(df_data,ignore_index=True)
    #     return df_data
    
    def AssociateSecurityGroups(self,region,network_interface_ids:list,security_group_ids:list,limit=100):
        # 上限为100，需要增加逻辑
        if limit>100 or limit<1:
            print('limit is out of range, set to 100')
            limit = 100
            
        resp_list = []
        try:
            httpProfile = HttpProfile()
            httpProfile.endpoint = "vpc.tencentcloudapi.com"

            clientProfile = ClientProfile()
            clientProfile.httpProfile = httpProfile
            # 实例化要请求产品的client对象,clientProfile是可选的
            client = vpc_client.VpcClient(self.credentials, region, clientProfile)

            # 实例化一个请求对象,每个接口都会对应一个request对象
            req = models.AssociateNetworkInterfaceSecurityGroupsRequest()
            for i in range(0,len(network_interface_ids),limit):
                network_interface_ids_temp = network_interface_ids[i:i+limit]
                params = {
                    "NetworkInterfaceIds": network_interface_ids_temp,
                    "SecurityGroupIds": security_group_ids
                }
                req.from_json_string(json.dumps(params))
                resp = client.AssociateNetworkInterfaceSecurityGroups(req)
                resp_list.append(json.loads(resp.to_json_string()))
            return resp_list
        except TencentCloudSDKException as err:
            print(err)
            return False

    def AssociateInstance(self,region,eni_id,instance_id):
        try:
            
            httpProfile = HttpProfile()
            httpProfile.endpoint = "vpc.tencentcloudapi.com"
            
            clientProfile = ClientProfile()
            clientProfile.httpProfile = httpProfile

            client = vpc_client.VpcClient(self.credentials, region, clientProfile)

            req = models.AttachNetworkInterfaceRequest()
            params = {
                "NetworkInterfaceId": eni_id,
                "InstanceId": instance_id
            }
            req.from_json_string(json.dumps(params))

            resp = client.AttachNetworkInterface(req)
            print(resp.to_json_string())
        except TencentCloudSDKException as err:
            print(err)

    
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