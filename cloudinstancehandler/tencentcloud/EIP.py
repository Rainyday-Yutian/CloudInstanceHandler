from cloudinstancehandler.tencentcloud.TencentInstance import *
import os
import json
import types
from tencentcloud.common import credential
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.vpc.v20170312 import vpc_client, models
from pprint import pprint

class EIP(TencentInstance):
    def __init__(self,access_key,secret_key)-> None:  # 初始化方法，接收access_key和secret_key两个参数，并指定返回类型为None
        super().__init__(access_key,secret_key)        # 调用父类的初始化方法，传入相同的access_key和secret_key参数
        
    def AssociateEIP(self,region,eip_id,cvm_id=None,eni_id=None,private_ip=None):
        try:
            httpProfile = HttpProfile()
            httpProfile.endpoint = "vpc.tencentcloudapi.com"

            # 实例化一个client选项，可选的，没有特殊需求可以跳过
            clientProfile = ClientProfile()
            clientProfile.httpProfile = httpProfile
            # 实例化要请求产品的client对象,clientProfile是可选的
            client = vpc_client.VpcClient(self.credentials, region, clientProfile)

            # 实例化一个请求对象,每个接口都会对应一个request对象
            req = models.AssociateAddressRequest()
            params = {
                "AddressId": eip_id,
                # "InstanceId": cvm_id,
                # "NetworkInterfaceId": eni_id
            }
            if cvm_id:
                params["InstanceId"] = cvm_id
            elif eni_id:
                if not private_ip:
                    raise Exception("Please provide private_ip when binding to ENI")
                params.update({
                    "NetworkInterfaceId": eni_id,
                    "PrivateIpAddress": private_ip
                })
            
            req.from_json_string(json.dumps(params))
            resp = client.AssociateAddress(req)
            return resp.to_json_string()

        except TencentCloudSDKException as err:
            pprint(err)