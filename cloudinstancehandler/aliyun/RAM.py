#!/usr/bin/env python
#coding=utf-8

# from cloudinstancehandler.common.funcs import *
from cloudinstancehandler.common.BasicDataFrame import *

import os
from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.acs_exception.exceptions import ClientException
from aliyunsdkcore.acs_exception.exceptions import ServerException
from aliyunsdkcore.auth.credentials import AccessKeyCredential
from aliyunsdkcore.auth.credentials import StsTokenCredential
from aliyunsdkram.request.v20150501.GetUserRequest import GetUserRequest

# Please ensure that the environment variables ALIBABA_CLOUD_ACCESS_KEY_ID and ALIBABA_CLOUD_ACCESS_KEY_SECRET are set.
credentials = AccessKeyCredential(os.environ['ALIBABA_CLOUD_ACCESS_KEY_ID'], os.environ['ALIBABA_CLOUD_ACCESS_KEY_SECRET'])
client = AcsClient(region_id='cn-shanghai', credential=credentials)

request = GetUserRequest()
request.set_accept_format('json')

response = client.do_action_with_exception(request)
# python2:  print(response) 
print(str(response, encoding='utf-8'))
