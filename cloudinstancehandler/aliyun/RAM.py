#!/usr/bin/env python
#coding=utf-8

# from cloudinstancehandler.common.funcs import *
# from cloudinstancehandler.common.BasicDataFrame import *
from cloudinstancehandler.aliyun.AliyunInstance import AliyunInstance

import asyncio
from typing import List

from alibabacloud_ram20150501.client import Client as Ram20150501Client
from alibabacloud_credentials.client import Client as CredentialClient
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_ram20150501 import models as ram_20150501_models
from alibabacloud_tea_util import models as util_models
from alibabacloud_tea_util.client import Client as UtilClient

class RAM(AliyunInstance):
    def __init__(self, access_key: str, secret_key: str):
        super().__init__(access_key,secret_key)
        
    def _create_client(self) -> Ram20150501Client:
        config = open_api_models.Config(
            access_key_id=self.access_key,
            access_key_secret=self.secret_key,
            endpoint="ram.aliyuncs.com"
        )
        return Ram20150501Client(config)
    
    # def fetch_instance_info(
    #     self,
    #     user_names: List[str] = None,
    #     auto_accumulate: bool = True,
    #     page_size: int = 10,
    # ) -> List[Dict[str, Any]]:  
    #     pass
    
    async def fetch_user_async(
        self,
        user_name: str
    ) -> None:
        client = self._create_client()
        request = ram_20150501_models.GetUserRequest(
            user_name=user_name
        )
        try:
            resp = await client.get_user_with_options_async(request, self.runtime)
            return resp.body.to_map()
        except Exception as error:
            error_code = getattr(error, 'code', None)
            if error_code == "EntityNotExist.User":
                return None
            raise
        
    async def fetch_users_batch_async(self, user_names: List[str], limit: int = 10) -> List[dict]:
        """
        批量查询 RAM 用户（真并发）

        Args:
            user_names: 用户名列表
            limit: 并发数量限制（默认10）

        Returns:
            List[dict]: 所有找到的用户信息列表（过滤掉不存在的用户）

        """

        if len(user_names) > 50:
            print(f"[Warning] user_names length ({len(user_names)}) exceeds recommended limit of 50")

        if not user_names:
            return []

        semaphore = asyncio.Semaphore(limit)

        async def _fetch_one_with_semaphore(name: str):
            async with semaphore:
                return await self.fetch_user_async(name)

        tasks = [_fetch_one_with_semaphore(name) for name in user_names]
        batch_results = await asyncio.gather(*tasks, return_exceptions=False)

        # 过滤掉None（不存在的用户），并提取User字段
        results = []
        for resp in batch_results:
            if resp is not None:
                results.append(resp.get("User"))

        return results