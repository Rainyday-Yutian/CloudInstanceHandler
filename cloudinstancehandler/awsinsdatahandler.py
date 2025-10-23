import boto3
from pprint import pprint
import pandas as pd
import os,re

# --------------------------------- 通用 --------------------------------- #
def parse_arn(arn):
    match = re.match(r'^arn:(?P<Partition>[^:]+):(?P<Service>[^:]+):'
                    r'(?P<ResourceRegion>[^:]*):(?P<AccountID>[^:]*):'
                    r'(?P<Type>[^:/]*)(?P<ResourceSeparator>[:/])?(?P<ResourceId>.*)$', arn)
    if not match:
        return None
    parsed = match.groupdict()
    parsed['ResourceSeparator'] = match.group('ResourceSeparator') or ''
    parsed['ResourceId'] = match.group('ResourceId') or ''
    return parsed

def extract_loadbalancer(text):
    info_list = text.split('/')
    if len(info_list) >=3:
        ident = '/'.join(info_list[:3])
    else:
        ident = text
    return ident

def extract_InstanceId(dimensions):
    if dimensions and isinstance(dimensions, list) and len(dimensions) > 0:
        last_dict = dimensions[-1]
        return last_dict.get('Value') if 'Value' in last_dict else None
    return None

# --------------------------------- 基础 --------------------------------- #

__CurrentPath__ = os.path.dirname(os.path.realpath(__file__)) + "/"
__DataPath__ = __CurrentPath__ + "../data/"

class BasicDataFrame():
    def __init__(self)-> None:
        self.InsType = self.__class__.__name__
        self.Prefix = "AWS"
        self.InsInfo:pd.DataFrame = pd.DataFrame()
        self.InsData:pd.DataFrame = pd.DataFrame()
        self.__SavePath__ = __DataPath__

    def saveInsInfo(self,one_sheet=True,rename_suffix=None,Path=None,split_by="self_RegionId")-> None:
        if Path is None:
            Path = self.__SavePath__
        if rename_suffix:
            filename = Path + f'{self.Prefix}_{self.InsType}_Info_{rename_suffix}.xlsx'
        else:
            filename = Path + f'{self.Prefix}_{self.InsType}_Info.xlsx'
        print("Exporting InsInfo to Excel: ", filename)
        writer = pd.ExcelWriter(filename)
        if one_sheet:
            self.InsInfo.to_excel(writer,sheet_name="ALL",index=False)
        elif one_sheet == False and split_by in self.InsInfo.columns:
            for sample in self.InsInfo[split_by].unique():
                df_info = self.InsInfo[self.InsInfo[split_by] == sample]
                df_info.to_excel(writer,sheet_name=sample,index=False)
        else:
            # raise ValueError("split_by must be in InsInfo.columns")
            print(f"split_by '{split_by}' must be in InsInfo.columns, saveing to one sheet!")
            self.InsInfo.to_excel(writer,sheet_name="ALL",index=False)
        writer.close()
        
    def saveInsData(self,one_sheet=True,rename_suffix="Data",Path=None,split_by="self_RegionId")-> None:
        if Path is None:
            Path = self.__SavePath__
        filename = Path + f'{self.Prefix}_{self.InsType}_{rename_suffix}.xlsx'
        print("Exporting InsData to Excel: ", filename)
        writer = pd.ExcelWriter(filename)
        if one_sheet:
            self.InsData.to_excel(writer,sheet_name="All",index=False)
        elif one_sheet == False and split_by in self.InsData.columns:
            for sample in self.InsData[split_by].unique():
                df_data = self.InsData[self.InsData[split_by] == sample]
                df_data.to_excel(writer,sheet_name=sample,index=False)
        writer.close()

    def saveOtherData(self,df,rename_suffix="MoreInfo",format="xlsx",Path=None)-> None:
        if Path is None:
            Path = self.__SavePath__
        if format == "xlsx":
            filename = Path + f'{self.Prefix}_{self.InsType}_{rename_suffix}.xlsx'
            print(f"Exporting {rename_suffix} Data to Excel: ", filename)
            df.to_excel(filename,sheet_name="All",index=False)
        elif format == "csv":
            filename = Path + f'{self.Prefix}_{self.InsType}_{rename_suffix}.csv'
            print(f"Exporting {rename_suffix} Data to CSV: ", filename)
            df.to_csv(filename,index=False)
        else:
            raise ValueError("Format must be 'csv' or 'xlsx'")

    def saveAll(self)-> None:
        self.saveInsInfo()
        self.saveInsData()

    def Filter(self,df,field,string,operator="contain"):
        pass

# ----------------------------------------  全资源 ---------------------------------------------- #
class Resources(BasicDataFrame):
    def __init__(self,AK,SK):
        self.AK = AK
        self.SK = SK

    def get_resources(self,region="ap-southeast-1",resource_type_list:list=None,parse_arns=True):
        session = boto3.Session(
            aws_access_key_id = self.AK,
            aws_secret_access_key = self.SK,
            region_name = region,
        )
        resource_explorer_client = session.client('resource-explorer-2')
        df_data_resources = pd.DataFrame()

        # https://docs.aws.amazon.com/zh_cn/resource-explorer/latest/userguide/using-search-query-syntax.html
        params = {
            'Filters':{
                'FilterString':'-resourcetype:cloudwatch:alarm -resourcetype:ec2:route-table'
                },
            'MaxResults': 100
        }
        
        paginator = resource_explorer_client.get_paginator('list_resources')
        for page in paginator.paginate(**params):
            for resource in page['Resources']:
                df_data_tmp = pd.DataFrame(resource)
                df_data_resources = pd.concat([df_data_resources, df_data_tmp])
        
        # 解析并扩展ARN字段
        if parse_arns:
            parsed_arns = df_data_resources['Arn'].apply(lambda x: pd.Series(parse_arn(x)))
            # print("Parsed ARNs:",parsed_arns)
            df_data_resources = pd.concat([df_data_resources, parsed_arns], axis=1)
            df_data_resources['LastReportedAt'] = df_data_resources['LastReportedAt'].dt.tz_convert(None)
            if resource_type_list:
                df_data_resources = df_data_resources[df_data_resources['ResourceType'].isin(resource_type_list)]
            # if not df_data_resources[df_data_resources['Service']=="elasticloadbalancing"].empty:
            df_data_resources['InstanceId'] = df_data_resources['ResourceId'].apply(extract_loadbalancer)
        return df_data_resources
    
    def get_supported_resource_types(self,region="ap-southeast-1"):
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/resource-explorer-2/client/list_resources.html
        session = boto3.Session(
            aws_access_key_id = self.AK,
            aws_secret_access_key = self.SK,
            region_name = region,
        )
        resource_explorer_client = session.client('resource-explorer-2')
        response = resource_explorer_client.list_supported_resource_types(
            #     MaxResults=123,
            # NextToken='string'
        )
        df_data = pd.DataFrame(response.get('ResourceTypes', []))
        return df_data
    
class CloudWatch(BasicDataFrame):
    def __init__(self,AK,SK):
        self.AK = AK
        self.SK = SK

    def get_cloudwatch_alarms(self,region_list:list=['ap-southeast-1'],ignore_namespace=None):
        df_data = pd.DataFrame()
        for region in region_list:
            session = boto3.Session(
                aws_access_key_id=self.AK,
                aws_secret_access_key=self.SK,
                region_name=region,
            )

            client = session.client('cloudwatch')
            response = client.describe_alarms(
                AlarmTypes=['MetricAlarm'],
                MaxRecords = 100,
                # NextToken='string'
            )
            df_data_tmp = pd.DataFrame(response['MetricAlarms'])
            df_data = pd.concat([df_data, df_data_tmp])
            while True:
                next_token = response.get('NextToken')
                if next_token:
                    response = client.describe_alarms(
                        AlarmTypes=['MetricAlarm'],
                        MaxRecords = 100,
                        NextToken=next_token
                    )
                    df_data_tmp = pd.DataFrame(response['MetricAlarms'])
                    df_data = pd.concat([df_data, df_data_tmp])
                else:
                    break
        df_data['AlarmConfigurationUpdatedTimestamp'] = df_data['AlarmConfigurationUpdatedTimestamp'].dt.tz_convert(None)
        df_data['StateTransitionedTimestamp'] = df_data['StateTransitionedTimestamp'].dt.tz_convert(None)
        df_data['StateUpdatedTimestamp'] = df_data['StateUpdatedTimestamp'].dt.tz_convert(None)
        df_data['InstanceId'] = df_data['Dimensions'].apply(extract_InstanceId)
        if ignore_namespace:
            df_data = df_data[~df_data['Namespace'].isin(ignore_namespace)]
        df_data.reset_index(drop=True, inplace=True)
        return df_data

class EC2(BasicDataFrame):
    def __init__(self,AK,SK):
        self.AK = AK
        self.SK = SK
    
    def getInsInfo(self,region,instance_id_list:list=None,page_size=50):
        session = boto3.Session(
            aws_access_key_id=self.AK,
            aws_secret_access_key=self.SK,
            region_name=region
        )

        ec2_client = session.client('ec2')
        info_data = []
        for idx in range(0, len(instance_id_list), page_size):
            batch_instance_list = instance_id_list[idx:idx + page_size]
            response = ec2_client.describe_instances(InstanceIds=batch_instance_list)
            # pprint(response)
            for reservation in response.get('Reservations'):
                for instance in reservation.get('Instances'):
                    data_tmp = {}
                    data_tmp['InstanceId'] = instance['InstanceId']
                    for tag in instance.get('Tags'):
                        if tag.get('Key') == 'Name':
                            data_tmp['InstanceName'] = tag.get('Value')
                            break
                    info_data.append(data_tmp)
        df_data = pd.DataFrame(info_data)
        return df_data

if __name__ == '__main__':
    ak=""
    sk=""
    resources = Resources(ak,sk)
    print(resources.get_resources())