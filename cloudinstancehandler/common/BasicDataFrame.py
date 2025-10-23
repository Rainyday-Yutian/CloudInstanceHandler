import os
import pandas as pd

# ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# DATA_DIR = os.path.join(ROOT_DIR, 'data')

class BasicDataFrame():
    def __init__(self)-> None:
        self.instance_type = self.__class__.__name__
        # self.prefix = "Aliyun"
        # self.CSP_Prefix = "Aliyun" 
        self.prefix = "CIH"  # Cloud Instance Data Handler / Cloud Instance Handler
        # 后期更新改成CSP_Prefix，标识清楚变量用于方便区分不同云服务商的模块
        self.data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'data')
        self.InsInfo:pd.DataFrame = pd.DataFrame()
        self.InsData:pd.DataFrame = pd.DataFrame()
        self.MetricData:pd.DataFrame = pd.DataFrame() # 模拟时序数据
        # 获取子类的文件路径和目录
        # self.SubClassFilePath = inspect.getfile(self.__class__)
        # self.SubClassDir = os.path.dirname(self.self.SubClassFilePath)

    # 对时序数据进行统计(单个字段数据，后期可以考虑多字段fields)
    def statisticMetricData(self,field_name:str,statistics_approach:list=['max'],df_data:pd.DataFrame=None,group_by:str=None,rename_field_name:str=None)-> pd.DataFrame:
        if df_data is None or df_data.empty:
            if self.MetricData.empty:
                raise ValueError("No data available for statistics.")
            df_data = self.MetricData
        if field_name not in df_data.columns:
            raise ValueError(f"Field '{field_name}' not found in DataFrame columns.")
        rename_field_name = rename_field_name or field_name

        if group_by is not None:
            if isinstance(group_by, str):
                if group_by not in df_data.columns:
                    raise ValueError(f"group_by column '{group_by}' not found in DataFrame columns.")
            elif isinstance(group_by, (list, tuple)):
                if not all(col in df_data.columns for col in group_by):
                    missing_cols = [col for col in group_by if col not in df_data.columns]
                    raise ValueError(f"group_by columns not found in DataFrame: {missing_cols}")
            else:
                raise TypeError(f"group_by must be str, list, or tuple, got {type(group_by)}")
        
        # 阿里云返回的数据中，指标统计列名有如下这些，当前方法改进作为多云厂共用，仅作参考
        # Allowed_Statistics = ["Value","Maximum","Minimum","Average","Sum"]
        # if statistic not in Allowed_Statistics:
        #     raise ValueError(f"Invalid statistic value '{statistic}'. Allowed values are: {', '.join(Allowed_Statistics)}")

        Allowed_Statistics_Approach = ['last', 'max', 'min', 'avg', 'max_95','sum','all']
        if 'all' in statistics_approach:
            statistics_approach = Allowed_Statistics_Approach[:-1]
        elif not statistics_approach:
            raise ValueError(f"Invalid statistics_approach: must contain at least one of {', '.join(Allowed_Statistics_Approach)}")
        elif not set(statistics_approach).issubset(set(Allowed_Statistics_Approach)):
            raise ValueError(f"Invalid statistics_approach value(s): {set(statistics_approach) - set(Allowed_Statistics_Approach)}. Allowed values are: {', '.join(Allowed_Statistics_Approach)}")

        # 后期可以考虑用映射表来简化代码
        # stat_map = {"max"："max"}
        # for stat in statistics_approach:
        #    if stat in stat_map:
        #        aggregation_functions[f'{rename_field_name}_{stat}'] = (field_name, stat_map[stat])
        aggregation_functions = {}
        if "last" in statistics_approach:
            # 此处需要确保数据已按时间排序，否则最后一个值可能不是最新的
            aggregation_functions[f'{rename_field_name}_last'] = (f'{field_name}', lambda x: x.iloc[-1])
        if "max" in statistics_approach:
            aggregation_functions[f'{rename_field_name}_max'] = (f'{field_name}', 'max')
        if "min" in statistics_approach:
            aggregation_functions[f'{rename_field_name}_min'] = (f'{field_name}', 'min')
        if "avg" in statistics_approach:
            aggregation_functions[f'{rename_field_name}_avg'] = (f'{field_name}', 'mean')
        if "max_95" in statistics_approach:
            aggregation_functions[f'{rename_field_name}_max_95'] = (f'{field_name}', lambda x: x.quantile(0.95))
        if "sum" in statistics_approach:
            aggregation_functions[f'{rename_field_name}_sum'] = (f'{field_name}', 'sum')
        # if "raw" in statistics_approach:
        #     aggregation_functions[f'{display_metric_name}'] = (f'{metric_name}', 'list')
        if group_by:
            df_statistics = df_data.groupby(group_by).agg(**aggregation_functions).reset_index()
        else:
            # 不分组则对整表进行统计
            df_statistics = pd.DataFrame([df_data.agg(aggregation_functions)])
            # df_statistics = df_data.agg(**aggregation_functions).reset_index()
        return df_statistics
    
    # @functools.wraps(func) # 先注释掉，日后新增日志记录功能再开启
    def _save_dataframe(self,df:pd.DataFrame,path:str=None,rename_suffix:str=None,format:str="xlsx",split_by:str=None)-> None:
        """
        path: 文件保存目录路径
        rename_suffix: 自定义重命名后缀
        format: 文件格式，限定为 "xlsx" 或 "csv"
        split_by: 如果指定，则按此字段分割数据保存到不同的表中而不是全在一个表格，例如：split_by='RegionId'。
        """
        output_dir = path if path is not None else self.data_dir
        try:
            os.makedirs(output_dir, exist_ok=True)
        except Exception as e:
            print(f"Warning: Failed to create directory {output_dir}: {e}")
            output_dir = self.data_dir
            os.makedirs(output_dir, exist_ok=True)
        if format not in ["xlsx", "csv"]:
            # raise ValueError("Format must be 'xlsx' or 'csv'")
            print("Format must be 'xlsx' or 'csv';\n Default to csv format.")
            format = "csv"
        if split_by is not None and split_by not in df.columns:
            print(f"split_by '{split_by}' must be in df.columns, saveing to one sheet!")
            split_by = None
        file_path = os.path.join(output_dir, f'{self.prefix}_{self.instance_type}')
        if rename_suffix:
            file_path += f'_{rename_suffix}'
        # file_path += f'.{format}'
        try:
            if format == "csv":
                if split_by is None:
                    file_path += '.csv'
                    df.to_csv(file_path, index=False)
                    print("Exporting data to:", file_path)
                else:
                    for sample in df[split_by].unique():
                        file_path_sample = f'{file_path}_{sample}.csv'
                        df_info = df[df[split_by] == sample]
                        df_info.to_csv(file_path_sample, index=False)
                        print("Exporting data to:", file_path_sample)
            # elif format == "xlsx":
            else:
                file_path += '.xlsx'
                print("Exporting data to Excel: ", file_path)
                with pd.ExcelWriter(file_path) as writer:
                    if split_by is None:
                        df.to_excel(writer, sheet_name="ALL", index=False)
                    else:
                        for sample in df[split_by].unique():
                            df_info = df[df[split_by] == sample]
                            df_info.to_excel(writer,sheet_name=sample,index=False)
        except Exception as e:
            file_path = os.path.join(self.data_dir, f'{self.prefix}_{self.instance_type}_Info.csv')
            print(f"Error saving data: {e};\nExporting data to csv: {file_path}")
            df.to_csv(file_path, index=False)

    def saveInsInfo(self,one_sheet:bool=True,path:str=None,rename_suffix:str=None,format:str="xlsx",split_by:str=None)-> None:
        """
        保存实例元信息数据（InsInfo）。

        Args:
            one_sheet (bool): [已废弃] 仅保留兼容性，实际由 split_by 控制。
            path (str): 自定义保存目录路径。默认使用 self.DataDir。
            rename_suffix (str): 文件名后缀，如 'Prod' → 生成 CIDH_MyClass_Info_Prod.xlsx
            format (str): 输出格式，支持 'xlsx' 或 'csv'。默认 xlsx。
            split_by (str): 按某列拆分保存，如 'RegionId'。
        """
        rename_suffix = f"Info_{rename_suffix}" if rename_suffix else "Info"
        self._save_dataframe(
            df=self.InsInfo,
            path=path,
            rename_suffix=rename_suffix,
            format=format,
            split_by=split_by
        )

    def saveInsData(self,one_sheet:bool=True,path:str=None,rename_suffix:str="Data",format:str="xlsx",split_by:str=None)-> None:
        """
        保存实例元信息数据（InsData）。

        Args:
            one_sheet (bool): [已废弃] 仅保留兼容性，实际由 split_by 控制。
            path (str): 自定义保存目录路径。默认使用 self.DataDir。
            rename_suffix (str): 文件名后缀，如 'Prod' → 生成 CIDH_MyClass_Info_Prod.xlsx
            format (str): 输出格式，支持 'xlsx' 或 'csv'。默认 xlsx。
            split_by (str): 按某列拆分保存，如 'RegionId'。
        """
        self._save_dataframe(
            df=self.InsData,
            path=path,
            rename_suffix=rename_suffix,
            format=format,
            split_by=split_by
        )

    def saveAll(self)-> None:
        self.saveInsInfo()
        self.saveInsData()

    def Filter(self,df,field,string,operator="contain"):
        pass

    # 阿里云用于生成维度的函数，此段可能需要改到子类实现
    def genDimension(self,df) -> list:
        # Dimensions_Disk = Test.InsInfo[['InstanceId', 'device_format']].to_dict('records')
        pass

# 通义点评：缺少 sheet 名安全处理（主要短板）
# import re  # 确保导入
# def _safe_sheet_name(self, name):
#     s = str(name).strip()
#     s = re.sub(r'[/\\?*\[\]:]', '-', s)  # 替换非法字符
#     s = s[:31] or 'Sheet'               # 长度 ≤31，不能为空
#     return s


# 原放置于各datahandler中，用于反查调用者的模块名，用于日志记录，存储路径等。
# 后面会设计一个统一的日志记录模块，因此弃用
# import inspect
# def printhw(message):
#     # 获取调用者的模块名
#     module_name = inspect.getmodule(inspect.currentframe().f_back).__name__
#     print(f"[{module_name}]: {message}")

# 原hwyuninsdatahandler.py中用于生成时间范围字典的方法，暂时留存
# def getTimeDict(start_offset_days=0,end_offset_days=0,start_offset_hours=0,end_offset_hours=0,start_offset_minutes=0,end_offset_minutes=0,start_datetime=None,end_datetime=None):
#     """
#     此方法基于基底时间和偏移量参数来生成时间范围字典： 起始时间或结束时间 = 基底时间 - 偏移量。
#     当end_datetime未被指定时，默认使用当前时间作为基底时间进行偏移计算。
#     当start_datetime未被指定时，默认使用end_datetime(此处指已被偏移过的end_datetime)作为基底时间进行偏移计算；
#     即起始时间和结束时间如果都未被指定，则默认使用当前时间作为基底时间进行偏移计算。
#     当end_datetime和start_datetime都被指定时，则各自分别作为起始时间和结束时间的基底时间。
#     """
#     if end_datetime is None:
#         end_datetime = datetime.datetime.now()
#     end_datetime = end_datetime - datetime.timedelta(days=end_offset_days,hours=end_offset_hours,minutes=end_offset_minutes)
#     if start_datetime:
#         start_datetime = start_datetime - datetime.timedelta(days=start_offset_days,hours=start_offset_hours,minutes=start_offset_minutes)
#     else:
#         start_datetime = end_datetime - datetime.timedelta(days=start_offset_days,hours=start_offset_hours,minutes=start_offset_minutes)
#     start_datetime.replace(second=0)
#     end_datetime.replace(second=0)
#     if start_datetime > end_datetime:
#         raise ValueError("Start time must be earlier than End time!")
#     if end_datetime-start_datetime > datetime.timedelta(days=60):
#         print("Warnings: Time range is too long!!")
#     TimeDict = {
#         "start_datetime":start_datetime,
#         "end_datetime":end_datetime,
#         "start_timestamp":int(start_datetime.timestamp()),
#         "end_timestamp":int(end_datetime.timestamp()),
#         "start_timestring":start_datetime.strftime('%Y-%m-%d %H:%M:%S'),
#         "end_timestring":end_datetime.strftime('%Y-%m-%d %H:%M:%S'),
#         "start_timestring_iso8601":start_datetime.strftime("%Y-%m-%dT%H:%M:%S")+"+08:00",
#         "end_timestring_iso8601":end_datetime.strftime("%Y-%m-%dT%H:%M:%S")+"+08:00"
#         }
#     print(f"{TimeDict['start_timestring']} -> {TimeDict['end_timestring']}")
#     return TimeDict