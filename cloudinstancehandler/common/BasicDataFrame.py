# cloudinstancehandler/common/BasicDataFrame.py
import os
import pandas as pd
from typing import List, Dict, Any, Optional

# ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# DATA_DIR = os.path.join(ROOT_DIR, 'data')

class BasicDataFrame():
    def __init__(self)-> None:
        """
        核心数据仓库：
            instance_info: pd.DataFrame 存储实例元信息；如实例ID、实例名称、所属地域等不随时间变化的属性数据，通过子类实现的fetch_instance_info方法获取并积累到此表中。
            instance_data: pd.DataFrame 存储实例数据，经过处理的数据；如精简过的instance_info数据与统计过的metric_data数据merge后存储到此表
            metric_data: pd.DataFrame 存储时序数据；如监控指标数据等随时间变化的数据，通过子类实现的fetch_metric_data方法获取并积累到此表中。
        辅助属性：
            identification_str:str 保留字段，默认 "CIH"（Cloud Instance Handler），可用于自动化生成实例的备注信息等。
            csp_name:str 标识具体实现子类所对应的云服务厂商，由子类赋值
        """
        self.instance_type = self.__class__.__name__
        self.identification_str = "CIH"
        self.csp_name = None 
        self.data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'data')
        self.instance_info:pd.DataFrame = pd.DataFrame() 
        self.instance_data:pd.DataFrame = pd.DataFrame()
        self.metric_data:pd.DataFrame = pd.DataFrame()
        # 获取子类的文件路径和目录
        # self.SubClassFilePath = inspect.getfile(self.__class__)
        # self.SubClassDir = os.path.dirname(self.self.SubClassFilePath)

    #    def _accumulate_data(
    #     self, 
    #     target_attr: str, 
    #     raw_records: Union[List[Dict[str, Any]], pd.DataFrame], 
    #     custom_field: Optional[Dict[str, Any]] = None
    # ) -> pd.DataFrame:
    #     """
    #     通用数据累积方法。
    #     将新数据追加到指定的 DataFrame 属性中。

    #     :param target_attr: 目标属性名 (字符串)，如 'instance_info', 'metric_data'
    #     :param raw_records: 新数据，可以是 List[Dict] 或 pd.DataFrame
    #     :param custom_field: 需要额外添加的列字典，如 {'RegionId': 'cn-hangzhou'}
    #     :return: 本次新增的 DataFrame (方便调用方直接使用，而不必去读 self.xxx)
    #     """
    #     # 1. 输入验证与转换
    #     if not raw_records:
    #         return pd.DataFrame()
        
    #     if isinstance(raw_records, list):
    #         if not raw_records:
    #             return pd.DataFrame()
    #         df_new = pd.DataFrame(raw_records)
    #     elif isinstance(raw_records, pd.DataFrame):
    #         if raw_records.empty:
    #             return pd.DataFrame()
    #         df_new = raw_records.copy() # 避免修改原对象
    #     else:
    #         raise TypeError(f"raw_records must be list or DataFrame, got {type(raw_records)}")

    #     # 2. 添加自定义字段
    #     if custom_field:
    #         for field, value in custom_field.items():
    #             df_new[field] = value
        
    #     # 3. 获取当前存储的目标 DataFrame
    #     if not hasattr(self, target_attr):
    #         raise AttributeError(f"Attribute '{target_attr}' does not exist in {self.__class__.__name__}")
        
    #     current_df = getattr(self, target_attr)

    #     # 4. 执行累积 (Concat)
    #     if current_df.empty:
    #         # 如果原是空的，直接赋值效率更高，且避免类型推断问题
    #         setattr(self, target_attr, df_new)
    #     else:
    #         # 忽略索引，重新生成连续索引
    #         updated_df = pd.concat([current_df, df_new], axis=0, ignore_index=True)
    #         setattr(self, target_attr, updated_df)        
    #     return df_new
    
    # def _accumulate_instance_info(self, raw_records: List[Dict[str, Any]], custom_field: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    #     """累积实例基础信息到 self.instance_info"""
    #     return self._accumulate_data('instance_info', raw_records, custom_field)

    # def _accumulate_instance_data(self, raw_records: Union[List[Dict[str, Any]], pd.DataFrame], custom_field: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    #     """累积实例详细数据到 self.instance_data"""
    #     return self._accumulate_data('instance_data', raw_records, custom_field)

    # def _accumulate_metric_data(self, raw_records: Union[List[Dict[str, Any]], pd.DataFrame], custom_field: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    #     """累积监控指标数据到 self.metric_data"""
    #     return self._accumulate_data('metric_data', raw_records, custom_field)
    def _accumulate_instance_info(self, raw_records: List[Dict[str, Any]], custom_field:dict=None) -> pd.DataFrame:
        if not raw_records:
            return pd.DataFrame()
        df = pd.DataFrame(raw_records)
        if custom_field:
            for field, value in custom_field.items():
                df[field] = value
        self.instance_info = pd.concat([self.instance_info, df], axis=0, ignore_index=True)
        return df
    
    # 对时序数据进行统计(单个字段数据，后期可以考虑多字段fields)
    def statistic_metric_data(self,field_name:str,statistics_approach:list=['max'],df_data:pd.DataFrame=None,group_by:str=None,rename_field_name:str=None)-> pd.DataFrame:
        if df_data is None or df_data.empty:
            if self.metric_data.empty:
                raise ValueError("No data available for statistics.")
            df_data = self.metric_data
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
        file_path = os.path.join(output_dir, f'{self.csp_name}_{self.instance_type}')
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
            file_path = os.path.join(self.data_dir, f'{self.csp_name}_{self.instance_type}_Info.csv')
            print(f"Error saving data: {e};\nExporting data to csv: {file_path}")
            df.to_csv(file_path, index=False)

    def save_instance_info(self,one_sheet:bool=True,path:str=None,rename_suffix:str=None,format:str="xlsx",split_by:str=None)-> None:
        """
        保存实例元信息数据（instance_info）。

        Args:
            one_sheet (bool): [已废弃] 仅保留兼容性，实际由 split_by 控制。
            path (str): 自定义保存目录路径。默认使用 self.DataDir。
            rename_suffix (str): 文件名后缀，如 'Prod' → 生成 CID_MyClass_Info_Prod.xlsx
            format (str): 输出格式，支持 'xlsx' 或 'csv'。默认 xlsx。
            split_by (str): 按某列拆分保存，如 'RegionId'。
        """
        rename_suffix = f"Info_{rename_suffix}" if rename_suffix else "Info"
        self._save_dataframe(
            df=self.instance_info,
            path=path,
            rename_suffix=rename_suffix,
            format=format,
            split_by=split_by
        )

    def save_instance_data(self,one_sheet:bool=True,path:str=None,rename_suffix:str="Data",format:str="xlsx",split_by:str=None)-> None:
        """
        保存实例元信息数据（instance_data）。

        Args:
            one_sheet (bool): [已废弃] 仅保留兼容性，实际由 split_by 控制。
            path (str): 自定义保存目录路径。默认使用 self.DataDir。
            rename_suffix (str): 文件名后缀，如 'Prod' → 生成 CIDH_MyClass_Info_Prod.xlsx
            format (str): 输出格式，支持 'xlsx' 或 'csv'。默认 xlsx。
            split_by (str): 按某列拆分保存，如 'RegionId'。
        """
        self._save_dataframe(
            df=self.instance_data,
            path=path,
            rename_suffix=rename_suffix,
            format=format,
            split_by=split_by
        )

    def save_all_data(self)-> None:
        self.save_instance_info()
        self.save_instance_data()

    def filter(self,df,field,string,operator="contain"):
        pass

    # 阿里云用于生成维度的函数，此段可能需要改到子类实现
    def generate_dimensions(self,df) -> list:
        # Dimensions_Disk = Test.instance_info[['InstanceId', 'device_format']].to_dict('records')
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