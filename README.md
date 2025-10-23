# GetAliyunInsData
## requirements
```txt
```
## CHANGELOG
### CloudInstanceDataHandler v1
* 将老版代码合并，正式成立可用项目 CloudInstanceDataHandler
* 修复华为共享带宽GEIP带宽排名中，因GEIP中有IPV6-ECS数据未适配而报错的问题





<br>

_______________________________________________________________

<br>

## TODO
### aliyuninsdatahandler v1.0.6:
* 需要重新考虑和设计类属性，比如getEIPFlowRank获取到的数据应该如何分离存储、合并存储，每次执行是否会替换原有数据，还是作为补充数据 
* 需要将getMetricData()的原始数据获取合并之后再进行数据处理，最新设计由getMetricsData或getInsData来实现
* 需要在getInsData()中判断self.InsInfo是否有数据，无数据则调用对应产品可用区扫描获取所有地域数据，有数据则查询InsInfo中指定实例的数据

### aliyuninsdatahandler v1.1.0
* ~~之前的getInsData中获取实例使用率的代码实际为复杂需求的实现逻辑，应该分离到独立文件的主入口去实现，不再作为模块程序中通用的类方法~~
* 后续考虑将获取云监控数据的方法进行与自定义统计方法的分离，以便于后续扩展
* 诸如self.getMoreInfo()则为获取更多信息,其方法仅返回更多信息的部分，v1.1.0之后版本设计会考虑为这些方法新增参数merge:bool，为True时,会将更多信息与self.InsInfo合并，缺省为True
  
### aliyuninsdatahandler v1.1.1
* getMetricsData/getInsData 需要适配多个metric以及displayname

### aliyuninsdatahandler v1.1.2 
* get\<InsType\>Info需要在至少下个版本开始重构并重命名为getInsInfo
* 可用区获取需要在下个版本开始支持
* 分页查询问题似乎也没设计好

<br>

## What's new(Old Version)


## Doc
### About Foundation Frame
**BasicDataFrame.InsInfo**    
  为实例的基底信息,由self.getInsInfo()方法获取并添置InsInfo，其方法最后返回当前单次调用获取到的数据副本  
  即self.InsInfo类似存储队列，存储该对象所有（每次）调用 self.getInsInfo() 获取到的数据  
  另外诸如self.getMoreInfo()则为获取更多信息,其方法仅返回更多信息的部分，v1.1.0之后版本设计会考虑为这些方法新增参数merge:bool，为True时,会将更多信息与self.InsInfo合并，缺省为True

**BasicDataFrame.InsData**  
为根据需求自由组合的实例数据，由self.getInsData()方法查询已保存在self.InsInfo中的实例的数据，一般为self.InsInfo + 自定义调用self.getMetricData()时返回的结果  
即执行self.getInsData()时，会根据self.InsInfo中的实例信息，调用self.getMetricData()方法获取实例数据，并添加到self.InsData中  
如共享带宽的实际InstanceId叫BandwidthPackageId，self.getInsData就需要实现适配每个云产品实际上的实例id字段名，  
如Redis有标准版、集群版等版本，版本不同导致指标名称不同，self.getInsData就需要实现适配实例类型来调整调用getMetricData中的metric_name参数  
最后拿到数据，根据并适配InsInfo字段名，将获取到的数据与InsInfo合并，添加至self.InsData中  
self.InsData的存储逻辑与self.InsInfo不同，每次执行self.getInsData()时，会清空self.InsData，然后重新添加数据

**TimeDict**  
为时间字典，用于存储时间信息，如开始时间、结束时间、~~时间间隔~~等，用于调用self.getMetricData()时传递参数  
支持多种时间格式类型，由getTimeDict()方法生成  
  

## 过期信息
### aliyuninsdatahandler v1.1.2
* 新增RDS类

### aliyuninsdatahandler v1.1.1
* 新增Cen相关实例类
* 优化 TimeDict() 时间范围生成逻辑
* 优化 getMetricData() Dimension相关处理，当Dimension维度过多或过于复杂时，支持自行传值
* 优化并同步更新为基于新版 getMetricData() 的共享带宽EIP排行获取方法 CBWP.getEIPBandwidthRank()
* 新增解析获取单个共享带宽内EIP信息方法 CBWP.extractEIPFromSingleCBWP()

### aliyuninsdatahandler v1.1.0
* 优化获取云监控最新数据的方法getMetricLast()
* 重构获取云监控数据的方法getMetricData()，并可自定义进行数据统计
* 从Instance类中拆离出新类InstanceBasicDataFrame，主要负责数据存储相关实现
* 原Instance类（后需版本已更名为AliyunInstance），主要负责存储和初始化数据获取时需要的信息，以及通用方法实现（如：云监控数据获取）
* 程序模块化设计：更多复杂需求的实现逻辑，将使用导入本模块的形式，在主程序中实现，用以更自由地实现需求细节

### liyuninsdatahandler v1.0.6
* 更新获取云监控数据获取方式，由get<InsType>Data改为getInsData，并优化获取逻辑
* 修复获取ENI绑定实例信息时，由于一个ECS实例绑定多个，查询时会重复查询到多个相同ECS信息，并且在并表时，并表键为ECS的ID，导致出现重复数据的问题
* 新增云企业网带宽包信息获取

### aliyuninsdatahandler v1.0.5
* 新增OSS信息数据获取
  
### aliyuninsdatahandler v1.0.4
* 新增账号内全局搜索IP资源已经绑定实例信息（利用AntiDDoS API实现）


### 