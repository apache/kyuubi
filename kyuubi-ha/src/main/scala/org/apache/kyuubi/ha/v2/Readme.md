参考 curator 项目的 curator-x-discovery 模块，部分接口如下：

+ InstanceProvider: 获取满足条件的实例
+ ProviderStrategy: 从 provider 提供的实例中选取一个实例
+ ProviderFilter: 用于 provider 中，过滤部分实例
+ InstanceSerializer: 序列化和反序列化实例对象，方便持久化更多信息，例如引擎版本、标签等信息
+ ServiceDiscoveryClient: 服务发现客户端，用于获取最终的实例

特性：
+ 支持版本过滤
+ 支持标签过滤
+ 支持随机策略
+ 支持基于 session number 的策略