# Kyuubi  [![Build Status](https://travis-ci.org/yaooqinn/kyuubi.svg?branch=master)](https://travis-ci.org/yaooqinn/kyuubi)

**Kyuubi** is an enhanced version of [Apache Spark](http://spark.apache.org)'s primordial [Thrift JDBC/ODBC Server](http://spark.apache.org/docs/latest/sql-programming-guide.html#running-the-thrift-jdbcodbc-server).    

**Thrift JDBC/ODBC Server** as a similar servcie of Spark SQL [Apache Hive](https://hive.apache.org) [HiveServer2](https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Overview)
而存在的服务，通过该服务允许用户使用JDBC/ODBC端口协议来执行Spark SQL查询。通过Thrift JDBC/ODBC Server，业务用户就可以用使用一些支持JDBC/ODBC连接的BI工具，比如[Tableau](https://www.tableau.com/zh-cn)，[网易有数](https://youdata.163.com)等，
来对接基于Spark的海量数据报表制作，并取得比Apache Hive更好的SQL on Hadoop性能。但由于Apache Spark本身架构的限制，要作为一款企业级的产品来使用，其与HiveServer2相比
还有不少的问题存在，比如多租户隔离、权限控制、高并发、高可用等等。而Apache Spark社区对这块的支持也处于长期停滞的状态。      

**Kyuubi**针对这些存在的问题在某些方面对Thrift JDBC/ODBC Server进行的增强。具体如下表所示，

 |---|**Thrift JDBC/ODBC Server**|**Kyuubi**|备注|   
 |:---:|:---:|:---:|---|
 |SparkContext多实例|✘|√|Apache Spark对于单个JVM中实例化多个SparkContext一直有较多的尝试，可以参见[这里](https://www.jianshu.com/p/e1cfcaece8f1)；</br> 而其多实例特性可以通过`spark.driver.allowMultipleContexts`开启，也不过是SparkContext被实例化多次，并公用一套调度和执行环境而已，有点像java对象的一次浅拷贝。 </br> Kyuubi附带的Patch提供了一种以用户隔离调度和执行环境的方法。|
 |SparkContext动态实例化|✘|√|Thrift JDBC/ODBC Server在启动时初始化一个SparkContext实例，而Kyuubi则在用户会话创建时去缓存中获取或新建SparkContext|
 |SparkContext动态回收|✘|√|Thrift JDBC/ODBC Server再用户断开会话后会回收SparkSession，而SparkContext则是常驻的变量；</br> Kyuubi对于SparkSession亦如是，不同的是由于SparkContext是动态新增的，从而对应的会有相应的回收机制。|
 |动态Yarn队列|✘|√|Spark on Yarn可以通过spark.yarn.queue指定队列，Thrift JDBC/ODBC Server指定这个队列后并无法修改这个队列，</br> HiveServer2可以`set mapred.job.queue.name=thequeue`来指定执行队列， </br> Kyuubi采取了折中方案，可以将spark.yarn.queue设置连接串中。|
 |动态参数设置|仅支持`spark.sql.`开头的动态参数|√|Kyuubi支持在连接串中指定`spark.executor.cores/memory`等参数动态设置对应SparkContext所能调度的资源|
 |权限控制|✘|✘|Kyuubi后续会增加[Spark Authorizer](https://github.com/yaooqinn/spark-authorizer)的支持。|
 |代理执行|仅支持代理一位用户，server启动时通过--proxy-user指定|支持hive.server2.proxy.user；</br> 支持hive.server2.doAs||
 |多租户|✘|√|基于以上特性，Kyuubi可以在开启LCE的Yarn集群上实现多租户|
 |SQL执行日志|✘|√|HiveServer2通过LogDivertAppender来从定向SQL执行的日志到文件中，Kyuubi基于重写了该Appender得以将执行日志拉去到文件中。|
 |高可用|✘|√|Thrift JDBC/ODBC Server实际是粗糙的改写了HiveServer2的部分代码，连接ZK实现高可用的代码被阉掉了，Kyuubi把他们加回来而已|
 |Cluster模式|✘|✘|Kyuubi后续会加入cluster模式的支持|
 
 
## 快速上手

#### 编译

**Kyuubi**基于maven构建

```sbtshell
build/mvn clean package
```

#### 运行

**Kyuubi**本质上作为一个Spark Application可以轻松的用spark-submit起来

```bash
$ $SPARK_HOME/bin/spark-submit \ 
    --class yaooqinn.kyuubi.server.KyuubiServer \
    --master yarn \
    --deploy-mode client \
    --driver-memory 10g \
    --conf spark.hadoop.hive.server2.thrift.port=10009 \
    $KYUUBI_HOME/target/kyuubi-1.0.0-SNAPSHOT.jar
```

作为一个长服务，当然最好用`nohup`配合`&`来使用。但是更加推荐内置脚本来运行Kyuubi。

首先，在`$KYUUBI_HOME/bin/kyuubi-env.sh`设置好`SPARK_HOME`
```bash
export SPARK_HOME=/the/path/to/an/runable/spark/binary/dir
```

其次，通过`bin/start-kyuubi.sh`启动Kyuubi
```bash
$ bin/start-kyuubi.sh \ 
    --master yarn \
    --deploy-mode client \
    --driver-memory 10g \
    --conf spark.hadoop.hive.server2.thrift.port=10009 \
```

即可通过[beeline](https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients)等Thrift API或者[Tableau](https://www.tableau.com/zh-cn)这种工具来连接了。

最后，`bin/stop-kyuubi.sh`来停止服务。

**当然，仅仅如此的话，Kyuubi完全等价于Thrift JDBC/ODBC Server，并不具备多租户的特性。**

## 多租户

#### 前置条件

  -  [Spark On Yarn](http://spark.apache.org/docs/latest/running-on-yarn.html) 
    +  配置好Spark On Yarn
    + [LunixExecutorCantainer](https://hadoop.apache.org/docs/r2.7.2/hadoop-yarn/hadoop-yarn-site/SecureContainer.html)
    + 为不同的用户创建队列(Optional)
  -  [Thrift JDBC/ODBC Server相关要求的配置](http://spark.apache.org/docs/latest/sql-programming-guide.html#running-the-thrift-jdbcodbc-server)
    +  配置好hive-site.xml
  -  为Spark打上对应的Patch
  
