<div align=center>

![](../imgs/kyuubi_logo_simple.png)

</div>

# Trouble Shooting

## Common Issues
### java.lang.UnsupportedClassVersionError .. Unsupported major.minor version 52.0
```java
Exception in thread "main" java.lang.UnsupportedClassVersionError: org/apache/kyuubi/server/KyuubiServer : Unsupported major.minor version 52.0
	at java.lang.ClassLoader.defineClass1(Native Method)
	at java.lang.ClassLoader.defineClass(ClassLoader.java:803)
	at java.security.SecureClassLoader.defineClass(SecureClassLoader.java:142)
	at java.net.URLClassLoader.defineClass(URLClassLoader.java:442)
	at java.net.URLClassLoader.access$100(URLClassLoader.java:64)
	at java.net.URLClassLoader$1.run(URLClassLoader.java:354)
	at java.net.URLClassLoader$1.run(URLClassLoader.java:348)
	at java.security.AccessController.doPrivileged(Native Method)
	at java.net.URLClassLoader.findClass(URLClassLoader.java:347)
	at java.lang.ClassLoader.loadClass(ClassLoader.java:425)
	at sun.misc.Launcher$AppClassLoader.loadClass(Launcher.java:312)
	at java.lang.ClassLoader.loadClass(ClassLoader.java:358)
	at sun.launcher.LauncherHelper.checkAndLoadMain(LauncherHelper.java:482)
```

Firstly, you should check the version of Java JRE used to run Kyuubi is actually matched with the version of Java compiler used to build Kyuubi.

```bash
$ java -version
java version "1.7.0_171"
OpenJDK Runtime Environment (rhel-2.6.13.2.el7-x86_64 u171-b01)
OpenJDK 64-Bit Server VM (build 24.171-b01, mixed mode)
```

```bash
$ cat RELEASE
Kyuubi 1.0.0-SNAPSHOT (git revision 39e5da5) built for
Java 1.8.0_251
Scala 2.12
Spark 3.0.1
Hadoop 2.7.4
Hive 2.3.7
Build flags:
```

To fix this problem you should export `JAVA_HOME` with a compatible one in `conf/kyuubi-env.sh`

```bash
echo "export JAVA_HOME=/path/to/jdk1.8.0_251" >> conf/kyuubi-env.sh
```

### org.apache.spark.SparkException: When running with master 'yarn' either HADOOP_CONF_DIR or YARN_CONF_DIR must be set in the environment

```java
Exception in thread "main" org.apache.spark.SparkException: When running with master 'yarn' either HADOOP_CONF_DIR or YARN_CONF_DIR must be set in the environment.
	at org.apache.spark.deploy.SparkSubmitArguments.error(SparkSubmitArguments.scala:630)
	at org.apache.spark.deploy.SparkSubmitArguments.validateSubmitArguments(SparkSubmitArguments.scala:270)
	at org.apache.spark.deploy.SparkSubmitArguments.validateArguments(SparkSubmitArguments.scala:233)
	at org.apache.spark.deploy.SparkSubmitArguments.<init>(SparkSubmitArguments.scala:119)
	at org.apache.spark.deploy.SparkSubmit$$anon$2$$anon$3.<init>(SparkSubmit.scala:990)
	at org.apache.spark.deploy.SparkSubmit$$anon$2.parseArguments(SparkSubmit.scala:990)
	at org.apache.spark.deploy.SparkSubmit.doSubmit(SparkSubmit.scala:85)
	at org.apache.spark.deploy.SparkSubmit$$anon$2.doSubmit(SparkSubmit.scala:1007)
	at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:1016)
	at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
```

When Kyuubi gets the `spark.master=yarn`, `HADOOP_CONF_DIR` should also be exported in `$KYUUBI_HOME/conf/kyuubi-env.sh`.

To fix this problem you should export `HADOOP_CONF_DIR` to the folder that contains the hadoop client settings in `conf/kyuubi-env.sh`.

```bash
echo "export HADOOP_CONF_DIR=/path/to/hadoop/conf" >> conf/kyuubi-env.sh
```


### javax.security.sasl.SaslException: GSS initiate failed [Caused by GSSException: No valid credentials provided (Mechanism level: Failed to find any Kerberos tgt)];


### org.apache.hadoop.security.AccessControlException: Permission denied: user=hzyanqin, access=WRITE, inode="/user":hdfs:hdfs:drwxr-xr-x

```java
org.apache.hadoop.security.AccessControlException: Permission denied: user=hzyanqin, access=WRITE, inode="/user":hdfs:hdfs:drwxr-xr-x
	at org.apache.hadoop.hdfs.server.namenode.FSPermissionChecker.check(FSPermissionChecker.java:350)
	at org.apache.hadoop.hdfs.server.namenode.FSPermissionChecker.checkPermission(FSPermissionChecker.java:251)
	at org.apache.ranger.authorization.hadoop.RangerHdfsAuthorizer$RangerAccessControlEnforcer.checkPermission(RangerHdfsAuthorizer.java:306)
	at org.apache.hadoop.hdfs.server.namenode.FSPermissionChecker.checkPermission(FSPermissionChecker.java:189)
	at org.apache.hadoop.hdfs.server.namenode.FSDirectory.checkPermission(FSDirectory.java:1767)
	at org.apache.hadoop.hdfs.server.namenode.FSDirectory.checkPermission(FSDirectory.java:1751)
	at org.apache.hadoop.hdfs.server.namenode.FSDirectory.checkAncestorAccess(FSDirectory.java:1710)
	at org.apache.hadoop.hdfs.server.namenode.FSDirMkdirOp.mkdirs(FSDirMkdirOp.java:60)
	at org.apache.hadoop.hdfs.server.namenode.FSNamesystem.mkdirs(FSNamesystem.java:3062)
	at org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer.mkdirs(NameNodeRpcServer.java:1156)
	at org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolServerSideTranslatorPB.mkdirs(ClientNamenodeProtocolServerSideTranslatorPB.java:652)
	at org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos$ClientNamenodeProtocol$2.callBlockingMethod(ClientNamenodeProtocolProtos.java)
	at org.apache.hadoop.ipc.ProtobufRpcEngine$Server$ProtoBufRpcInvoker.call(ProtobufRpcEngine.java:503)
	at org.apache.hadoop.ipc.RPC$Server.call(RPC.java:989)
	at org.apache.hadoop.ipc.Server$RpcCall.run(Server.java:871)
	at org.apache.hadoop.ipc.Server$RpcCall.run(Server.java:817)
	at java.security.AccessController.doPrivileged(Native Method)
	at javax.security.auth.Subject.doAs(Subject.java:422)
	at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1893)
	at org.apache.hadoop.ipc.Server$Handler.run(Server.java:2606)

	at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
	at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
	at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
	at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
	at org.apache.hadoop.ipc.RemoteException.instantiateException(RemoteException.java:106)
	at org.apache.hadoop.ipc.RemoteException.unwrapRemoteException(RemoteException.java:73)
	at org.apache.hadoop.hdfs.DFSClient.primitiveMkdir(DFSClient.java:3007)
	at org.apache.hadoop.hdfs.DFSClient.mkdirs(DFSClient.java:2975)
	at org.apache.hadoop.hdfs.DistributedFileSystem$21.doCall(DistributedFileSystem.java:1047)
	at org.apache.hadoop.hdfs.DistributedFileSystem$21.doCall(DistributedFileSystem.java:1043)
	at org.apache.hadoop.fs.FileSystemLinkResolver.resolve(FileSystemLinkResolver.java:81)
	at org.apache.hadoop.hdfs.DistributedFileSystem.mkdirsInternal(DistributedFileSystem.java:1061)
	at org.apache.hadoop.hdfs.DistributedFileSystem.mkdirs(DistributedFileSystem.java:1036)
	at org.apache.hadoop.fs.FileSystem.mkdirs(FileSystem.java:1881)
	at org.apache.hadoop.fs.FileSystem.mkdirs(FileSystem.java:600)
	at org.apache.spark.deploy.yarn.Client.prepareLocalResources(Client.scala:441)
	at org.apache.spark.deploy.yarn.Client.createContainerLaunchContext(Client.scala:876)
	at org.apache.spark.deploy.yarn.Client.submitApplication(Client.scala:196)
	at org.apache.spark.scheduler.cluster.YarnClientSchedulerBackend.start(YarnClientSchedulerBackend.scala:60)
	at org.apache.spark.scheduler.TaskSchedulerImpl.start(TaskSchedulerImpl.scala:201)
	at org.apache.spark.SparkContext.<init>(SparkContext.scala:555)
	at org.apache.spark.SparkContext$.getOrCreate(SparkContext.scala:2574)
	at org.apache.spark.sql.SparkSession$Builder.$anonfun$getOrCreate$2(SparkSession.scala:934)
	at scala.Option.getOrElse(Option.scala:189)
	at org.apache.spark.sql.SparkSession$Builder.getOrCreate(SparkSession.scala:928)
	at org.apache.kyuubi.engine.spark.SparkSQLEngine$.createSpark(SparkSQLEngine.scala:72)
	at org.apache.kyuubi.engine.spark.SparkSQLEngine$.main(SparkSQLEngine.scala:101)
	at org.apache.kyuubi.engine.spark.SparkSQLEngine.main(SparkSQLEngine.scala)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at org.apache.spark.deploy.JavaMainApplication.start(SparkApplication.scala:52)
	at org.apache.spark.deploy.SparkSubmit.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:928)
	at org.apache.spark.deploy.SparkSubmit$$anon$1.run(SparkSubmit.scala:165)
	at org.apache.spark.deploy.SparkSubmit$$anon$1.run(SparkSubmit.scala:163)
	at java.security.AccessController.doPrivileged(Native Method)
	at javax.security.auth.Subject.doAs(Subject.java:422)
	at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1746)
	at org.apache.spark.deploy.SparkSubmit.doRunMain$1(SparkSubmit.scala:163)
	at org.apache.spark.deploy.SparkSubmit.submit(SparkSubmit.scala:203)
	at org.apache.spark.deploy.SparkSubmit.doSubmit(SparkSubmit.scala:90)
	at org.apache.spark.deploy.SparkSubmit$$anon$2.doSubmit(SparkSubmit.scala:1007)
	at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:1016)
	at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
```

The user do not have permission to create to Hadoop home dir, which is `/user/hzyanqin` in the case above.

To fix this problem you need to create this directory first and grant ACL permission for `hzyanqin`.


### org.apache.thrift.TApplicationException: Invalid method name: 'get_table_req'

```java
Caused by: org.apache.thrift.TApplicationException: Invalid method name: 'get_table_req'
	at org.apache.thrift.TServiceClient.receiveBase(TServiceClient.java:79)
	at org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore$Client.recv_get_table_req(ThriftHiveMetastore.java:1567)
	at org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore$Client.get_table_req(ThriftHiveMetastore.java:1554)
	at org.apache.hadoop.hive.metastore.HiveMetaStoreClient.getTable(HiveMetaStoreClient.java:1350)
	at org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient.getTable(SessionHiveMetaStoreClient.java:127)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at org.apache.hadoop.hive.metastore.RetryingMetaStoreClient.invoke(RetryingMetaStoreClient.java:173)
	at com.sun.proxy.$Proxy37.getTable(Unknown Source)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at org.apache.hadoop.hive.metastore.HiveMetaStoreClient$SynchronizedHandler.invoke(HiveMetaStoreClient.java:2336)
	at com.sun.proxy.$Proxy37.getTable(Unknown Source)
	at org.apache.hadoop.hive.ql.metadata.Hive.getTable(Hive.java:1274)
	... 93 more
```

This error means that you are using incompatible version of Hive metastore client to connect the Hive metastore server.

To fix this problem you could use a compatible version for Hive client by configuring
`spark.sql.hive.metastore.jars` and `spark.sql.hive.metastore.version` at Spark side.


### hive.server2.thrift.max.worker.threads

```java
Unexpected end of file when reading from HS2 server. The root cause might be too many concurrent connections. Please ask the administrator to check the number of active connections, and adjust hive.server2.thrift.max.worker.threads if applicable.
Error: org.apache.thrift.transport.TTransportException (state=08S01,code=0)
```

In Kyuubi, we should increase `kyuubi.frontend.min.worker.threads` instead of `hive.server2.thrift.max.worker.threads`

### Failed to create function using jar
`CREATE TEMPORARY FUNCTION TEST AS 'com.netease.UDFTest' using jar 'hdfs:///tmp/udf.jar'`

```java
Error operating EXECUTE_STATEMENT: org.apache.spark.sql.AnalysisException: Can not load class 'com.netease.UDFTest' when registering the function 'test', please make sure it is on the classpath;
	at org.apache.spark.sql.catalyst.catalog.SessionCatalog.$anonfun$registerFunction$1(SessionCatalog.scala:1336)
	at scala.Option.getOrElse(Option.scala:189)
	at org.apache.spark.sql.catalyst.catalog.SessionCatalog.registerFunction(SessionCatalog.scala:1333)
	at org.apache.spark.sql.execution.command.CreateFunctionCommand.run(functions.scala:82)
	at org.apache.spark.sql.execution.command.ExecutedCommandExec.sideEffectResult$lzycompute(commands.scala:70)
	at org.apache.spark.sql.execution.command.ExecutedCommandExec.sideEffectResult(commands.scala:68)
	at org.apache.spark.sql.execution.command.ExecutedCommandExec.executeCollect(commands.scala:79)
	at org.apache.spark.sql.Dataset.$anonfun$logicalPlan$1(Dataset.scala:229)
	at org.apache.spark.sql.Dataset.$anonfun$withAction$1(Dataset.scala:3618)
	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$5(SQLExecution.scala:100)
	at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:160)
	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:87)
	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:764)
	at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:64)
	at org.apache.spark.sql.Dataset.withAction(Dataset.scala:3616)
	at org.apache.spark.sql.Dataset.<init>(Dataset.scala:229)
	at org.apache.spark.sql.Dataset$.$anonfun$ofRows$2(Dataset.scala:100)
	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:764)
	at org.apache.spark.sql.Dataset$.ofRows(Dataset.scala:97)
	at org.apache.spark.sql.SparkSession.$anonfun$sql$1(SparkSession.scala:607)
	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:764)
	at org.apache.spark.sql.SparkSession.sql(SparkSession.scala:602)
	at org.apache.kyuubi.engine.spark.operation.ExecuteStatement.org$apache$kyuubi$engine$spark$operation$ExecuteStatement$$executeStatement(ExecuteStatement.scala:64)
	at org.apache.kyuubi.engine.spark.operation.ExecuteStatement$$anon$1.run(ExecuteStatement.scala:80)
	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
	at java.util.concurrent.FutureTask.run(FutureTask.java:266)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
	at java.lang.Thread.run(Thread.java:745)
```

If you get this exception when creating a function, you can check your JDK version.
You should update JDK to JDK1.8.0_121 and later, since JDK1.8.0_121 fix a security issue [Additional access restrictions for URLClassLoader.newInstance](https://www.oracle.com/java/technologies/javase/8u121-relnotes.html).

### Failed to start Spark 3.1 with error msg 'Cannot modify the value of a Spark config'
Here is the error message
```java
Caused by: org.apache.spark.sql.AnalysisException: Cannot modify the value of a Spark config: spark.yarn.queue
	at org.apache.spark.sql.RuntimeConfig.requireNonStaticConf(RuntimeConfig.scala:156)
	at org.apache.spark.sql.RuntimeConfig.set(RuntimeConfig.scala:40)
	at org.apache.kyuubi.engine.spark.session.SparkSQLSessionManager.$anonfun$openSession$2(SparkSQLSessionManager.scala:68)
	at org.apache.kyuubi.engine.spark.session.SparkSQLSessionManager.$anonfun$openSession$2$adapted(SparkSQLSessionManager.scala:56)
	at scala.collection.immutable.Map$Map4.foreach(Map.scala:236)
	at org.apache.kyuubi.engine.spark.session.SparkSQLSessionManager.openSession(SparkSQLSessionManager.scala:56)
	... 12 more
```

This is because Spark-3.1 will check the config which you set and throw exception if the config is static or used in other module (e.g. yarn/core).
The related PR is https://github.com/apache/spark/pull/27062.

You can add a config `spark.sql.legacy.setCommandRejectsSparkCoreConfs=false` in `spark-defaults.conf` to disable this behavior.
