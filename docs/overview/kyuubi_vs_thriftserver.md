

**Kyuubi** is an enhanced edition of the [Apache Spark](http://spark.apache.org)'s primordial
[Thrift JDBC/ODBC Server](http://spark.apache.org/docs/latest/sql-programming-guide.html#running-the-thrift-jdbcodbc-server).
In enterprise computing

It is mainly designed for directly running SQL towards a cluster with all components including HDFS, YARN, Hive MetaStore,
and itself secured. The main purpose of Kyuubi is to realize an architecture that can not only speed up SQL queries using
Spark SQL Engine, and also be compatible with the HiveServer2's behavior as much as possible. Thus, Kyuubi use the same protocol
of HiveServer2, which can be found at [HiveServer2 Thrift API](https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Thrift+API)
as the client-server communication mechanism, and a user session level `SparkContext` instantiating / registering / caching / recycling
mechanism to implement multi-tenant functionality.