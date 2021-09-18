<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

# ACL Management Guide

- [Authorization Modes](#1)
    - [Storage-Based Authorization](#1.1)
    - [SQL-Standard Based Authorization](#1.2)
    - [Ranger Security Support](#1.3)


<h2 id="1">Authorization Modes</h2>

Three primary modes for Kyuubi authorization are available by [Submarine Spark Security](https://github.com/apache/submarine/tree/master/submarine-security/spark-security):

<h4 id="1.1">Storage-Based Authorization</h4>

Enabling Storage Based Authorization in the `Hive Metastore Server` uses the HDFS permissions to act as the main source for verification and allows for consistent data and metadata authorization policy. This allows control over metadata access by verifying if the user has permission to access corresponding directories on the HDFS. Similar with `HiveServer2`, files and directories will be translated into hive metadata objects, such as dbs, tables, partitions, and be protected from end user's queries through Kyuubi.

Storage-Based Authorization offers users with Database, Table and Partition-level coarse-gained access control.

<h4 id="1.2">SQL-Standard Based Authorization</h4>

Enabling SQL-Standard Based Authorization gives users more fine-gained control over access comparing with Storage Based Authorization. Besides of the ability of Storage Based Authorization,  SQL-Standard Based Authorization can improve it to Views and Column-level. Unfortunately, Spark SQL does not support grant/revoke statements which controls access, this might be done only through the  HiveServer2. But it's gratifying that [Submarine Spark Security](https://github.com/apache/submarine/tree/master/submarine-security/spark-security) makes Spark SQL be able to understand this fine-grain access control granted or revoked by Hive.

With [Kyuubi](https://github.com/apache/incubator-kyuubi), the SQL-Standard Based Authorization is guaranteed for the security configurations, metadata, and storage information is preserved from end users.

Please refer to the [Submarine Spark Security](https://submarine.apache.org/docs/userDocs/submarine-security/spark-security/README) in the online documentation for an overview on how to configure SQL-Standard Based Authorization for Spark SQL.

<h4 id="1.3">Ranger Security Support (Recommended)</h4>

[Apache Ranger](https://ranger.apache.org/)  is a framework to enable, monitor and manage comprehensive data security across the Hadoop platform but end before Spark or Spark SQL. The [Submarine Spark Security](https://github.com/apache/submarine/tree/master/submarine-security/spark-security) enables Kyuubi with control access ability reusing [Ranger Plugin for Hive MetaStore
](https://cwiki.apache.org/confluence/display/RANGER/Ranger+Plugin+for+Hive+MetaStore). [Apache Ranger](https://ranger.apache.org/) makes the scope of existing SQL-Standard Based Authorization expanded but without supporting Spark SQL. [Submarine Spark Security](https://github.com/apache/submarine/tree/master/submarine-security/spark-security) sticks them together.

Please refer to the [Submarine Spark Security](https://submarine.apache.org/docs/userDocs/submarine-security/spark-security/README) in the online documentation for an overview on how to configure Ranger for Spark SQL.
