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

<div align=center>

![](../imgs/kyuubi_logo.png)

</div>

# Terminologies

## Kyuubi

Kyuubi is a unified multi-tenant JDBC interface for large-scale data processing and analytics, built on top of Apache Spark.

### JDBC

> The Java Database Connectivity (JDBC) API is the industry standard for database-independent connectivity between the Java programming language and a wide range of databases SQL databases and other tabular data sources,
> such as spreadsheets or flat files.
> The JDBC API provides a call-level API for SQL-based database access.

> JDBC technology allows you to use the Java programming language to exploit "Write Once, Run Anywhere" capabilities for applications that require access to enterprise data.
> With a JDBC technology-enabled driver, you can connect all corporate data even in a heterogeneous environment.

<p align=right>
<em>
<a href="https://www.oracle.com/java/technologies/javase/javase-tech-database.html">https://www.oracle.com/java/technologies/javase/javase-tech-database.html</a>
</em>
</p>

Typically, there is a gap between business development and big data analytics.
If the two are forcefully coupled, it would make the corresponding system difficult to operate and optimize.
One the flip side, if decoupled, the values of both can be maximized.
Business experts can stay focused on their own business development,
while Big Data engineers can continuously optimize server-side performance and stability.
Kyuubi combines the two seamlessly through an easy-to-use JDBC interface.

#### Apache Hive

> The Apache Hive ™ data warehouse software facilitates reading, writing, and managing large datasets residing in distributed storage using SQL. Structure can be projected onto data already in storage. A command line tool and JDBC driver are provided to connect users to Hive.

<p align=right>
<em>
<a href="https://hive.apache.org/">https://hive.apache.org</a>
</em>
</p>

Kyuubi supports Hive JDBC driver, which helps you seamlessly migrate your slow queries from Hive to Spark SQL.

#### Apache Thrift

> The Apache Thrift software framework, for scalable cross-language services development, combines a software stack with a code generation engine to build services that work efficiently and seamlessly between C++, Java, Python, PHP, Ruby, Erlang, Perl, Haskell, C#, Cocoa, JavaScript, Node.js, Smalltalk, OCaml and Delphi and other languages.

<p align=right>
<em>
<a href="https://thrift.apache.org/">https://thrift.apache.org</a>
</em>
</p>

### Server

Server is a daemon process that handles concurrent connection and query requests and converting these requests into various operations against the **query engines** to complete the responses to clients.

_**Aliases: Kyuubi Server / Kyuubi Instance / k.i.**_

### ServerSpace

A ServerSpace is used to register servers and expose them together as a service layer to clients.

### Engine

An engine handles all queries through Kyuubi servers.
It is created one Kyuubi server and can be shared with other Kyuubi servers by registering itself to an engine namespace.
All its capabilities are mainly powered by Spark SQL.

_**Aliases: Query Engine / Engine Instance / e.i.**_

### EngineSpace

An EngineSpace is internally used by servers to register and interact with engines.

#### Apache Spark

> [Apache Spark™](https://spark.apache.org/) is a unified analytics engine for large-scale data processing.

<p align=right>
<em>
<a href="https://spark.apache.org">https://spark.apache.org</a>
</em>
</p>

### Multi Tenancy

Kyuubi guarantees end-to-end multi-tenant isolation and sharing in the following pipeline

```
Client --> Kyuubi --> Query Engine(Spark) --> Resource Manager --> Data Storage Layer
```

### High Availability / Load Balance

As an enterprise service, SLA commitment is essential. Deploying Kyuubi in High Availability (HA) mode helps you guarantee that.

#### Apache Zookeeper

> Apache ZooKeeper is an effort to develop and maintain an open-source server which enables highly reliable distributed coordination.

<p align=right>
<em>
<a href="https://zookeeper.apache.org/">https://zookeeper.apache.org</a>
</em>
</p>

#### Apache Curator

> Apache Curator is a Java/JVM client library for Apache ZooKeeper, a distributed coordination service. It includes a highlevel API framework and utilities to make using Apache ZooKeeper much easier and more reliable. It also includes recipes for common use cases and extensions such as service discovery and a Java 8 asynchronous DSL.

<p align=right>
<em>
<a href="https://curator.apache.org/">https://curator.apache.org</a>
</em>
</p>

## DataLake & LakeHouse

Kyuubi unifies DataLake & LakeHouse access in the simplest pure SQL way, meanwhile it's also the securest way with authentication and SQL standard authorization.

### Apache Iceberg

> Apache Iceberg is an open table format for huge analytic datasets. Iceberg adds tables to Trino and Spark that use a high-performance format that works just like a SQL table.

<p align=right>
<em>
<a href="http://iceberg.apache.org/">http://iceberg.apache.org/</a>
</em>
</p>

### Delta Lake

> Delta Lake is an open-source storage layer that brings ACID transactions to Apache Spark™ and big data workloads.

<p align=right>
<em>
<a href="https://delta.io/">https://delta.io</a>
</em>
</p>

### Apache Hudi

> Apache Hudi ingests & manages storage of large analytical datasets over DFS (hdfs or cloud stores).

<p align=right>
<em>
<a href="https://hudi.apache.org/">https://hudi.apache.org</a>
</em>
</p>
