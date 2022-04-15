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

# Kyuubi AuthZ Plugin For Spark SQL

<img src="https://svn.apache.org/repos/asf/comdev/project-logos/originals/kyuubi-1.svg" alt="Kyuubi logo" width="25%" align="right" />

Security is one of the fundamental features for enterprise adoption with Kyuubi.
When deploying Kyuubi against secured clusters,
storage-based authorization is enabled by default, which only provides file-level coarse-grained authorization mode.
When row/column-level fine-grained access control is required,
we can enhance the data access model with the Kyuubi Spark AuthZ plugin.


## Storage-Based Authorization

Enabling Storage Based Authorization in the `Hive Metastore Server` uses the HDFS permissions to act as the main source for verification and allows for consistent data and metadata authorization policy.
This allows control over metadata access by verifying if the user has permission to access corresponding directories on the HDFS.
Similar with `HiveServer2`, files and directories will be mapping to hive metadata objects, such as databases, tables, partitions, and be protected from end user's queries through Kyuubi.

Storage-Based Authorization offers users with Database, Table and Partition-level coarse-gained access control.

## SQL-Standard Based Authorization with Ranger

[Apache Ranger](https://ranger.apache.org/) is a framework to enable, monitor and manage comprehensive data security across the Hadoop platform. 
This plugin enables Kyuubi with data and metadata control access ability for Spark SQL Engines, including,

- Column-level fine-grained authorization
- Row-level fine-grained authorization, a.k.a. Row-level filtering
- Data masking
