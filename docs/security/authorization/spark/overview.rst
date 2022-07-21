.. Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.


Kyuubi AuthZ Plugin For Spark SQL
=================================

Security is one of the fundamental features for enterprise adoption with Kyuubi.
When deploying Kyuubi against secured clusters,
storage-based authorization is enabled by default, which only provides file-level
coarse-grained authorization mode.
When row/column-level fine-grained access control is required,
we can enhance the data access model with the Kyuubi Spark AuthZ plugin.

Authorization in Kyuubi
-----------------------

Storage-based Authorization
***************************

As Kyuubi supports multi tenancy, a tenant can only visit authorized resources,
including computing resources, data, etc.
Most file systems, such as HDFS, support ACL management based on files and directories.

A so called Storage-based authorization mode is supported by Kyuubi by default.
In this model, all objects, such as databases, tables, partitions, in meta layer are mapping to folders or files in the storage layer,
as well as their permissions.

Storage-based authorization offers users with database, table and partition-level coarse-gained access control.

SQL-standard authorization with Ranger
**************************************

A SQL-standard authorization usually offers a row/colum-level fine-grained access control to meet the real-world data security need.

`Apache Ranger`_ is a framework to enable, monitor and manage comprehensive data security across the Hadoop platform.
This plugin enables Kyuubi with data and metadata control access ability for Spark SQL Engines, including,

- Column-level fine-grained authorization
- Row-level fine-grained authorization, a.k.a. Row-level filtering
- Data masking

The Plugin Itself
-----------------

Kyuubi Spark Authz Plugin itself provides general purpose for ACL management for data & metadata while using Spark SQL.
It is not necessary to deploy it with the Kyuubi server and engine, and can be used as an extension for any Spark SQL jobs.
However, the authorization always requires a robust authentication layer and multi tenancy support, so Kyuubi is a perfect match.

.. _Apache Ranger: https://ranger.apache.org/