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

![](docs/imgs/kyuubi_logo.png)

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![](https://tokei.rs/b1/github.com/apache/incubator-kyuubi)](https://github.com/apache/incubator-kyuubi)
![GitHub top language](https://img.shields.io/github/languages/top/apache/incubator-kyuubi)
[![GitHub release](https://img.shields.io/github/v/release/apache/incubator-kyuubi.svg)](https://github.com/apache/incubator-kyuubi/releases)
[![codecov](https://codecov.io/gh/apache/incubator-kyuubi/branch/master/graph/badge.svg)](https://codecov.io/gh/apache/incubator-kyuubi)
[![Travis](https://api.travis-ci.com/apache/incubator-kyuubi.svg?branch=master)](https://travis-ci.com/apache/incubator-kyuubi)
![GitHub Workflow Status](https://img.shields.io/github/workflow/status/apache/incubator-kyuubi/Kyuubi/master?style=plastic)
[![Documentation Status](https://readthedocs.org/projects/kyuubi/badge/?version=latest)](https://kyuubi.apache.org/?badge=latest)

## What is Kyuubi?

Kyuubi is a distributed multi-tenant Thrift JDBC/ODBC server for large-scale data management, processing, and analytics, built on top of Apache Spark and designed to support more engines (i.e., Flink). It has been open-sourced by NetEase since 2018. We are aiming to make Kyuubi an "out-of-the-box" tool for data warehouses and data lakes.

Kyuubi provides a pure SQL gateway through Thrift JDBC/ODBC interface for end-users to manipulate large-scale data with pre-programmed and extensible Spark SQL engines. This "out-of-the-box" model minimizes the barriers and costs for end-users to use Spark at the client side. At the server-side, Kyuubi server and engines' multi-tenant architecture provides the administrators a way to achieve computing resource isolation, data security, high availability, high client concurrency, etc.

![](./docs/imgs/kyuubi_positioning.png)

- [x] A HiveServer2-like API
- [x] Multi-tenant Spark Support
- [x] Running Spark in a serverless way


### Target Users

Kyuubi's goal is to make it easy and efficient for `anyone` to use Spark(maybe other engines soon) and facilitate users to handle big data like ordinary data. Here, `anyone` means that users do not need to have a Spark technical background but a human language, SQL only. Sometimes, SQL skills are unnecessary when integrating Kyuubi with Apache Superset, which supports rich visualizations and dashboards.


In typical big data production environments with Kyuubi, there should be system administrators and end-users.

- System administrators: A small group consists of Spark experts responsible for Kyuubi deployment, configuration, and tuning.
- End-users: Focus on business data of their own, not where it stores, how it computes.

Additionally, the Kyuubi community will continuously optimize the whole system with various features, such as History-Based Optimizer, Auto-tuning, Materialized View, SQL Dialects, Functions, e.t.c.


### Usage scenarios

#### Port workloads from HiveServer2 to Spark SQL

In typical big data production environments, especially secured ones, all bundled services manage access control lists to restricting access to authorized users. For example, Hadoop YARN divides compute resources into queues. With Queue ACLs, it can identify and control which users/groups can take actions on particular queues. Similarly, HDFS ACLs control access of HDFS files by providing a way to set different permissions for specific users/groups.

Apache Spark is a unified analytics engine for large-scale data processing. It provides a Distributed SQL Engine, a.k.a, the Spark Thrift Server(STS), designed to be seamlessly compatible with HiveServer2 and get even better performance.

HiveServer2 can identify and authenticate a caller, and then if the caller also has permissions for the YARN queue and HDFS files, it succeeds. Otherwise, it fails. However, on the one hand, STS is a single Spark application. The user and queue to which STS belongs are uniquely determined at startup. Consequently, STS cannot leverage cluster managers such as YARN and Kubernetes for resource isolation and sharing or control the access for callers by the single user inside the whole system. On the other hand, the Thrift Server is coupled in the Spark driver's JVM process. This coupled architect puts a high risk on server stability and makes it unable to handle high client concurrency or apply high availability such as load balancing as it is stateful.

Kyuubi extends the use of STS in a multi-tenant model based on a unified interface and relies on the concept of multi-tenancy to interact with cluster managers to finally gain the ability of resources sharing/isolation and data security. The loosely coupled architecture of the Kyuubi server and engine dramatically improves the client concurrency and service stability of the service itself.


#### DataLake/LakeHouse Support

The vision of Kyuubi is to unify the portal and become an easy-to-use data lake management platform. Different kinds of workloads, such as ETL processing and BI analytics, can be supported by one platform, using one copy of data, with one SQL interface.

- Logical View support via Kyuubi DataLake Metadata APIs
- Multiple Catalogs support
- SQL Standard Authorization support for DataLake(coming)


#### Cloud Native Support

Kyuubi can deploy its engines on different kinds of Cluster Managers, such as, Hadoop YARN, Kubernetes, etc.


![](./docs/imgs/kyuubi_migrating_yarn_to_k8s.png)


### The Kyuubi Ecosystem(present and future)


The figure below shows our vision for the Kyuubi Ecosystem. Some of them have been realized, some in development, and others would not be possible without your help.

![](./docs/imgs/kyuubi_ecosystem.png)



## Online Documentation

Since Kyuubi 1.0.0, the Kyuubi online documentation is hosted by [https://readthedocs.org/](https://readthedocs.org/).
You can find the specific version of Kyuubi documentation as listed below.

- [master/latest](https://kyuubi.apache.org/docs/latest/)
- [1.3.0-incubating](https://kyuubi.apache.org/docs/r1.3.0-incubating/)
- [stable](https://kyuubi.apache.org/docs/stable/)

For 1.2 and earlier versions, please check the [Github Pages](https://apache.github.io/incubator-kyuubi/) directly.

## Quick Start

Ready? [Getting Started](https://kyuubi.apache.org/docs/latest/quick_start/quick_start.html) with Kyuubi.

## Contributing

All bits of help are welcome. You can make various types of contributions to Kyuubi, including the following but not limited to,

- Help new users in chat channel or share your success stories with us - [![Gitter](https://badges.gitter.im/kyuubi-on-spark/Lobby.svg)](https://gitter.im/kyuubi-on-spark/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)
- Improve Documentation - [![Documentation Status](https://readthedocs.org/projects/kyuubi/badge/?version=latest)](https://kyuubi.readthedocs.io/en/latest/?badge=latest)
- Test releases - [![GitHub release](https://img.shields.io/github/release/apache/incubator-kyuubi.svg)](https://github.com/apache/incubator-kyuubi//releases)
- Improve test coverage - [![codecov](https://codecov.io/gh/apache/incubator-kyuubi/branch/master/graph/badge.svg)](https://codecov.io/gh/apache/incubator-kyuubi)
- Report bugs and better help developers to reproduce
- Review changes
- Make a pull request
- Promote to others
- Click the star button if you like this project

Before you start, we recommend that you check the [Contribution Guidelines](https://kyuubi.apache.org/docs/latest/community/contributions.html) first.

## Aside

The project took its name from a character of a popular Japanese manga - `Naruto`.
The character is named `Kyuubi Kitsune/Kurama`, which is a nine-tailed fox in mythology.
`Kyuubi` spread the power and spirit of fire, which is used here to represent the powerful [Apache Spark](http://spark.apache.org).
Its nine tails stand for end-to-end multi-tenancy support of this project.

## License

This project is licensed under the Apache 2.0 License. See the [LICENSE](./LICENSE) file for details.
