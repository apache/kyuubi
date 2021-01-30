.. Kyuubi documentation master file, created by
   sphinx-quickstart on Wed Oct 28 14:23:28 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. image:: imgs/kyuubi_logo.png
   :align: center


Welcome to Kyuubi's documentation!
==================================

Kyuubi™ is a unified multi-tenant JDBC interface for large-scale data processing and analytics, built on top of `Apache Spark™ <http://spark.apache.org/>`_.

.. image:: ./imgs/kyuubi_layers.*

In general, the complete ecosystem of Kyuubi falls into the hierarchies shown in the above figure, with each layer loosely coupled to the other.

For example, you can use Kyuubi, Spark and `Apache Iceberg <https://iceberg.apache.org/>`_ to build and manage Data Lake with pure SQL for both data processing e.g. ETL, and analytics e.g. BI.
All workloads can be done on one platform, using one copy of data, with one SQL interface.

Kyuubi provides the following features:

Multi-tenancy
-------------

Kyuubi supports the end-to-end multi-tenancy,
and this is why we want to create this project despite that the Spark `Thrift JDBC/ODBC server <http://spark.apache.org/docs/latest/sql-distributed-sql-engine.html#running-the-thrift-jdbcodbc-server>`_ already exists.

1. Supports multi-client concurrency and authentication
2. Supports one Spark application per account(SPA).
3. Supports QUEUE/NAMESPACE Access Control Lists (ACL)
4. Supports metadata & data Access Control Lists

Users who have valid accounts could use all kinds of client tools, e.g.
Hive Beeline, `HUE <https://gethue.com/>`_, `DBeaver <https://dbeaver.io/>`_,
`SQuirreL SQL Client <http://squirrel-sql.sourceforge.net/>`_, etc,
to operate with Kyuubi server concurrently.

The SPA policy makes sure 1) a user account can only get computing resource with managed ACLs, e.g.
`Queue Access Control Lists <https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/FairScheduler.html#Queue_Access_Control_Lists>`_,
from cluster managers, e.g.
`Apache Hadoop YARN <https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html>`_,
`Kubernetes (K8s) <https://kubernetes.io/>`_ to create the Spark application;
2) a user account can only access data and metadata from a storage system, e.g.
`Apache Hadoop HDFS <https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html>`_,
with permissions.

Ease of Use
------------

You only need to be familiar with Structured Query Language (SQL) and Java Database Connectivity (JDBC) to handle massive data.
It helps you focus on the design and implementation of your business system.

Run Anywhere
------------

Kyuubi can submit Spark applications to all supported cluster managers, including YARN, Mesos, Kubernetes, Standalone, and local.

The SPA policy also make it possible for you to launch different applications against different cluster managers.

High Performance
----------------

Kyuubi is built on the Apache Spark, a lightning-fast unified analytics engine.

 - **Concurrent execution**: multiple Spark applications work together
 - **Quick response**: long-running Spark applications without startup cost
 - **Optimal execution plan**: fully supports Spark SQL Catalyst Optimizer,

Authentication & Authorization
------------------------------

With strong authentication and fine-grained column/row level authorization,
Kyuubi keeps your system and data secure.

High Availability
-----------------

Kyuubi provides both high availability and load balancing solutions based on Zookeeper.

.. toctree::
   :caption: Usage Guide
   :maxdepth: 2
   :glob:

   quick_start/index
   Deploy <deployment/index>
   Security <security/index>
   client/index
   integrations/index

.. toctree::
   :caption: Kyuubi Insider
   :maxdepth: 2

   overview/index

.. toctree::
   :caption: Contributing
   :maxdepth: 2

   tools/index
   community/index

.. toctree::
   :caption: Appendix
   :maxdepth: 2

   appendix/index
