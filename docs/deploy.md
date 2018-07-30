# Kyuubi Deployment Guide

This document gives you a quick overview of how Kyuubi runs on clusters. We first need to distinguish the concept of
deploy mode for Kyuubi and Spark in order to describe this topic as clearly as possible.

**1. How Kyuubi submit Spark applications**(client)

**2. How to submit Kyuubi server itself**(client/cluster)

Spark supports many kinds of [cluster manager types](http://spark.apache.org/docs/latest/cluster-overview.html#cluster-manager-types)
for deploying itself. The cluster manager refers to an external service for acquiring resources on the cluster (e.g. k8s, YARN).
Spark applications can be submitted to a cluster in two different kinds of deploy mode distinguished by where the driver process runs.
In client mode, the driver is launched outside the cluster, while in cluster mode the driver inside. The driver usually refers
to where the SparkContext instance lives.

Different from ordinary Spark applications, Kyuubi manages multiple SparkContext instances in the Kyuubi server JVM.
In other words, Kyuubi supports submitting Spark applications only in client mode under current implementation.

For running the Kyuubi server, we also support launching the server instance in two different ways. One is to launch it
in a local machine(a.k.a client mode), the other in a YARN Container(a.k.a cluster mode).

<h2 id="1">Cluster Manager</h2>

Although Spark currently supports several cluster managers, such as [Standalone](http://spark.apache.org/docs/latest/spark-standalone.html),
[Apache Mesos](http://spark.apache.org/docs/latest/running-on-mesos.html), [Kubernetes](http://spark.apache.org/docs/latest/running-on-kubernetes.html),
and [Hadoop YARN](http://spark.apache.org/docs/latest/running-on-yarn.html), we choose the [Hadoop YARN](http://spark.apache.org/docs/latest/running-on-yarn.html) as
as the first-class support to gain better compatibility and multi tenancy on Hadoop clusters.

Kyuubi cluster mode only support on YARN.

<h2 id="2">Launching Kyuubi at local</h2>

<h4 id="2.1">Preparations</h4>

Running Kyuubi on YARN requires:

- A binary distribution of Kyuubi, which can be download from the [downloads page](https://github.com/yaooqinn/kyuubi/releases) or [Building Kyuubi](https://yaooqinn.github.io/kyuubi/docs/building.html) by yourself. 
- A binary distribution of Spark which is built with YARN support, which can be downloaded from the [spark downloads page](http://spark.apache.org/downloads.html) or [Building Spark](http://spark.apache.org/docs/latest/building-spark.html) by yourself.

<h4 id="2.2">Configurations</h4>

Make sure that `HADOOP_CONF_DIR` or `YARN_CONF_DIR` points to the directory which contains the client side configurations.
files for the Hadoop cluster.  

For example(in kyuubi-env.sh/spark-env.sh):

```bash
export HADOOP_CONF_DIR=/path/to/hadoop/conf
```

These configurations are used to read/write system staging files and data files to HDFS, and connect to the ResourceManager.

Kyuubi relays `SPARK_HOME` to identify Spark and other dependencies, so export `SPARK_HOME` in `$KYUUBI_HOME/bin/kyuubi-env.sh`

```bash
export SPARK_HOME=/the/path/to/a/runable/spark/binary/dir
```

To correctly connect the Hive Metastore, we need to configure `hive-site.xml` in `SPARK_HOME/conf` directory.

<h4 id="2.3">Startup</h4>

If this is the first time to play with Kyuubi, we suggest you that execute `SPARK_HOME/bin/spark-sql` and run some test
sql statement  to verify the Spark/Yarn/Hive client are all ready and correct at the very beginning.

And then the last, start Kyuubi with  `bin/start-kyuubi.sh`
```bash
$ bin/start-kyuubi.sh \ 
    --master yarn \
    --deploy-mode client \
    --driver-memory 10g \
    --conf spark.kyuubi.frontend.bind.port=10009
```

This will launch Kyuubi server at the machine you execute the script.

<h4 id="2.4">Additions</h4>

Please refer to the [Configuration Guide](https://yaooqinn.github.io/kyuubi/docs/configurations.html) in the online documentation for an overview on how to configure Kyuubi.

Please refer to the [Kyuubi Containerization Guide](https://yaooqinn.github.io/kyuubi/docs/containerization.html) in the online documentation to learn how to enable Kyuubi on YARN cluster.

## Additional Documentations
[Building Kyuubi](https://yaooqinn.github.io/kyuubi/docs/building.html)  
[Configuration Guide](https://yaooqinn.github.io/kyuubi/docs/configurations.html)  
[Authentication/Security Guide](https://yaooqinn.github.io/kyuubi/docs/authentication.html)  
[Kyuubi ACL Management Guide](https://yaooqinn.github.io/kyuubi/docs/authorization.html)  
[Kyuubi Architecture](https://yaooqinn.github.io/kyuubi/docs/architecture.html)  
[Home Page](https://yaooqinn.github.io/kyuubi/)
 
