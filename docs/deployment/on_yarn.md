<div align=center>

![](../imgs/kyuubi_logo_simple.png)

</div>

# Running Kyuubi on Yarn

## Requirements

When you want to deploy Kyuubi's Spark SQL engines on YARN, you'd better have cognition upon the following things.

- Knowing the basics about [Running Spark on YARN](http://spark.apache.org/docs/latest/running-on-yarn.html)
- A binary distribution of Spark which is built with YARN support
  - You can use the built-in Spark distribution
  - You can get it from [Spark official website](https://spark.apache.org/downloads.html) directly
  - You can [Build Spark](http://spark.apache.org/docs/latest/building-spark.html#specifying-the-hadoop-version-and-enabling-yarn) with `-Pyarn` maven option
- An active [Apache Hadoop YARN](https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html) cluster
- An active Apache Hadoop HDFS cluster
- Setup Hadoop client configurations at the machine the Kyuubi server locates


## Configurations

### Environment

Either `HADOOP_CONF_DIR` or `YARN_CONF_DIR` is configured and points to the Hadoop client configurations directory, usually,`$HADOOP_HOME/etc/hadoop` 

If the `HADOOP_CONF_DIR` points the YARN and HDFS cluster correctly, you should be able to run the `SparkPi` example on YARN.
```bash
$ HADOOP_CONF_DIR=/path/to/hadoop/conf $SPARK_HOME/bin/spark-submit \
    --class org.apache.spark.examples.SparkPi \
    --master yarn \
    --queue thequeue \
    $SPARK_HOME/examples/jars/spark-examples*.jar \
    10
```

If the `SparkPi` passes, configure it in `$KYUUBI_HOME/conf/kyuubi-env.sh` or `$SPARK_HOME/conf/spark-env.sh`, e.g.

```bash
$ echo "export HADOOP_CONF_DIR=/path/to/hadoop/conf" >> $KYUUBI_HOME/conf/kyuubi-env.sh
```

### Spark Properties

These properties are defined by Spark and Kyuubi will pass them to `spark-submit` to create Spark applications.

**Note:** None of these would take effect if the application for a particular user already exists.

- Specify it in the JDBC connection URL, e.g. `jdbc:hive2://localhost:10009/;#spark.master=yarn;spark.yarn.queue=thequeue`
- Specify it in `$KYUUBI_HOME/conf/kyuubi-defaults.conf`
- Specify it in `$SPARK_HOME/conf/spark-defaults.conf`

**Note:** The priority goes down from top to bottom.

#### Master

Setting `spark.master=yarn` tells Kyuubi to submit Spark SQL engine applications to the YARN cluster manager.

#### Queue

Set `spark.yarn.queue=thequeue` in the JDBC connection string to tell Kyuubi to use the QUEUE in the YARN cluster, otherwise,
the QUEUE configured at Kyuubi server side will be used as default.

#### Sizing


- | Default | Meaning
--- | --- | ---
spark.yarn.am.memory | 512m | Amount of memory to use for the YARN Application Master in client mode
spark.yarn.am.memoryOverhead | amMemory * 0.10, with minimum of 384 | Amount of non-heap memory to be allocated per am process in client mode
spark.driver.memory | 1g | Amount of memory to use for the driver process
spark.driver.memoryOverhead | driverMemory * 0.10, with minimum of 384 | Amount of non-heap memory to be allocated per driver process in cluster mode
spark.executor.memory | 1g | Amount of memory to use for the executor process
spark.executor.memoryOverhead | executorMemory * 0.10, with minimum of 384 | Amount of additional memory to be allocated per executor process. This is memory that accounts for things like VM overheads, interned strings other native overheads, etc


#### 
 
 
#### Others

Acceptable [Spark properties](http://spark.apache.org/docs/latest/running-on-yarn.html#spark-properties)





