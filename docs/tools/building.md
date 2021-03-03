<div align=center>

![](../imgs/kyuubi_logo_simple.png)

</div>

# Building Kyuubi

## Building Kyuubi with Apache Maven

**Kyuubi** is built based on [Apache Maven](http://maven.apache.org),

```bash
./build/mvn clean package -DskipTests
```

This results in the creation of all sub-modules of Kyuubi project without running any unit test.

If you want to test it manually, you can start Kyuubi directly from the Kyuubi project root by running

```bash
bin/kyuubi start
```

## Building Submodules Individually

For instance, you can build the Kyuubi Common module using:

```bash
build/mvn clean package -pl :kyuubi-common -DskipTests
```

## Skipping Some modules

For instance, you can build the Kyuubi modules without Kyuubi Codecov and Assembly modules using:

```bash
 mvn clean install -pl '!:kyuubi-codecov,!:kyuubi-assembly' -DskipTests
```

## Defining the Apache Mirror for Spark

By default, we use `https://archive.apache.org/dist/spark/` to download the built-in Spark release package,
but if you find it hard to reach, or the downloading speed is too slow, you can define the `spark.archive.mirror`
property to a suitable Apache mirror site. For instance,

```bash
build/mvn clean package -Dspark.archive.mirror=https://mirrors.bfsu.edu.cn/apache/spark/spark-3.0.1
```

Visit [Apache Mirrors](http://www.apache.org/mirrors/) and choose a mirror based on your region.
