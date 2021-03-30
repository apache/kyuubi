<div align=center>

![](../imgs/kyuubi_logo.png)

</div>

# Running Kyuubi on Kubernetes

## Requirements

When you want to deploy Kyuubi’s Spark SQL engines on Kubernetes(use K8S as the name below), you’d better have cognition
upon the following things.

* Knowing the basics about [Running Spark on Kubernetes](http://spark.apache.org/docs/latest/running-on-kubernetes.html)
* A binary distribution of Spark which is built with K8S
* An active K8S cluster
* An active Apache Hadoop HDFS cluster
* Setup Hadoop client configurations at the machine which Kyuubi server locates

## Configurations

### TestCluster

You can use the blew shell to test you k8s cluster whether it is normal or not.

```shell
$SPARK_HOME/bin/spark-submit \
  --master k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port> \
  --class org.apache.spark.examples.SparkPi \
  --conf spark.executor.instances=5 \
  --conf spark.kubernetes.container.image=<spark-image> \
  local:///path/to/examples.jar
```

### Spark Properties

These properties are defined by Spark and Kyuubi will pass them to `spark-submit` to create Spark Applications.

Note: None of these would take effect if the application for a particular user already exists.

* Specify it in the JDBC connection URL, e.g.

`jdbc:hive2://<host>:<port>/;#spark.master=k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port>;spark.kubernetes.container.image=<spark-image>`

* Specify it in `$KYUUBI_HOME/conf/kyuubi-defaults.conf`

* Specify it in `$SPARK_HOME/conf/spark-defaults.conf`

Note: The priority goes down from top to bottom.

#### Master

Setting `spark.master=k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port>` tells Kyuubi to submit Spark SQL engine
applications to the K8S cluster manager.

You can use `kubectl cluster-info` to see where the K8S master is running.

URL must like this format, reference to the
docs [Running Spark on Kubernetes](http://spark.apache.org/docs/latest/running-on-kubernetes.html)
> The Spark master, specified either via passing the --master command line argument to spark-submit or by setting spark.
> master in the application’s configuration, must be a URL with the format k8s://<api_server_host>:<k8s-apiserver-port>. The port must always be specified, even if it’s the HTTPS port 443.
> Prefixing the master string with k8s:// will cause the Spark application to launch on the Kubernetes cluster, with the API server being contacted at api_server_url.
> If no HTTP protocol is specified in the URL, it defaults to https.
> For example, setting the master to k8s://example.com:443 is equivalent to setting it to k8s://https://example.com:443, but to connect without TLS on a different port, the master would be set to k8s://http://example.com:8080.

#### Image

Kubernetes requires users to supply images that can be deployed into containers within pods.

Spark also ships within a bin/docker-image-tool.sh script that can be used to build and publish the Docker images to use
with the K8S backend.

Example usage is:

```shell
$ ./bin/docker-image-tool.sh -r <repo> -t my-tag build
$ ./bin/docker-image-tool.sh -r <repo> -t my-tag push
 # To build additional PySpark docker image
$ ./bin/docker-image-tool.sh -r <repo> -t my-tag -p ./kubernetes/dockerfiles/spark/bindings/python/Dockerfile build
# To build additional SparkR docker image
$ ./bin/docker-image-tool.sh -r <repo> -t my-tag -R ./kubernetes/dockerfiles/spark/bindings/R/Dockerfile build
```

Also, you can build images by yourself. According
to [Running Spark on Kubernetes](http://spark.apache.org/docs/latest/running-on-kubernetes.html), you should use
customized images to achieve the goal of Security.

#### Others

You can check other acceptable configs
in [Spark properties](http://spark.apache.org/docs/latest/running-on-kubernetes.html#spark-properties)