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

# Deploy Kyuubi engines on Kubernetes

## Requirements

When you want to run Kyuubi's Spark SQL engine on Kubernetes, you'd better have cognition upon the following things.

* Read about [Running Spark on Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html)
* An active Kubernetes cluster
* [kubectl](https://kubernetes.io/docs/reference/kubectl/overview/)
* [kubeconfig](https://kubernetes.io/docs/concepts/configuration/organize-cluster-access-kubeconfig/) of the target cluster

## Configurations

### Master

Spark on Kubernetes configures `spark.master` by using a special format.

`spark.master=k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port>`

You can use cmd `kubectl cluster-info` to get api-server host and port.

### Deploy Mode

One of the main advantages of the Kyuubi server compared to other interactive Spark clients is that it supports cluster deploy mode.
It means that the Spark driver runs in an independent Pod which is isolated to Kyuubi server's pod.
It is highly recommended to run Spark on K8s in cluster mode.

The minimum required configurations are:

* spark.submit.deployMode cluster
* spark.kubernetes.file.upload.path (path on S3 or HDFS)
* spark.kubernetes.authenticate.driver.serviceAccountName ([viz ServiceAccount](#serviceaccount))

The vanilla Spark neither support rolling nor expiration mechanism for `spark.kubernetes.file.upload.path`, if you use
file system that does not support TTL, e.g. HDFS, additional cleanup mechanisms are needed to prevent the files in this
directory from growing indefinitely. Since Kyuubi v1.11.0, you can configure `spark.kubernetes.file.upload.path` with
placeholders `{{YEAR}}`, `{{MONTH}}` and `{{DAY}}`, and enable `kyuubi.kubernetes.spark.autoCreateFileUploadPath.enabled`
to let Kyuubi server create the directory with 777 permission automatically before submitting Spark application.

Note that, Spark would create sub dir `s"spark-upload-${UUID.randomUUID()}"` under the `spark.kubernetes.file.upload.path`
for each uploading, the administer still needs to clean up the staging directory periodically.

For example, the user can configure the below configurations in `kyuubi-defaults.conf` to enable monthly rolling support
for `spark.kubernetes.file.upload.path`

```
kyuubi.kubernetes.spark.autoCreateFileUploadPath.enabled=true
spark.kubernetes.file.upload.path=hdfs://hadoop-cluster/spark-upload-{{YEAR}}{{MONTH}}
```

and the staging files would be like

```
hdfs://hadoop-cluster/spark-upload-202412/spark-upload-f2b71340-dc1d-4940-89e2-c5fc31614eb4
hdfs://hadoop-cluster/spark-upload-202412/spark-upload-173a8653-4d3e-48c0-b8ab-b7f92ae582d6
hdfs://hadoop-cluster/spark-upload-202501/spark-upload-3b22710f-a4a0-40bb-a3a8-16e481038a63
```

then the administer can safely delete the `hdfs://hadoop-cluster/spark-upload-202412` after 20250101.

### Docker Image

Spark ships a `./bin/docker-image-tool.sh` script to build and publish the Docker images for running Spark applications on Kubernetes.

When deploying Kyuubi engines against a Kubernetes cluster, we need to set up the docker images in the Docker registry first.

Example usage is:

```shell
./bin/docker-image-tool.sh -r <repo> -t <tag> build
./bin/docker-image-tool.sh -r <repo> -t <tag> push
# To build docker image with specify openJdk 
./bin/docker-image-tool.sh -r <repo> -t <tag> -b java_image_tag=<openjdk:${java_image_tag}> build
# To build additional PySpark docker image
./bin/docker-image-tool.sh -r <repo> -t <tag> -p ./kubernetes/dockerfiles/spark/bindings/python/Dockerfile build
# To build additional SparkR docker image
./bin/docker-image-tool.sh -r <repo> -t <tag> -R ./kubernetes/dockerfiles/spark/bindings/R/Dockerfile build
```

### Test Cluster

You can use the shell code to test your cluster whether it is normal or not.

```shell
$SPARK_HOME/bin/spark-submit \
 --master k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port> \
 --class org.apache.spark.examples.SparkPi \
 --conf spark.executor.instances=5 \
 --conf spark.dynamicAllocation.enabled=false \
 --conf spark.shuffle.service.enabled=false \
 --conf spark.kubernetes.container.image=<spark-image> \
 local://<path_to_examples.jar>
```

When running shell, you can use cmd `kubectl describe pod <podName>` to check if the information meets expectations.

### ServiceAccount

When use client mode to submit application, Spark driver uses the [kubeconfig](https://kubernetes.io/docs/concepts/configuration/organize-cluster-access-kubeconfig/) to access Kubernetes API server to create and watch executor pods.

When use cluster mode to submit application, Spark driver pod uses [ServiceAccount](https://kubernetes.io/docs/concepts/security/service-accounts/) to access Kubernetes API server to create and watch executor pods.

In both cases, you need to figure out whether you have the permissions under the corresponding namespace. You can use following commands to create ServiceAccount.

```shell
# create ServiceAccount
kubectl create serviceaccount spark -n <namespace>
# binding role
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=<namespace>:spark --namespace=<namespace>
```

### Volumes

As it known to us all, Kubernetes can use configurations to mount volumes into driver and executor pods.

* hostPath: mounts a file or directory from the host node’s filesystem into a pod.
* emptyDir: an initially empty volume created when a pod is assigned to a node.
* nfs: mounts an existing NFS(Network File System) into a pod.
* persistentVolumeClaim: mounts a PersistentVolume into a pod.

Note: Please
see [the Security section](https://spark.apache.org/docs/latest/running-on-kubernetes.html#security) for security issues related to volume mounts.

```
spark.kubernetes.driver.volumes.<type>.<name>.options.path=<dist_path>
spark.kubernetes.driver.volumes.<type>.<name>.mount.path=<container_path>

spark.kubernetes.executor.volumes.<type>.<name>.options.path=<dist_path>
spark.kubernetes.executor.volumes.<type>.<name>.mount.path=<container_path>
```

Read [Using Kubernetes Volumes](https://spark.apache.org/docs/latest/running-on-kubernetes.html#using-kubernetes-volumes) for more about volumes.

### PodTemplateFile

Kubernetes allows defining pods from template files. Spark users can similarly use template files to define the driver or executor pod configurations that Spark configurations do not support.

To do so, specify the Spark properties `spark.kubernetes.driver.podTemplateFile` and `spark.kubernetes.executor.podTemplateFile` to point to local files accessible to the `spark-submit` process.

### Other

You can read Spark's official documentation for [Running on Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html) for more information.
