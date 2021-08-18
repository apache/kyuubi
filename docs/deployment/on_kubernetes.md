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

# Deploy Kyuubi engines on Kubernetes

## Requirements

When you want to run Kyuubi's Spark SQL engines on Kubernetes, you'd better have cognition upon the following things.

* Read about [Running Spark On Kubernetes](http://spark.apache.org/docs/latest/running-on-kubernetes.html)
* An active Kubernetes cluster
* [Kubectl](https://kubernetes.io/docs/reference/kubectl/overview/)
* KubeConfig of the target cluster

## Configurations

### Master

Spark on Kubernetes config master by using a special format.

`spark.master=k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port>`

You can use cmd `kubectl cluster-info` to get api-server host and port.

### Docker Image

Spark ships a `./bin/docker-image-tool.sh` script to build and publish the Docker images for running Spark applications on Kubernetes.

When deploying Kyuubi engines against a Kubernetes cluster, we need to set up the docker images in the Docker registry first.

Example usage is:

```shell
./bin/docker-image-tool.sh -r <repo> -t my-tag build
./bin/docker-image-tool.sh -r <repo> -t my-tag push
# To build docker image with specify openJdk 
./bin/docker-image-tool.sh -r <repo> -t my-tag -b java_image_tag=<openjdk:${java_image_tag}> build
# To build additional PySpark docker image
./bin/docker-image-tool.sh -r <repo> -t my-tag -p ./kubernetes/dockerfiles/spark/bindings/python/Dockerfile build
# To build additional SparkR docker image
./bin/docker-image-tool.sh -r <repo> -t my-tag -R ./kubernetes/dockerfiles/spark/bindings/R/Dockerfile build
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

When use Client mode to submit application, spark driver use the kubeconfig to access api-service to create and watch executor pods.

When use Cluster mode to submit application, spark driver pod use serviceAccount to access api-service to create and watch executor pods.

In both cases, you need to figure out whether you have the permissions under the corresponding namespace. You can use following cmd to create serviceAccount (You need to have the kubeconfig which have the create serviceAccount permission).

```shell
# create serviceAccount
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
see [the Security section of this document](http://spark.apache.org/docs/latest/running-on-kubernetes.html#security) for security issues related to volume mounts.

```
spark.kubernetes.driver.volumes.<type>.<name>.options.path=<dist_path>
spark.kubernetes.driver.volumes.<type>.<name>.mount.path=<container_path>

spark.kubernetes.executor.volumes.<type>.<name>.options.path=<dist_path>
spark.kubernetes.executor.volumes.<type>.<name>.mount.path=<container_path>
```

Read [Using Kubernetes Volumes](http://spark.apache.org/docs/latest/running-on-kubernetes.html#using-kubernetes-volumes) for more about volumes.

### PodTemplateFile

Kubernetes allows defining pods from template files. Spark users can similarly use template files to define the driver or executor pod configurations that Spark configurations do not support.

To do so, specify the spark properties `spark.kubernetes.driver.podTemplateFile` and `spark.kubernetes.executor.podTemplateFile` to point to local files accessible to the spark-submit process.

### Other

You can read Spark's official documentation for [Running on Kubernetes](http://spark.apache.org/docs/latest/running-on-kubernetes.html) for more information.