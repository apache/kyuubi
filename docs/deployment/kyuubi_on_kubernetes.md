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


# Deploy Kyuubi On Kubernetes

## Requirements

If you want to deploy Kyuubi on Kubernetes, you'd better get a sense of the following things.

* Use Kyuubi official docker image or build Kyuubi docker image
* An active Kubernetes cluster
* Reading About [Deploy Kyuubi engines on Kubernetes](engine_on_kubernetes.md)
* [Kubectl](https://kubernetes.io/docs/reference/kubectl/overview/)
* KubeConfig of the target cluster

## Kyuubi Official Docker Image 

You can find the official docker image at [Apache Kyuubi (Incubating) Docker Hub](https://registry.hub.docker.com/r/apache/kyuubi).

## Build Kyuubi Docker Image

You can build custom Docker images from the `${KYUUBI_HOME}/bin/docker-image-tool.sh` contained in the binary package.

Examples:
```shell
  - Build and push image with tag "v1.4.0" to docker.io/myrepo
    $0 -r docker.io/myrepo -t v1.4.0 build
    $0 -r docker.io/myrepo -t v1.4.0 push

  - Build and push with tag "v1.4.0" and Spark-3.2.1 as base image to docker.io/myrepo
    $0 -r docker.io/myrepo -t v1.4.0 -b BASE_IMAGE=repo/spark:3.2.1 build
    $0 -r docker.io/myrepo -t v1.4.0 push

  - Build and push for multiple archs to docker.io/myrepo
    $0 -r docker.io/myrepo -t v1.4.0 -X build

  - Build with Spark placed "/path/spark"
    $0 -s /path/spark build
    
  - Build with Spark Image myrepo/spark:3.1.0
    $0 -S /opt/spark -b BASE_IMAGE=myrepo/spark:3.1.0 build
```

`${KYUUBI_HOME}/bin/docker-image-tool.sh` use `Kyuubi Version` as default docker tag and always build `${repo}/kyuubi:${tag}` image.

The script can also help build external Spark into a Kyuubi image that acts as a client for submitting tasks by `-s ${SPARK_HOME}`.

Of course, if you have an image that contains the Spark binary package, you don't have to copy Spark locally. Make your Spark Image as BASE_IMAGE by using the `-S ${SPARK_HOME_IN_DOCKER}` and `-b BASE_IMAGE=${SPARK_IMAGE}` arguments.

You can use `${KYUUBI_HOME}/bin/docker-image-tool.sh -h` for more parameters.

## Deploy

Multiple YAML files are provided under `${KYUUBI_HOME}/docker/` to help you deploy Kyuubi.

You can deploy single-node Kyuubi through `${KYUUBI_HOME}/docker/kyuubi-pod.yaml` or `${KYUUBI_HOME}/docker/kyuubi-deployment.yaml`.

Also, you can use `${KYUUBI_HOME}/docker/kyuubi-service.yaml` to deploy Kyuubi Service.

### [Optional] ServiceAccount

According to [Kubernetes RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac/), we need to give kyuubi server the corresponding kubernetes privileges for `created/list/delete` engine pods in kubernetes.

You should create your serviceAccount ( or reuse account with the appropriate privileges ) and set your serviceAccountName for kyuubi pod, which you can find template in `${KYUUBI_HOME}/docker/kyuubi-deployment.yaml` or `${KYUUBI_HOME}/docker/kyuubi-pod.yaml`.

For example, you can create serviceAccount by following command:

```shell
kubectl create serviceAccount kyuubi -n <your namespace>

kubectl create rolebinding kyuubi-role --role=edit --serviceAccount=<your namespace>:kyuubi --namespace=<your namespace>
```

See more related details in [Using RBAC Authorization](https://kubernetes.io/docs/reference/access-authn-authz/rbac/) and [Configure Service Accounts for Pods](https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/).

## Config

You can configure Kyuubi the old-fashioned way by placing kyuubi-default.conf inside the image. Kyuubi do not recommend using this way on Kubernetes.

Kyuubi provide `${KYUUBI_HOME}/docker/kyuubi-configmap.yaml` to build Configmap for Kyuubi.

You can find out how to use it in the comments inside the above file.

If you want to know kyuubi engine on kubernetes configurations, you can refer to [Deploy Kyuubi engines on Kubernetes](engine_on_kubernetes.md)

## Connect

If you do not use Service or HostNetwork to get the IP address of the node where Kyuubi deployed.
You should connect like:
```shell
kubectl exec -it kyuubi-example -- /bin/bash
${KYUUBI_HOME}/bin/beeline -u 'jdbc:hive2://localhost:10009'
```

Or you can submit tasks directly through local beeline:
```shell
${KYUUBI_HOME}/bin/beeline -u 'jdbc:hive2://${hostname}:${port}'
```
As using service nodePort, port means nodePort and hostname means any hostname of kubernetes node.

As using HostNetwork, port means kyuubi containerPort and hostname means hostname of node where Kyuubi deployed.

## TODO 
Kyuubi will provide other connection methods in the future, like `Ingress`, `Load Balance`.
