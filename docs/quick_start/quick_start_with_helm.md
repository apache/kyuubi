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

# Getting Started With Kyuubi on kubernetes

## Running kyuubi with helm

[Helm](https://helm.sh/) is the package manager for Kubernetes，it can be used to find, share, and use software built for Kubernetes.

### Get helm and Install

Please go to [Install Helm](https://helm.sh/docs/intro/install/) page to get and install an appropriate release version for yourself.

### Get Kyuubi Started

#### [Optional] Create namespace on kubernetes
```bash
cretate ns kyuubi
```

#### Get kyuubi started
```bash
helm install kyuubi-helm ${KYUUBI_HOME}/docker/helm -n ${namespace_name}
```
It will print variables and the way to get kyuubi expose ip and port
```bash
NAME: kyuubi-helm
LAST DEPLOYED: Wed Oct 20 15:22:47 2021
NAMESPACE: kyuubi
STATUS: deployed
REVISION: 1
TEST SUITE: None
NOTES:
Get kyuubi expose URL by running these commands:
  export NODE_PORT=$(kubectl get --namespace kyuubi -o jsonpath="{.spec.ports[0].nodePort}" services kyuubi-helm-nodeport)
  export NODE_IP=$(kubectl get nodes --namespace kyuubi -o jsonpath="{.items[0].status.addresses[0].address}")
  echo $NODE_IP:$NODE_PORT
```

#### Using hive beeline  
[Using Hive Beeline](https://kyuubi.apache.org/docs/latest/quick_start/quick_start.html#using-hive-beeline) to opening a connection.

#### Remove kyuubi
```bash
helm uninstall kyuubi-helm -n ${namespace_name}
```

#### Edit server config

Modify `values.yaml` under `${KYUUBI_HOME}/docker/helm`
```yaml
# Kyuubi server numbers
replicaCount: 2

image:
  repository: apache/kyuubi
  pullPolicy: Always
  # Overrides the image tag whose default is the chart appVersion.
  tag: "master-snapshot"

server:
  bind:
    host: 0.0.0.0
    port: 10009
  conf:
    mountPath: /opt/kyuubi/conf

service:
  type: NodePort
  # The default port limit of kubernetes is 30000-32767
  # to change:
  #   vim kube-apiserver.yaml (usually under path: /etc/kubernetes/manifests/)
  #   add or change line 'service-node-port-range=1-32767' under kube-apiserver
  port: 30009
```

#### Get server log  
List all server pods:
```bash
kubectl get po -n ${namespace_name}
```
The server pods will print:
```text
NAME                             READY   STATUS    RESTARTS   AGE
kyuubi-server-585d8944c5-m7j5s   1/1     Running   0          30m
kyuubi-server-32sdsa1245-2d2sj   1/1     Running   0          30m
```
then, use pod name to get logs
```bash
kubectl -n ${namespace_name} logs kyuubi-server-585d8944c5-m7j5s
```
