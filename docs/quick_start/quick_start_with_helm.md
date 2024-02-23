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

# Getting Started with Helm

## Running Kyuubi with Helm

[Helm](https://helm.sh/) is the package manager for Kubernetes, it can be used to find, share, and use software built for Kubernetes.

### Install Helm

Please go to [Installing Helm](https://helm.sh/docs/intro/install/) page to get and install an appropriate release version for yourself.

### Get Kyuubi Started

#### Install the chart

```shell
helm install kyuubi ${KYUUBI_HOME}/charts/kyuubi -n kyuubi --create-namespace
```

It will print release info with notes, including the ways to get Kyuubi accessed within Kubernetes cluster and exposed externally depending on the configuration provided.

```shell
NAME: kyuubi
LAST DEPLOYED: Fri Feb 23 13:15:10 UTC 2024
NAMESPACE: kyuubi
STATUS: deployed
REVISION: 1
TEST SUITE: None
NOTES:
The chart has been installed!

In order to check the release status, use:
  helm status kyuubi -n kyuubi
    or for more detailed info
  helm get all kyuubi -n kyuubi

************************
******* Services *******
************************
REST:
- To access kyuubi-rest service within the cluster, use the following URL:
    kyuubi-rest.kyuubi.svc.cluster.local
THRIFT_BINARY:
- To access kyuubi-thrift-binary service within the cluster, use the following URL:
    kyuubi-thrift-binary.kyuubi.svc.cluster.local
```

#### Uninstall the chart

```shell
helm uninstall kyuubi -n kyuubi
```

#### Configure chart release

Specify configuration properties using `--set` flag.
For example, to install the chart with `replicaCount` set to `1`, use the following command:

```shell
helm install kyuubi ${KYUUBI_HOME}/charts/kyuubi -n kyuubi --create-namespace --set replicaCount=1
```

Also, custom values file can be used to override default property values. For example, create `myvalues.yaml` to specify `replicaCount` and `resources`:

```yaml
replicaCount: 1

resources:
  requests:
    cpu: 2
    memory: 4Gi
  limits:
    cpu: 4
    memory: 10Gi
```

and use it to override default chart values with `-f` flag:

```shell
helm install kyuubi ${KYUUBI_HOME}/charts/kyuubi -n kyuubi --create-namespace -f myvalues.yaml
```

#### Access logs

List all pods in the release namespace:

```shell
kubectl get pod -n kyuubi
```

Find Kyuubi pods:

```shell
NAME       READY   STATUS    RESTARTS   AGE
kyuubi-0   1/1     Running   0          38m
kyuubi-1   1/1     Running   0          38m
```

Then, use pod name to retrieve logs:

```shell
kubectl logs kyuubi-0 -n kyuubi
```

