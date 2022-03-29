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

![](../../imgs/kyuubi_logo.png)

</div>

# Basics

This article assumes you have read the [Quick Start](../../quick_start/quick_start.html).

## [Flink Configurations](../settings.html#flink-configurations)

## Deployment Mode

You can set the Flink
option [`execution.target`](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/config/#execution-target)
to change different deployment mode.

[More About Deployment Mode](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/overview/#deployment-modes)

### Local Mode

`execution.target` default is local, you can also set nothing.

#### Flink Configurations

```bash
execution.target: local
```

### Standalone Mode

#### Flink Configurations

```bash
execution.target: remote
```

#### Environment

Standalone mode need an active standalone cluster. The following show how to launch a Flink standalone cluster, and
submit an example job:

```bash
# we assume to be in the root directory of the unzipped Flink distribution

# (1) Start Cluster
$ ./bin/start-cluster.sh

# (2) You can now access the Flink Web Interface on http://localhost:8081

# (3) Submit example job
$ ./bin/flink run ./examples/streaming/TopSpeedWindowing.jar

# (4) Stop the cluster again
$ ./bin/stop-cluster.sh
```

In step `(1)`, we've started 2 processes: A JVM for the JobManager, and a JVM for the TaskManager. The JobManager is
serving the web interface accessible at [localhost:8081](http://localhost:8081). In step `(3)`, we are starting a Flink
Client (a short-lived JVM process) that submits an application to the JobManager.

[More About Standalone Mode](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/resource-providers/standalone/overview/#standalone)

### Yarn Mode

[Deploy Kyuubi Flink Engine on Yarn](../engine_on_yarn.html#deploy-kyuubi-flink-engine-on-yarn)

### Kubernetes Mode (Coming Soon)

## Troubleshooting

If you have a problem with Flink Engine when using Kyuubi, keep in mind that Kyuubi only wraps
the [Flink SQL Client](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sqlclient/) and your problem
might be independent of Kyuubi and sometimes can be solved by upgrading Flink version or reconfiguring Flink.
