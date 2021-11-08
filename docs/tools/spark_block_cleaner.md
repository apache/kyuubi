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

# Kubernetes Tools Spark Block Cleaner

## Requirements

You'd better have cognition upon the following things when you want to use spark-block-cleaner.

* Read this article
* An active Kubernetes cluster
* [Kubectl](https://kubernetes.io/docs/reference/kubectl/overview/)
* [Docker](https://www.docker.com/)

## Scenes

When you're using Spark On Kubernetes with Client mode and don't use `emptyDir` for Spark `local-dir` type, you may face the same scenario that executor pods deleted without clean all the Block files. It may cause disk overflow.

Therefore, we chose to use Spark Block Cleaner to clear the block files accumulated by Spark.

## Principle

When deploying Spark Block Cleaner, we will configure volumes for the destination folder. Spark Block Cleaner will perceive the folder by the parameter `CACHE_DIRS`. 

Spark Block Cleaner will clear the perceived folder in a fixed loop(which can be configured by `SCHEDULE_INTERVAL`). And Spark Block Cleaner will select folder start with `blockmgr` and `spark` for deletion using the logic Spark uses to create those folders. 

Before deleting those files, Spark Block Cleaner will determine whether it is a recently modified file(depending on whether the file has not been acted on within the specified time which configured by `FILE_EXPIRED_TIME`). Only delete files those beyond that time interval.

And Spark Block Cleaner will check the disk utilization after clean, if the remaining space is less than the specified value(control by `FREE_SPACE_THRESHOLD`), will trigger deep clean(which file expired time control by `DEEP_CLEAN_FILE_EXPIRED_TIME`).

## Usage

Before you start using Spark Block Cleaner, you should build its docker images.

### Build Block Cleaner Docker Image

In the `KYUUBI_HOME` directory, you can use the following cmd to build docker image.
```shell
docker build ./tools/spark-block-cleaner/kubernetes/docker
```

### Modify spark-block-cleaner.yml

You need to modify the `${KYUUBI_HOME}/tools/spark-block-cleaner/kubernetes/spark-block-cleaner.yml` to fit your current environment.

In Kyuubi tools, we recommend using `DaemonSet` to start, and we offer default yaml file in daemonSet way.

Base file structure: 
```yaml
apiVersion
kind
metadata
  name
  namespace
spec
  select
  template
    metadata
    spce
      containers
      - image
      - volumeMounts
      - env
    volumes
```

You can use affect the performance of Spark Block Cleaner through configure parameters in containers env part of `spark-block-cleaner.yml`.
```yaml
env:
  - name: CACHE_DIRS
    value: /data/data1,/data/data2
  - name: FILE_EXPIRED_TIME
    value: 604800
  - name: DEEP_CLEAN_FILE_EXPIRED_TIME
    value: 432000
  - name: FREE_SPACE_THRESHOLD
    value: 60
  - name: SCHEDULE_INTERVAL
    value: 3600
```

The most important thing, configure volumeMounts and volumes corresponding to Spark local-dirs.

For example, Spark use /spark/shuffle1 as local-dir, you can configure like:
```yaml
volumes:
  - name: block-files-dir-1
    hostPath:
      path: /spark/shuffle1
```
```yaml
volumeMounts:
  - name: block-files-dir-1
    mountPath: /data/data1
```
```yaml
env:
  - name: CACHE_DIRS
    value: /data/data1
```

### Start daemonSet

After you finishing modifying the above, you can use the following command `kubectl apply -f ${KYUUBI_HOME}/tools/spark-block-cleaner/kubernetes/spark-block-cleaner.yml` to start daemonSet.

## Related parameters

Name | Default | unit | Meaning
--- | --- | --- | ---
CACHE_DIRS | /data/data1,/data/data2|  | The target dirs in container path which will clean block files.
FILE_EXPIRED_TIME | 604800 | seconds | Cleaner will clean the block files which current time - last modified time more than the fileExpiredTime.
DEEP_CLEAN_FILE_EXPIRED_TIME | 432000 | seconds | Deep clean will clean the block files which current time - last modified time more than the deepCleanFileExpiredTime.
FREE_SPACE_THRESHOLD | 60 | % | After first clean, if free Space low than threshold trigger deep clean.
SCHEDULE_INTERVAL | 3600 | seconds | Cleaner sleep between cleaning.
