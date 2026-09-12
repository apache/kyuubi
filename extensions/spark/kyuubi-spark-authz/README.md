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

# Kyuubi Spark AuthZ Extension

## Functions

- [x] Column-level fine-grained authorization
- [x] Row-level fine-grained authorization, a.k.a. Row-level filtering
- [x] Data masking
- [x] Fail-closed handling of unclassified plan nodes ("paranoid mode"),
      via `spark.kyuubi.authz.unclassifiedNode.behavior=allow|warn|deny`

## Design Notes

- [Paranoid mode](docs/paranoid-mode.md) — why non-recognition of a plan node must not
  silently authorize it, the runtime `allow|warn|deny` mechanism, the
  `known_harmless_spec.json` allowlist policy, and the per-Spark-profile build-time
  coverage checks (`ClassificationCoverageSuite`).

## Build

```shell
build/mvn clean package -DskipTests -pl :kyuubi-spark-authz_2.12 -am -Dspark.version=3.5.6 -Dranger.version=2.6.0
```

### Supported Apache Spark Versions

`-Dspark.version=`

- [x] 4.2.x
- [x] 4.1.x
- [x] 4.0.x
- [x] 3.5.x (default)
- [ ] 3.4.x
- [ ] 3.3.x

### Supported Apache Ranger Versions

`-Dranger.version=`

- [ ] 2.7.x
- [x] 2.6.x (default)
- [x] 2.5.x
- [x] 2.4.x
- [x] 2.3.x
- [x] 2.2.x
- [x] 2.1.x
- [ ] 2.0.x
