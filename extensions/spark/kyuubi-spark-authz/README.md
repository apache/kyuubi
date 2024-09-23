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

## Build

```shell
build/mvn clean package -DskipTests -pl :kyuubi-spark-authz_2.12 -am -Dspark.version=3.2.1 -Dranger.version=2.5.0
```

### Supported Apache Spark Versions

`-Dspark.version=`

- [x] master
- [x] 3.5.x (default)
- [x] 3.4.x
- [x] 3.3.x
- [x] 3.2.x
- [x] 3.1.x
- [ ] 3.0.x
- [ ] 2.4.x and earlier

### Supported Apache Ranger Versions

`-Dranger.version=`

- [x] 2.5.x (default)
- [x] 2.4.x
- [x] 2.3.x
- [x] 2.2.x
- [x] 2.1.x
- [x] 2.0.x
- [x] 1.2.x
- [x] 1.1.x
- [x] 1.0.x
- [x] 0.7.x
- [ ] 0.6.x

