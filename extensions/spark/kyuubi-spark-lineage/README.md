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

# Kyuubi Spark Listener Extension

## Functions

- [x] All `listener` extensions can be implemented in this module, like `QueryExecutionListener` and `ExtraListener`
- [x] Add `SparkOperationLineageQueryExecutionListener` to extends spark `QueryExecutionListener`
- [x] SQL lineage parsing will be triggered after SQL execution and will be written to the json logger file

## Build

```shell
build/mvn clean package -DskipTests -pl :kyuubi-spark-lineage_2.12 -am -Dspark.version=3.5.6
```

### Supported Apache Spark Versions

`-Dspark.version=`

- [x] 4.0.x
- [x] 3.5.x (default)
- [x] 3.4.x
- [x] 3.3.x

