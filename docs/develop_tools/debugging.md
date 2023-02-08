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

# Debugging Kyuubi

You can use the [Java Debug Wire Protocol](https://docs.oracle.com/javase/8/docs/technotes/guides/jpda/conninv.html#Plugin) to debug Kyuubi
with your favorite IDE tool, e.g. IntelliJ IDEA.

## Debugging Server

We can configure the JDWP agent in `KYUUBI_JAVA_OPTS` for debugging.

For example,

```bash
KYUUBI_JAVA_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 \
bin/kyuubi start
```

In the IDE, you set the corresponding parameters(host&port) in debug configurations, for example,

<div align=center>

![](../imgs/idea_debug.png)

</div>

## Debugging Engine

We can configure the Kyuubi properties to enable debugging engine.

### Flink Engine

```bash
kyuubi.engine.flink.java.options -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005
```

### Trino Engine

```bash
kyuubi.engine.trino.java.options -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005
```

### Hive Engine

```bash
kyuubi.engine.hive.java.options -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005
```

## Debugging Apps

### Spark Engine

- Spark Driver

```bash
spark.driver.extraJavaOptions   -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005
```

- Spark Executor

```bash
spark.executor.extraJavaOptions   -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005
```

### Flink Engine

- Flink Processes

```bash
env.java.opts   -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005
```

- Flink JobManager

```bash
env.java.opts.jobmanager   -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005
```

- Flink TaskManager

```bash
env.java.opts.taskmanager   -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005
```

- Flink HistoryServer

```bash
env.java.opts.historyserver   -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005
```

- Flink Client

```bash
env.java.opts.client   -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005
```

