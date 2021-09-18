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

# Building a Runnable Distribution

To create a Kyuubi distribution like those distributed by [Kyuubi Release Page](https://github.com/apache/incubator-kyuubi/releases),
and that is laid out so as to be runnable, use `./build/dist` in the project root directory.

For more information on usage, run `./build/dist --help`

```logtalk
./build/dist - Tool for making binary distributions of Kyuubi Server

Usage:
+--------------------------------------------------------------------------------------+
| ./build/dist [--name <custom_name>] [--tgz] [--spark-provided] <maven build options> |
+--------------------------------------------------------------------------------------+
name:           -  custom binary name, using project version if undefined
tgz:            -  whether to make a whole bundled package
spark-provided: -  whether to make a package without Spark binary
```

For instance,

```bash
./build/dist --name custom-name --tgz
```

This results a Kyuubi distribution named `kyuubi-{version}-bin-custom-name.tgz` for you.

If you are planing to deploy Kyuubi where `spark` is provided, in other word, it's not required to bundle spark binary, use 

```bash
./build/dist --tgz --spark-provided
```

Then you will get a Kyuubi distribution without spark binary named `kyuubi-{version}-bin.tgz`.
