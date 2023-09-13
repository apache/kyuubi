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

# Auxiliary SQL Functions

Kyuubi provides several auxiliary SQL functions as supplement to
Flink's [Built-in Functions](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/functions/systemfunctions/)

|        Name         |                         Description                         | Return Type | Since |
|---------------------|-------------------------------------------------------------|-------------|-------|
| kyuubi_version      | Return the version of Kyuubi Server                         | string      | 1.8.0 |
| kyuubi_engine_name  | Return the application name for the associated query engine | string      | 1.8.0 |
| kyuubi_engine_id    | Return the application id for the associated query engine   | string      | 1.8.0 |
| kyuubi_system_user  | Return the system user name for the associated query engine | string      | 1.8.0 |
| kyuubi_session_user | Return the session username for the associated query engine | string      | 1.8.0 |

