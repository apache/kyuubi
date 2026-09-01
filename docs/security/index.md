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

# Kyuubi Security Overview

Securing Kyuubi involves enabling authentication(authn), authorization(authz)
and encryption, etc.

## Protect Session Configurations

For a multi-tenant deployment, configure at least one of
`kyuubi.session.conf.ignore.list` or `kyuubi.session.conf.restrict.list`. Both
lists are empty by default, so clients can otherwise override sensitive
session-level configurations during engine bootstrap and connection setup.

One conservative starting point is to keep server-defined resource sizing while
rejecting client changes to deployment and authorization settings:

```properties
kyuubi.session.conf.ignore.list=spark.driver.memory,spark.executor.memory
kyuubi.session.conf.restrict.list=spark.master,spark.submit.deployMode,spark.sql.extensions,spark.sql.optimizer.excludedRules
```

Tailor these lists to the deployment. The ignore list silently drops matching
client values, while the restrict list rejects the connection. They do not
prevent later changes through `SET` statements; configure the engine's
operation-level restrictions separately when that protection is required.

See the [session configuration settings](../configuration/settings.md#session)
for details.

```{toctree}
:maxdepth: 2

Authentication <authentication>
Authorization <authorization/index>
kinit
hadoop_credentials_manager
internal_secure_access
```
