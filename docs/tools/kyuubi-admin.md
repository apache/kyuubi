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


# Administer Kyuubi Server Tool

The tool to administer the Kyuubi server based on admin resource rest api.
You can specify the rest host url(`--hostUrl`), auth schema(`--authSchema`), spnego host(`--spnegoHost`) and so on for rest rpc call.

## Usage
```shell
bin/kyuubi-admin --help
```

## Refresh config

Refresh the config with specified type. The valid config type can be one of the following: hadoopConf.

```shell
bin/kyuubi-admin refresh config hadoopConf
```

