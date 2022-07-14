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

## Usage
```shell
bin/kyuubi-admin --help
```
Output
```shell
kyuubi 1.6.0-SNAPSHOT
Usage: kyuubi-admin [refresh] [options]

  -b, --verbose            Print additional debug output.
  --hostUrl <value>        Host url for rest api.
  --authSchema <value>     Auth schema for rest api, valid values are basic, spnego.
  --username <value>       Username for basic authentication.
  --password <value>       Password for basic authentication.
  --spnegoHost <value>     Spnego host for spnego authentication.
  --conf <value>           Kyuubi config property pair, formatted key=value.

Command: refresh [config] <args>...
        Refresh the resource.
Command: refresh config [<configType>]
        Refresh the config with specified type.
  <configType>             The valid config type can be one of the following: hadoopConf.

  -h, --help               Show help message and exit.
```

## Administer kyuubi server

The tool is based on admin rest api.
You can specify the rest host url(`--hostUrl`), auth schema(`--authSchema`), spnego host(`--spnegoHost`) and so on for rest rpc call.

### Refresh config
Refresh the config with specified type. The valid config type can be one of the following: hadoopConf.
```shell
bin/kyuubi-admin refresh config hadoopConf
```

