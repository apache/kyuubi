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

# Getting Started With Kyuubi and DataGrip
## What is DataGrip
[DataGrip](https://www.jetbrains.com/datagrip/) is a multi-engine database environment released by JetBrains, supporting MySQL and PostgreSQL, Microsoft SQL Server and Oracle, Sybase, DB2, SQLite, HyperSQL, Apache Derby, and H2.

## Preparation
### Get DataGrip And Install
Please go to [Download DataGrip](https://www.jetbrains.com/datagrip/download) to get and install an appropriate version for yourself.
### Get Kyuubi Started
[Get kyuubi server started](https://kyuubi.apache.org/docs/latest/quick_start/quick_start.html) before you try DataGrip with kyuubi.

For debugging purpose, you can use `tail -f` or `tailf` to track the server log.
## Configurations
### Start DataGrip
After you install DataGrip, just launch it.
### Select Database
Substantially, this step is to choose a JDBC Driver type to use later. We can choose Apache Hive to set up a driver for Kyuubi.

![select database](../imgs/datagrip/select_database.png)
### Datasource Driver
You should first download the missing driver files. Just click on the link below, DataGrip will download and install those. 

![datasource and driver](../imgs/datagrip/datasource_and_driver.png)
### Generic JDBC Connection Settings
After install drivers, you should configure the right host and port which you can find in kyuubi server log. By default, we use `localhost` and `10009` to configure.

Of course, you can fill other configs.

After generic configs, you can use test connection to test.

![configuration](../imgs/datagrip/configuration.png)
## Interacting With Kyuubi Server
Now, you can interact with Kyuubi server.

The left side of the photo is the table, and the right side of the photo is the console.

You can interact through the visual interface or code.

![workspace](../imgs/datagrip/workspace.png)
## The End
There are many other amazing features in both Kyuubi and DataGrip and here is just the tip of the iceberg. The rest is for you to discover.