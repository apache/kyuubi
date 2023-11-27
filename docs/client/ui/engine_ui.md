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

# Engine UI

This engine UI is able to help you understand status of the engine behind Kyuubi servers.

## Engine Management Details

The Engine UI offers an Engine Management feature on the left side of UI page. This allows users to access detailed information about the engines.
However, not all available engines are displayed by default. Thus, users have to add correct filter conditions to get engines they prefer. After setting the right conditions, please click on 'search' button.
The engines that meet your specified requirements should be listed on the page as the below picture shown.

![workspace](../../imgs/ui/engine_ui.png)

| Name           | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|:---------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Engine address | The engine IP address                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| Engine ID      | The unique identifier of engine                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| Engine Type    | The engine types(only SPARK-SQL engine can be shown in this page now)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| Share Level    | The share level of engine, such as user, connection, group and server                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| User           | The user created the engine                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| Version        | The version of the Kyuubi server associated with this engine                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| Operation      | Extra operations that users can do further.<br/> 1. View native engine UI<br/>find and select the engine you wish to view its native UI. <br/>clink on the view button, you should be redirected to the native engine UI powered by Kyuubi proxy.  <br/>2. Delete the specified engine gracefully <br/>select the specific engine you would like to delete from the Engine Management page. <br/>click on delete button and confirm your choice, then the engine will be remove from service discovery like Zookeeper, ETCD and etc. <br/>The engine will eventually be shut down once all connected session closed. |

