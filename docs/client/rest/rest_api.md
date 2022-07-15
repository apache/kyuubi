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

# REST API v1

Note that: now the api version is v1 and the base uri is `/api/v1`.

## Batch Resource

### GET /batches

Returns all the batches.

#### Request Parameters

| Name       | Description                                                                                             | Type   |
| :--------- |:--------------------------------------------------------------------------------------------------------| :----- |
| batchType  | The batch type, such as spark/flink, if no batchType is specified,<br/> return all types                | String |
| batchState | The valid batch state can be one of the following:<br/> PENDING, RUNNING, FINISHED, ERROR, CANCELED     | String |
| batchUser  | The user name that created the batch                                                                    | String |
| createTime | Return the batch that created after this timestamp                                                      | Long   |
| endTime    | Return the batch that ended before this timestamp                                                       | Long   |
| from       | The start index to fetch sessions                                                                       | Int    |
| size       | Number of sessions to fetch                                                                             | Int    |

#### Response Body

| Name    | Description                         | Type |
| :------ | :---------------------------------- | :--- |
| from    | The start index of fetched sessions | Int  |
| total   | Number of sessions fetched          | Int  |
| batches | [Batch](#batch) List                | List |

### POST /batches

#### Request Body

| Name      | Description                                        | Type             |
| :-------- |:---------------------------------------------------|:-----------------|
| batchType | The batch type, such as Spark, Flink               | String           |
| resource  | The resource containing the application to execute | Path (required)  |
| className | Application main class                             | String(required) |
| name      | The name of this batch.                            | String           |
| conf      | Configuration properties                           | Map of key=val   |
| args      | Command line arguments for the application         | List of Strings  |


#### Response Body

The created [Batch](#batch) object.

### GET /batches/{batchId}

Returns the batch information.

#### Response Body

The [Batch](#batch).

### DELETE /batches/${batchId}

Kill the batch if it is still running.

#### Request Parameters

| Name                    | Description                   | Type             |
| :---------------------- | :---------------------------- | :--------------- |
| hive.server2.proxy.user | the proxy user to impersonate | String(optional) |

#### Response Body

| Name    | Description                           | Type    |
| :------ |:--------------------------------------| :------ |
| success | Whether killed the batch successfully | Boolean |
| msg     | The kill batch message                | String  |

### GET /batches/${batchId}/localLog

Gets the local log lines from this batch.

#### Request Parameters

| Name | Description                       | Type |
| :--- | :-------------------------------- | :--- |
| from | Offset                            | Int  |
| size | Max number of log lines to return | Int  |

#### Response Body

| Name      | Description       | Type          |
| :-------- | :---------------- |:--------------|
| logRowSet | The log lines     | List of sting |
| rowCount  | The log row count | Int           |

### Batch

| Name           | Description                                                       | Type   |
| :------------- |:------------------------------------------------------------------| :----- |
| id             | The batch id                                                      | String |
| user           | The user created the batch                                        | String |
| batchType      | The batch type                                                    | String |
| name           | The batch name                                                    | String |
| appId          | The batch application Id                                          | String |
| appUrl         | The batch application tracking url                                | String |
| appState       | The batch application state                                       | String |
| appDiagnostic  | The batch application diagnostic                                  | String |
| kyuubiInstance | The kyuubi instance that created the batch                        | String |
| state          | The kyuubi batch operation state                                  | String |
| createTime     | The batch create time                                             | Long   |
| endTime        | The batch end time, if it has not been terminated, the value is 0 | Long   |
