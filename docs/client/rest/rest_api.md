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

Returns all the batch sessions.

#### Request Parameters

| Name       | Description                                                                                             | Type   |
| :--------- |:--------------------------------------------------------------------------------------------------------| :----- |
| batchType  | The batch type, such as spark/flink, if no batchType is specified,<br/> return all types                | string |
| batchState | The valid batch state can be one of the following:<br/> PENDING, RUNNING, FINISHED, ERROR, CANCELED     | string |
| batchUser  | The user name that created the batch                                                                    | string |
| createTime | Return the batch that created after this timestamp                                                      | long   |
| endTime    | Return the batch that ended before this timestamp                                                       | long   |
| from       | The start index to fetch sessions                                                                       | int    |
| size       | Number of sessions to fetch                                                                             | int    |

#### Response Body

| Name    | Description                         | Type |
| :------ | :---------------------------------- | :--- |
| from    | The start index of fetched sessions | int  |
| total   | Number of sessions fetched          | int  |
| batches | [Batch](#batch) list                | list |

### POST /batches

#### Request Body

| Name      | Description                                        | Type             |
| :-------- | :------------------------------------------------- | :--------------- |
| batchType | The batch job type, such as Spark, Flink           | String           |
| resource  | The resource containing the application to execute | path (required)  |
| className | Application main class                             | string(required) |
| name      | The name of this batch job.                        | string           |
| conf      | Configuration properties                           | Map of key=val   |
| args      | Command line arguments for the application         | list of strings  |


#### Response Body

The created [Batch](#batch) object.

### GET /batches/{batchId}

Returns the batch session information.

#### Response Body

The [Batch](#batch).

### DELETE /batches/${batchId}

Kill the batch job if it is still running.

#### Request Parameters

| Name                    | Description                   | Type             |
| :---------------------- | :---------------------------- | :--------------- |
| hive.server2.proxy.user | the proxy user to impersonate | String(optional) |

#### Response Body

| Name    | Description                             | Type    |
| :------ | :-------------------------------------- | :------ |
| success | Whether kill the batch job successfully | boolean |
| msg     | The kill batch job response             | string  |

### GET /batches/${batchId}/localLog

Gets the local log lines from this batch.

#### Request Parameters

| Name | Description                       | Type |
| :--- | :-------------------------------- | :--- |
| from | Offset                            | int  |
| size | Max number of log lines to return | int  |

#### Response Body

| Name      | Description       | Type          |
| :-------- | :---------------- | :------------ |
| logRowSet | The log lines     | list of sting |
| rowCount  | The log row count | Int           |

### Batch

| Name           | Description                                                       | Type   |
| :------------- |:------------------------------------------------------------------| :----- |
| id             | The batch id                                                      | string |
| user           | The user created the batch                                        | string |
| batchType      | The batch type                                                    | string |
| name           | The batch job name                                                | string |
| appId          | The batch job applicationId                                       | string |
| appUrl         | The batch job application tracking url                            | string |
| appState       | The batch job application state                                   | string |
| appDiagnostic  | The batch job application diagnostic                              | string |
| kyuubiInstance | The kyuubi instance that created the batch                        | string |
| state          | The kyuubi batch operation state                                  | string |
| createTime     | The batch create time                                             | long   |
| endTime        | The batch end time, if the batch has not been terminated, it is 0 | long   |
