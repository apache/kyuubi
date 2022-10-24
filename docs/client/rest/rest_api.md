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

## Session Resource

### GET /sessions

Get the list of all live sessions

#### Response Body

| Name       | Description                                              | Type   |
|:-----------|:---------------------------------------------------------|:-------|
| identifier | The session identifier                                   | String |
| user       | The user name that created the session                   | String |
| ipAddr     | The client IP address that created the session           | String |
| conf       | The configuration of the session                         | Map    |
| createTime | The session that created at this timestamp               | Long   |
| duration   | The interval that last access time subtract created time | Long   |
| idleTime   | The interval of no operation                             | Long   |

### GET /sessions/${sessionHandle}

Get a session event

#### Response Body

The [KyuubiSessionEvent](#kyuubisessionevent).

### GET /sessions/${sessionHandle}/info/${infoType}

Get an information detail of a session

#### Request Parameters

| Name     | Description                   | Type |
|:---------|:------------------------------|:-----|
| infoType | The id of Hive Thrift GetInfo | Int  |

#### Response Body

| Name      | Description                                | Type   |
|:----------|:-------------------------------------------|:-------|
| infoType  | The type of session information            | String |
| infoValue | The value of session information           | String |

### GET /sessions/count

Get the current open session count

#### Response Body

| Name             | Description                       | Type |
|:-----------------|:----------------------------------|:-----|
| openSessionCount | The count of opening session      | Int  |

### GET /sessions/execPool/statistic

Get statistic info of background executors

#### Response Body

| Name                | Description                                                         | Type |
|:--------------------|:--------------------------------------------------------------------|:-----|
| execPoolSize        | The current number of threads in the pool                           | Int  |
| execPoolActiveCount | The approximate number of threads that are actively executing tasks | Int  |

### POST /sessions

Create a session

#### Request Parameters

| Name            | Description                              | Type   |
|:----------------|:-----------------------------------------|:-------|
| protocolVersion | The protocol version of Hive CLI service | Int    |
| user            | The user name                            | String |
| password        | The user password                        | String |
| ipAddr          | The user client IP address               | String |
| configs         | The configuration of the session         | Map    |

#### Response Body

| Name       | Description                   | Type   |
|:-----------|:------------------------------|:-------|
| identifier | The session handle identifier | String |

### DELETE /sessions/${sessionHandle}

Close a session.

### POST /sessions/${sessionHandle}/operations/statement

Create an operation with EXECUTE_STATEMENT type

#### Request Body

| Name         | Description                                                    | Type    |
|:-------------|:---------------------------------------------------------------|:--------|
| statement    | The SQL statement that you execute                             | String  |
| runAsync     | The flag indicates whether the query runs synchronously or not | Boolean |
| queryTimeout | The interval of query time out                                 | Long    |

#### Response Body

| Name       | Description                 | Type   |
|:-----------|:----------------------------|:-------|
| identifier | The identifier of operation | String |

### POST /sessions/${sessionHandle}/operations/typeInfo

Create an operation with GET_TYPE_INFO type

#### Response Body

| Name       | Description                 | Type   |
|:-----------|:----------------------------|:-------|
| identifier | The identifier of operation | String |

### POST /sessions/${sessionHandle}/operations/catalogs

Create an operation with GET_CATALOGS type

#### Response Body

| Name       | Description                 | Type   |
|:-----------|:----------------------------|:-------|
| identifier | The identifier of operation | String |

### POST /sessions/${sessionHandle}/operations/schemas

Create an operation with GET_SCHEMAS type

#### Request Body

| Name        | Description      | Type   |
|:------------|:-----------------|:-------|
| catalogName | The catalog name | String |
| schemaName  | The schema name  | String |

#### Response Body

| Name       | Description                 | Type   |
|:-----------|:----------------------------|:-------|
| identifier | The identifier of operation | String |

### POST /sessions/${sessionHandle}/operations/tables

#### Request Body

| Name        | Description                                   | Type   |
|:------------|:----------------------------------------------|:-------|
| catalogName | The catalog name                              | String |
| schemaName  | The schema name                               | String |
| tableName   | The table name                                | String |
| tableTypes  | The type of table, for example: TABLE or VIEW | String |

#### Response Body

| Name       | Description                 | Type   |
|:-----------|:----------------------------|:-------|
| identifier | The identifier of operation | String |

### POST /sessions/${sessionHandle}/operations/tableTypes

#### Request Parameters

#### Response Body

| Name       | Description                 | Type   |
|:-----------|:----------------------------|:-------|
| identifier | The identifier of operation | String |

### POST /sessions/${sessionHandle}/operations/columns

#### Request Body

| Name        | Description      | Type   |
|:------------|:-----------------|:-------|
| catalogName | The catalog name | String |
| schemaName  | The schema name  | String |
| tableName   | The table name   | String |
| columnName  | The column name  | String |

#### Response Body

| Name       | Description                 | Type   |
|:-----------|:----------------------------|:-------|
| identifier | The identifier of operation | String |

### POST /sessions/${sessionHandle}/operations/functions

#### Request Body

| Name         | Description       | Type   |
|:-------------|:------------------|:-------|
| catalogName  | The catalog name  | String |
| schemaName   | The schema name   | String |
| functionName | The function name | String |

#### Response Body

| Name       | Description                 | Type   |
|:-----------|:----------------------------|:-------|
| identifier | The identifier of operation | String |

### POST /sessions/${sessionHandle}/operations/primaryKeys

#### Request Parameters

| Name        | Description      | Type   |
|:------------|:-----------------|:-------|
| catalogName | The catalog name | String |
| schemaName  | The schema name  | String |
| tableName   | The table name   | String |

#### Response Body

| Name       | Description                 | Type   |
|:-----------|:----------------------------|:-------|
| identifier | The identifier of operation | String |

### POST /sessions/${sessionHandle}/operations/crossReference

| Name       | Description                 | Type   |
|:-----------|:----------------------------|:-------|
| identifier | The identifier of operation | String |

#### Request Body

| Name           | Description              | Type   |
|:---------------|:-------------------------|:-------|
| primaryCatalog | The primary catalog name | String |
| primarySchema  | The primary schema name  | String |
| primaryTable   | The primary table name   | String |
| foreignCatalog | The foreign catalog name | String |
| foreignSchema  | The foreign schema name  | String |
| foreignTable   | The foreign table name   | String |

#### Response Body

| Name       | Description                 | Type   |
|:-----------|:----------------------------|:-------|
| identifier | The identifier of operation | String |

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

Create a new batch.

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

## Admin Resource

### POST /admin/refresh/hadoop_conf

Refresh the Kyuubi server hadoop conf.

### DELETE /admin/engine

Delete specified engine.

#### Request Parameters

| Name                    | Description                   | Type             |
|:------------------------|:------------------------------| :--------------- |
| type                    | the engine type               | String(optional) |
| sharelevel              | the engine share level        | String(optional) |
| subdomain               | the engine subdomain          | String(optional) |
| hive.server2.proxy.user | the proxy user to impersonate | String(optional) |

### GET /admin/engine

Get a list of engines.

#### Request Parameters

| Name                    | Description                   | Type             |
|:------------------------|:------------------------------| :--------------- |
| type                    | the engine type               | String(optional) |
| sharelevel              | the engine share level        | String(optional) |
| subdomain               | the engine subdomain          | String(optional) |
| hive.server2.proxy.user | the proxy user to impersonate | String(optional) |

#### Response Body
The [Engine](#engine) List.

## REST Objects

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

### KyuubiSessionEvent

| Name            | Description                                                                                                         | Type      |
|:----------------|:--------------------------------------------------------------------------------------------------------------------|:----------|
| sessionId       | The session id                                                                                                      | String    |
| clientVersion   | The client version                                                                                                  | Int       |
| sessionType     | The session type                                                                                                    | String    |
| sessionName     | The session name, if user not specify it, we use empty string instead                                               | String    |
| user            | The session user name                                                                                               | String    |
| clientIP        | The client ip address                                                                                               | String    |
| serverIP        | A unique Kyuubi server id, e.g. kyuubi server ip address and port, it is useful if has multi-instance Kyuubi Server | String    |
| conf            | The session config                                                                                                  | Map       |
| startTime       | The session create time                                                                                             | Long      |
| remoteSessionId | The remote engine session id                                                                                        | String    |
| engineId        | The engine id. For engine on yarn, it is applicationId                                                              | String    |
| openedTime      | The session opened time                                                                                             | Long      |
| endTime         | The session end time                                                                                                | Long      |
| totalOperations | How many queries and meta calls                                                                                     | Int       |
| exception       | The session exception, such as the exception that occur when opening session                                        | Throwable |
| eventType       | The type of session event                                                                                           | String    |

### Engine

| Name           | Description                                              | Type   |
| :------------- |:---------------------------------------------------------| :----- |
| version        | The kyuubi version                                       | String |
| user           | The user created the engine                              | String |
| engineType     | The engine type                                          | String |
| sharelevel     | The engine share level                                   | String |
| subdomain      | The engine subdomain                                     | String |
| instance       | host:port for the engine node                            | String |
| namespace      | The namespace used to expose the engine to KyuubiServers | String |