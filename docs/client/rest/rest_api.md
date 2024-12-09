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

## Authentication

REST API supports the [Basic Authentication](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Authorization#basic_authentication) which relies on `Authorization` header in HTTP request.

```
Authorization: Basic <credentials>
```

The `<credentials>` value is the Base64 encoded string of `username:password`. For example, in the case of a user `aladdin` with the password `opensesame`, set the `Authorization` header to `Basic YWxhZGRpbjpvcGVuc2VzYW1l` as Base64 encoded `aladdin:opensesame`.

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

| Name      | Description                      | Type   |
|:----------|:---------------------------------|:-------|
| infoType  | The type of session information  | String |
| infoValue | The value of session information | String |

### GET /sessions/count

Get the current open session count

#### Response Body

| Name             | Description                  | Type |
|:-----------------|:-----------------------------|:-----|
| openSessionCount | The count of opening session | Int  |

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

| Name    | Description                      | Type |
|:--------|:---------------------------------|:-----|
| configs | The configuration of the session | Map  |

#### Response Body

| Name           | Description                                                                                        | Type   |
|:---------------|:---------------------------------------------------------------------------------------------------|:-------|
| identifier     | The session handle identifier                                                                      | String |
| kyuubiInstance | The Kyuubi instance that holds the session and to call for the following operations in the session | String |

### DELETE /sessions/${sessionHandle}

Close a session.

### POST /sessions/${sessionHandle}/operations/statement

Create an operation with EXECUTE_STATEMENT type

#### Request Body

| Name         | Description                                                    | Type           |
|:-------------|:---------------------------------------------------------------|:---------------|
| statement    | The SQL statement that you execute                             | String         |
| runAsync     | The flag indicates whether the query runs synchronously or not | Boolean        |
| queryTimeout | The interval of query time out                                 | Long           |
| confOverlay  | The conf to overlay only for current operation                 | Map of key=val |

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

## Operation Resource

### GET /operations/${operationHandle}/event

Get the current event of the operation by the specified operation handle.

#### Response Body

The [KyuubiOperationEvent](#kyuubioperationevent).

### PUT /operations/${operationHandle}

Perform an action to the pending or running operation.

#### Request Body

| Name   | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Type   |
|:-------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-------|
| action | The action that is performed to the operation. Currently, supported actions are 'cancel' and 'close'. <ul><li>Cancel: to cancel the operation, which means the operation and its corresponding background task will be stopped, and its state will be switched to CANCELED. A CANCELED operation's status can still be fetched by client requests.</li><li>Close: to close the operation, which means the operation and its corresponding background task will be stopped, and its state will be switched to CLOSED. A CLOSED operation's status will be removed on the server side and can not be fetched anymore.</li><ul> | String |

### GET /operations/${operationHandle}/resultsetmetadata

Get the result set schema of the operation by the specified operation handle.

#### Response Body

| Name    | Description                 | Type               |
|:--------|:----------------------------|:-------------------|
| columns | The descriptions of columns | List of ColumnDesc |

### GET /operations/${operationHandle}/log

Get a list of operation log lines of the running operation by the specified operation handle.

#### Request Parameters

| Name    | Description                           | Type |
|:--------|:--------------------------------------|:-----|
| maxrows | The max row that are pulled each time | Int  |

#### Response Body

| Name      | Description              | Type            |
|:----------|:-------------------------|:----------------|
| logRowSet | The set of log set       | List of Strings |
| rowCount  | The count of log row set | Int             |

### GET /operations/${operationHandle}/rowset

Get the operation result as a list of rows by the specified operation handle.

#### Request Parameters

| Name             | Description                                                                                                            | Type   |
|:-----------------|:-----------------------------------------------------------------------------------------------------------------------|:-------|
| maxrows          | The max rows that are pulled each time                                                                                 | Int    |
| fetchorientation | The orientation of fetch, for example FETCH_NEXT, FETCH_PRIOR, FETCH_FIRST, FETCH_LAST, FETCH_RELATIVE, FETCH_ABSOLUTE | String |

#### Response Body

| Name     | Description       | Type         |
|:---------|:------------------|:-------------|
| rows     | The list of rows  | List of Rows |
| rowCount | The count of rows | Int          |

## Batch Resource

### GET /batches

Returns all the batches.

#### Request Parameters

| Name       | Description                                                                                         | Type    |
|:-----------|:----------------------------------------------------------------------------------------------------|:--------|
| batchType  | The batch type, such as spark/flink, if no batchType is specified,<br/> return all types            | String  |
| batchState | The valid batch state can be one of the following:<br/> PENDING, RUNNING, FINISHED, ERROR, CANCELED | String  |
| batchUser  | The user name that created the batch                                                                | String  |
| createTime | Return the batch that created after this timestamp                                                  | Long    |
| endTime    | Return the batch that ended before this timestamp                                                   | Long    |
| from       | The start index to fetch batches                                                                    | Int     |
| size       | Number of batches to fetch, 100 by default                                                          | Int     |
| desc       | List the batches in descending order, false by default.                                             | Boolean |

#### Response Body

| Name    | Description                        | Type |
|:--------|:-----------------------------------|:-----|
| from    | The start index of fetched batches | Int  |
| total   | Number of batches fetched          | Int  |
| batches | [Batch](#batch) List               | List |

### POST /batches

Create a new batch.

#### Request Body

- Media type: `application-json`
- JSON structure:

| Name      | Description                                        | Type             |
|:----------|:---------------------------------------------------|:-----------------|
| batchType | The batch type, such as Spark, Flink               | String           |
| resource  | The resource containing the application to execute | Path (required)  |
| className | Application main class                             | String(required) |
| name      | The name of this batch.                            | String           |
| conf      | Configuration properties                           | Map of key=val   |
| args      | Command line arguments for the application         | List of Strings  |

#### Response Body

The created [Batch](#batch) object.

### POST /batches (with uploading resource)

Create a new batch with uploading resource file.

Example of using `curl` command to send POST request to `/v1/batches` in `multipart-formdata` media type with uploading resource file from local path.

```shell
curl --location --request POST 'http://localhost:10099/api/v1/batches' \
--form 'batchRequest="{\"batchType\":\"SPARK\",\"className\":\"org.apache.spark.examples.SparkPi\",\"name\":\"Spark Pi\"}";type=application/json' \
--form 'resourceFile=@"/local_path/example.jar"'
```

#### Request Body

- Media type: `multipart-formdata`
- Request body structure in multiparts:

| Name         | Description                                                                                       | Media Type       |
|:-------------|:--------------------------------------------------------------------------------------------------|:-----------------|
| batchRequest | The batch request in JSON format as request body required in [POST /batches](#post-batches)       | application/json |
| resourceFile | The resource to upload and execute, which will be cached on server and cleaned up after execution | File             |

#### Response Body

The created [Batch](#batch) object.

### GET /batches/${batchId}

Returns the batch information.

#### Response Body

The [Batch](#batch).

### DELETE /batches/${batchId}

Kill the batch if it is still running.

#### Response Body

| Name    | Description                           | Type    |
|:--------|:--------------------------------------|:--------|
| success | Whether killed the batch successfully | Boolean |
| msg     | The kill batch message                | String  |

### GET /batches/${batchId}/localLog

Gets the local log lines from this batch.

#### Request Parameters

| Name | Description                                       | Type |
|:-----|:--------------------------------------------------|:-----|
| from | Offset                                            | Int  |
| size | Max number of log lines to return, 100 by default | Int  |

#### Response Body

| Name      | Description       | Type            |
|:----------|:------------------|:----------------|
| logRowSet | The log lines     | List of Strings |
| rowCount  | The log row count | Int             |

## Admin Resource

### POST /admin/refresh/hadoop_conf

Refresh the Hadoop configurations of the Kyuubi server.

### POST /admin/refresh/user_defaults_conf

Refresh the [user defaults configs](../../configuration/settings.html#user-defaults) with key in format in the form of `___{username}___.{config key}` from default property file.

### POST /admin/refresh/kubernetes_conf

Refresh the kubernetes configs with key prefixed with `kyuubi.kubernetes` from default property file.

It is helpful if you need to support multiple kubernetes contexts and namespaces, see [KYUUBI #4843](https://github.com/apache/kyuubi/issues/4843).

### DELETE /admin/engine

Delete the specified engine.

#### Request Parameters

| Name                    | Description                                                  | Type              |
|:------------------------|:-------------------------------------------------------------|:------------------|
| type                    | the engine type                                              | String(optional)  |
| sharelevel              | the engine share level                                       | String(optional)  |
| subdomain               | the engine subdomain                                         | String(optional)  |
| proxyUser               | the proxy user to impersonate                                | String(optional)  |
| hive.server2.proxy.user | the proxy user to impersonate                                | String(optional)  |
| kill                    | whether to kill the engine forcibly. Default value is false. | Boolean(optional) |

`proxyUser` is an alternative to `hive.server2.proxy.user`, and the current behavior is consistent with
`hive.server2.proxy.user`. When both parameters are set, `proxyUser` takes precedence.

### GET /admin/engine

Get a list of satisfied engines.

#### Request Parameters

| Name                    | Description                   | Type             |
|:------------------------|:------------------------------|:-----------------|
| type                    | the engine type               | String(optional) |
| sharelevel              | the engine share level        | String(optional) |
| subdomain               | the engine subdomain          | String(optional) |
| proxyUser               | the proxy user to impersonate | String(optional) |
| hive.server2.proxy.user | the proxy user to impersonate | String(optional) |

`proxyUser` is an alternative to hive.server2.proxy.user, and the current behavior is consistent with
hive.server2.proxy.user. When both parameters are set, proxyUser takes precedence.

#### Response Body

The [Engine](#engine) List.

## REST Objects

### Batch

| Name           | Description                                                       | Type   |
|:---------------|:------------------------------------------------------------------|:-------|
| id             | The batch id                                                      | String |
| user           | The user created the batch                                        | String |
| batchType      | The batch type                                                    | String |
| name           | The batch name                                                    | String |
| appStartTime   | The batch application start time                                  | Long   |
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

#### KyuubiOperationEvent

| Name           | Description                                                     | Type      |
|:---------------|:----------------------------------------------------------------|:----------|
| statementId    | The unique identifier of a single operation                     | String    |
| remoteId       | The unique identifier of a single operation at engine side      | String    |
| statement      | The sql that you execute                                        | String    |
| shouldRunAsync | The flag indicating whether the query runs synchronously or not | Boolean   |
| state          | The current operation state                                     | String    |
| eventTime      | The time when the event created & logged                        | Long      |
| createTime     | The time for changing to the current operation state            | Long      |
| startTime      | The time the query start to time of this operation              | Long      |
| completeTime   | Time time the query ends                                        | Long      |
| exception      | Caught exception if have                                        | Throwable |
| sessionId      | The identifier of the parent session                            | String    |
| sessionUser    | The authenticated client user                                   | String    |

### ColumnDesc

| Name        | Description                            | Type   |
|:------------|:---------------------------------------|:-------|
| columnName  | The name of the column                 | String |
| dataType    | The type descriptor for this column    | String |
| columnIndex | The index of this column in the schema | Int    |
| precision   | The precision of the column            | Int    |
| scale       | The scale of the column                | Int    |
| comment     | The comment of the column              | String |

### Row

| Name   | Description       | Type           |
|:-------|:------------------|:---------------|
| fields | The fields of row | List of Fields |

### Field

| Name     | Description         | Type   |
|:---------|:--------------------|:-------|
| dataType | The type of column  | String |
| value    | The value of column | Object |

### Engine

| Name       | Description                                                        | Type   |
|:-----------|:-------------------------------------------------------------------|:-------|
| version    | The version of the Kyuubi server that creates this engine instance | String |
| user       | The user created the engine                                        | String |
| engineType | The engine type                                                    | String |
| sharelevel | The engine share level                                             | String |
| subdomain  | The engine subdomain                                               | String |
| instance   | host:port for the engine node                                      | String |
| namespace  | The namespace used to expose the engine to KyuubiServers           | String |

