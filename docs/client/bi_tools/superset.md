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

# Apache Superset

[Apache Superset](https://superset.apache.org/) is an open source platform for
data exploration and visualization. It can connect to Kyuubi through the
HiveServer2 protocol using the PyHive SQLAlchemy dialect.

## Prerequisites

- A running Kyuubi server with a Thrift binary frontend accessible from Superset.
  See [Getting Started](../../quick_start/quick_start.md).
- A running Superset instance with the PyHive driver installed.

The following setup was tested with:

|    Component    | Version |
|-----------------|---------|
| Apache Superset | 6.1.0   |
| Apache Kyuubi   | 1.11.0  |
| Apache Spark    | 3.5.7   |
| PyHive          | 0.7.0   |

## Install the database driver

Install the driver in the Python environment used by Superset.

For a deployment using Superset's Docker Compose configuration, add the following
dependencies to `docker/requirements-local.txt` in the Superset checkout before
starting the services:

```text
PyHive[hive_pure_sasl]==0.7.0
thrift>=0.14.1,<1.0.0
thrift_sasl>=0.4.3,<1.0.0
tableschema
```

The `hive_pure_sasl` extra uses a pure Python SASL implementation. With
Superset 6.1.0 on Python 3.10, installing `apache-superset[hive]` also pulls in
the native `sasl` package. Its installation can fail when the container does
not have a C++ compiler.

Make sure the driver is installed in each Superset container that executes queries.

## Configure the connection

1. Open **Settings → Database Connections** and click **+ Database**.
2. Select **Apache Hive**.
3. Choose the **SQLAlchemy URI** connection method.
4. Enter a display name and a connection URI:

   ```text
   hive://<username>@<kyuubi-host>:10009/<database>?auth=NONE
   ```
5. Click **Test connection**, then save the connection.
6. Enable **Expose database in SQL Lab** in the database's advanced settings.

Replace the host, port, username, and database with values for your environment.
The default Kyuubi Thrift binary port is `10009`.

The example uses `NONE` authentication for local testing. For other
authentication mechanisms, see [PyHive authentication](../python/pyhive.md#authentication).

When Superset runs in a container on macOS and Kyuubi's port is published on
the host, the following URI can be used:

```text
hive://anonymous@host.docker.internal:10009/default?auth=NONE
```

The hostname must be reachable from the Superset container. In this example,
`host.docker.internal` refers to the host machine.

## Run queries in SQL Lab

1. Open **SQL → SQL Lab**.
2. Click **Add a new tab**.
3. Select the saved Kyuubi connection and a schema.
4. Execute the following query:

   ```sql
   SELECT 1 AS test_value;
   ```

The result should contain one row with `test_value` equal to `1`.

When using the Kyuubi Docker playground, select the `tiny` schema and query
the sample TPC-DS customer table:

```sql
SELECT c_customer_sk, c_first_name, c_last_name
FROM tpcds.tiny.customer
LIMIT 5;
```

This query returns up to five rows with the three selected columns.
The `tpcds.tiny.customer` table is provided by the playground's TPC-DS catalog.
