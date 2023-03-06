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

# PyHive

[PyHive](https://github.com/dropbox/PyHive) is a collection of Python DB-API and SQLAlchemy interfaces for Hive. PyHive can connect with the Kyuubi server serving in thrift protocol as HiveServer2.

## Requirements

PyHive works with Python 2.7 / Python 3. Install PyHive via pip for the Hive interface.

```
pip install 'pyhive[hive]'
```

## Usage

Use the Kyuubi server's host and thrift protocol port to connect.

For further information about usages and features, e.g. DB-API async fetching, using in SQLAlchemy, please refer to [project homepage](https://github.com/dropbox/PyHive).

### DB-API

```python
from pyhive import hive
cursor = hive.connect(host=kyuubi_host,port=10009).cursor()
cursor.execute('SELECT * FROM my_awesome_data LIMIT 10')
print(cursor.fetchone())
print(cursor.fetchall())
```

### Use PyHive with Pandas

PyHive provides a handy way to establish a SQLAlchemy compatible connection and works with Pandas dataframe for executing SQL and reading data via [`pandas.read_sql`](https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html).

```python
from pyhive import hive
import pandas as pd

# open connection
conn = hive.Connection(host=kyuubi_host,port=10009)

# query the table to a new dataframe
dataframe = pd.read_sql("SELECT id, name FROM test.example_table", conn)
```

### Authentication

If password is provided for connection, make sure the `auth` param set to either `CUSTOM` or `LDAP`.

```python
# open connection
conn = hive.Connection(host=kyuubi_host,port=10009, 
user='user', password='password', auth='CUSTOM')
```

