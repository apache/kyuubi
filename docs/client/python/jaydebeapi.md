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

# Python-JayDeBeApi

The [JayDeBeApi](https://pypi.org/project/JayDeBeApi/) module allows you to connect from Python code to databases using Java JDBC.
It provides a Python DB-API v2.0 to that database.

## Requirements

To install Python-JayDeBeApi, you can use pip, the Python package manager. Open your command-line interface or terminal and run the following command:

```shell
pip install jaydebeapi
```

If you want to install JayDeBeApi in Jython, you'll need to ensure that you have either pip or EasyInstall available for Jython. These tools are used to install Python packages, including JayDeBeApi.
Or you can get a copy of the source by cloning from the [JayDeBeApi GitHub project](https://github.com/baztian/jaydebeapi) and install it.

```shell
python setup.py install
```

or if you are using Jython use

```shell
jython setup.py install
```

## Preparation

Using the Python-JayDeBeApi package to connect to Kyuubi, you need to install the library and configure the relevant JDBC driver. You can download JDBC driver from maven repository and specify its path in Python. Choose the matching driver `kyuubi-hive-jdbc-*.jar` package based on the Kyuubi server version.
The driver class name is `org.apache.kyuubi.jdbc.KyuubiHiveDriver`.

|      Package       |                                                Repo                                                 |
|--------------------|-----------------------------------------------------------------------------------------------------|
| kyuubi jdbc driver | [kyuubi-hive-jdbc-*.jar](https://repo1.maven.org/maven2/org/apache/kyuubi/kyuubi-hive-jdbc-shaded/) |

## Usage

Below is a simple example demonstrating how to use Python-JayDeBeApi to connect to Kyuubi database and execute a query:

```python
import jaydebeapi

# Set JDBC driver path and connection URL
driver = "org.apache.kyuubi.jdbc.KyuubiHiveDriver"
url = "jdbc:kyuubi://host:port/default"
jdbc_driver_path = ["/path/to/kyuubi-hive-jdbc-*.jar"]

# Connect to the database using JayDeBeApi
conn = jaydebeapi.connect(driver, url, ["user", "password"], jdbc_driver_path)

# Create a cursor object
cursor = conn.cursor()

# Execute the SQL query
cursor.execute("SELECT * FROM example_table LIMIT 10")

# Retrieve query results
result_set = cursor.fetchall()

# Process the results
for row in result_set:
    print(row)

# Close the cursor and the connection
cursor.close()
conn.close()
```

Make sure to replace the placeholders (host, port, user, password) with your actual Kyuubi configuration.
With the above code, you can connect to Kyuubi and execute SQL queries in Python. Please handle exceptions and errors appropriately in real-world applications.
