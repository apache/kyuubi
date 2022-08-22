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


# Configure Kyuubi to Use JDBC Authentication

Kyuubi supports authentication via JDBC query. A query is prepared with user/password value and sent to the database configured in JDBC URL. Authentication passes if the result set is not empty.

The SQL statement must start with the `SELECT` clause. Placeholders are supported and listed below for substitution:
- `${user}`
- `${password}`

For example, `SELECT 1 FROM auth_db.auth_table WHERE user=${user} AND 
passwd=MD5(CONCAT(salt,${password}))` will be prepared as `SELECT 1 FROM auth_db.auth_table WHERE user=? AND passwd=MD5(CONCAT(salt,?))` with value replacement of `user` and `password` in string type.

## Enable JDBC Authentication 

To enable the jdbc authentication method, we need to

- Put the jdbc driver jar file to `$KYUUBI_HOME/jars` directory to make it visible for
  the classpath of the kyuubi server.
- Configure the following properties to `$KYUUBI_HOME/conf/kyuubi-defaults.conf`
  on each node where kyuubi server is installed.

## Configure the authentication properties
Configure the following properties to `$KYUUBI_HOME/conf/kyuubi-defaults.conf` on each node where kyuubi server is installed.

```properties
kyuubi.authentication=JDBC
kyuubi.authentication.jdbc.driver.class = com.mysql.jdbc.Driver
kyuubi.authentication.jdbc.url = jdbc:mysql://127.0.0.1:3306/auth_db
kyuubi.authentication.jdbc.user = bowenliang123
kyuubi.authentication.jdbc.password = bowenliang123@kyuubi
kyuubi.authentication.jdbc.query = SELECT 1 FROM auth_table WHERE user=${user} AND passwd=MD5(CONCAT(salt,${password}))
```