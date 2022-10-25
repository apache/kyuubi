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

## Authentication with In Memory Database

Used with auto created in-memory database, JDBC authentication could be applied for token validation without start up a dedicated database service or custom plugin. 

Consider an authentication for a username and a token which contacted with an `expire_time` in timestamp and a signature for sequence of `expire_time`, `username` and a secret key. With the following example, an H2 in-memory database will be auto crated with Kyuubi Server and used for authentication with its system function `HASH` and checking token expire time with `NOW()`.

```properties
kyuubi.authentication=JDBC
kyuubi.authentication.jdbc.driver.class = org.h2.Driver
kyuubi.authentication.jdbc.url = jdbc:h2:mem:
kyuubi.authentication.jdbc.user = no_user
kyuubi.authentication.jdbc.query = SELECT 1 FROM (SELECT RAWTOHEX(HASH('MD5', STRINGTOUTF8(CONCAT(SUBSTR(input.token, 34), input.sign_key)))) AS valid_sign, input.username AS input_user, SUBSTR(input.token, 1, 32) AS token_sign, SUBSTR(input.token, 34, LENGTH(input.username)) AS token_user, CAST(SUBSTR(input.token, 35 + LENGTH(input.username), 19) AS TIMESTAMP) AS token_expire FROM (SELECT CAST('dgAuthKey' AS VARCHAR) AS sign_key, CAST(${user} AS VARCHAR) AS username, CAST(${password} AS VARCHAR) AS token ) input ) result WHERE token_user = input_user AND token_sign = valid_sign AND token_expire > NOW();

```