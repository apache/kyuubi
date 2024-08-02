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

# Kyuubi Beeline

## What is Kyuubi Beeline

Kyuubi Beeline is a Command Line Shell that uses JDBC driver to connect to Kyuubi server to execute queries.
Kyuubi Beeline is derived from
[Hive Beeline](https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients#HiveServer2Clients-Beeline%E2%80%93CommandLineShell),
almost functionalities of Hive Beeline should be applicable to Kyuubi Beeline as well.

Note: Kyuubi Beeline removes the support of "embedded mode" because it is coupled with Apache Hive implementation details,
thus only "remote mode" can be used in Kyuubi Beeline.

## How to use Kyuubi Beeline

Run `kyuubi-beeline --help` to show help message with all options and examples.

```
Usage: kyuubi-beeline <options>.

Options:
  -u <database url>               The JDBC URL to connect to.
  -c <named url>                  The named JDBC URL to connect to,
                                  which should be present in beeline-site.xml
                                  as the value of beeline.hs2.jdbc.url.<namedUrl>.

  -r                              Reconnect to last saved connect url (in conjunction with !save).
  -n <username>                   The username to connect as.
  -p <password>                   The password to connect as.
  -d <driver class>               The driver class to use.
  -i <init file>                  Script file for initialization.
  -e <query>                      Query that should be executed.
  -f <exec file>                  Script file that should be executed.
  -w, --password-file <file>      The password file to read password from.
  --hiveconf property=value       Use value for given property.
  --conf property=value           Alias of --hiveconf.
  --hivevar name=value            Hive variable name and value.
                                  This is Hive specific settings in which variables
                                  can be set at session level and referenced in Hive
                                  commands or queries.

  --property-file=<property file> The file to read connection properties (url, driver, user, password) from.
  --color=[true|false]            Control whether color is used for display.
  --showHeader=[true|false]       Show column names in query results.
  --escapeCRLF=[true|false]       Show carriage return and line feeds in query results as escaped \r and \n.
  --headerInterval=ROWS;          The interval between which heades are displayed.
  --fastConnect=[true|false]      Skip building table/column list for tab-completion.
  --autoCommit=[true|false]       Enable/disable automatic transaction commit.
  --verbose=[true|false]          Show verbose error messages and debug info.
  --showWarnings=[true|false]     Display connection warnings.
  --showDbInPrompt=[true|false]   Display the current database name in the prompt.
  --showNestedErrs=[true|false]   Display nested errors.
  --numberFormat=[pattern]        Format numbers using DecimalFormat pattern.
  --force=[true|false]            Continue running script even after errors.
  --maxWidth=MAXWIDTH             The maximum width of the terminal.
  --maxColumnWidth=MAXCOLWIDTH    The maximum width to use when displaying columns.
  --silent=[true|false]           Be more silent.
  --autosave=[true|false]         Automatically save preferences.
  --outputformat=<format mode>    Format mode for result display.
                                  The available options ars [table|vertical|csv2|tsv2|dsv|csv|tsv|json|jsonfile].
                                  Note that csv, and tsv are deprecated, use csv2, tsv2 instead.

  --incremental=[true|false]      Defaults to true. When set to false, the entire result set
                                  is fetched and buffered before being displayed, yielding optimal
                                  display column sizing. When set to true, result rows are displayed
                                  immediately as they are fetched, yielding lower latency and
                                  memory usage at the price of extra display column padding.
                                  Setting --incremental=true is recommended if you encounter an OutOfMemory
                                  on the client side (due to the fetched result set size being large).
                                  Only applicable if --outputformat=table.

  --incrementalBufferRows=NUMROWS The number of rows to buffer when printing rows on stdout,
                                  defaults to 1000; only applicable if --incremental=true
                                  and --outputformat=table.

  --truncateTable=[true|false]    Truncate table column when it exceeds length.
  --delimiterForDSV=DELIMITER     Specify the delimiter for delimiter-separated values output format (default: |).
  --isolation=LEVEL               Set the transaction isolation level.
  --nullemptystring=[true|false]  Set to true to get historic behavior of printing null as empty string.
  --maxHistoryRows=MAXHISTORYROWS The maximum number of rows to store beeline history.
  --delimiter=DELIMITER           Set the query delimiter; multi-char delimiters are allowed, but quotation
                                  marks, slashes, and -- are not allowed (default: ;).

  --convertBinaryArrayToString=[true|false]
                                  Display binary column data as string or as byte array.

  --python-mode                   Execute python code/script.
  -h, --help                      Display this message.

Examples:
  1. Connect using simple authentication to Kyuubi Server on localhost:10009.
  $ kyuubi-beeline -u jdbc:kyuubi://localhost:10009 -n username

  2. Connect using simple authentication to Kyuubi Server on kyuubi.local:10009 using -n for username and -p for password.
  $ kyuubi-beeline -n username -p password -u jdbc:kyuubi://kyuubi.local:10009

  3. Connect using Kerberos authentication with kyuubi/localhost@mydomain.com as Kyuubi Server principal(kinit is required before connection).
  $ kyuubi-beeline -u "jdbc:kyuubi://kyuubi.local:10009/default;kyuubiServerPrincipal=kyuubi/localhost@mydomain.com"

  4. Connect using Kerberos authentication using principal and keytab directly.
  $ kyuubi-beeline -u "jdbc:kyuubi://kyuubi.local:10009/default;kyuubiClientPrincipal=user@mydomain.com;kyuubiClientKeytab=/local/path/client.keytab;kyuubiServerPrincipal=kyuubi/localhost@mydomain.com"

  5. Connect using SSL connection to Kyuubi Server on localhost:10009.
  $ kyuubi-beeline -u "jdbc:kyuubi://localhost:10009/default;ssl=true;sslTrustStore=/usr/local/truststore;trustStorePassword=mytruststorepassword"

  6. Connect using LDAP authentication.
  $ kyuubi-beeline -u jdbc:kyuubi://kyuubi.local:10009/default -n ldap-username -p ldap-password

  7. Connect using the ZooKeeper address to Kyuubi HA cluster.
  $ kyuubi-beeline -u "jdbc:kyuubi://zk1:2181,zk2:2181,zk3:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=kyuubi" -n username
```

## Using `kyuubi-defaults.conf` to automatically connect to Kyuubi

As of Kyuubi 1.10, Kyuubi Beeline adds support to use the `kyuubi-defaults.conf` present in the `KYUUBI_CONF_DIR` to
automatically generate a connection URL based on the configuration properties in `kyuubi-defaults.conf` and an
additional user configuration file. Not all the URL properties can be derived from `kyuubi-defaults.conf` and hence
in order to use this feature user must create a configuration file called `beeline-hs2-connection.xml` which is a
Hadoop XML format file. This file is used to provide user-specific connection properties for the connection URL.
Kyuubi Beeline looks for this configuration file in `${user.home}/.beeline/` (UNIX-like OS) or `${user.home}\beeline\`
directory (in case of Windows). If the file is not found in the above locations Beeline looks for it in `HIVE_CONF_DIR`
location and `/etc/hive/conf` in that order. Once the file is found, Kyuubi Beeline uses `beeline-hs2-connection.xml`
in conjunction with the `kyuubi-defaults.conf` to determine the connection URL.

The URL connection properties in `beeline-hs2-connection.xml` must have the prefix `beeline.hs2.connection.` followed by
the URL property name. For example in order to provide the property ssl the property key in the `beeline-hs2-connection.xml`
should be `beeline.hs2.connection.ssl`. The sample `beeline.hs2.connection.xml` below provides the value of user and
password for the Beeline connection URL. In this case the rest of the properties like Kyuubi hostname and port information,
Kerberos configuration properties, SSL properties, transport mode, etc., are picked up using the `kyuubi-defaults.conf`.
If the password is empty `beeline.hs2.connection.password` property should be removed. In most cases the below
configuration values in `beeline-hs2-connection.xml` and the correct `kyuubi-defaults.conf` should be sufficient to
make the connection to the Kyuubi.

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
  <name>beeline.hs2.connection.user</name>
  <value>kyuubi</value>
</property>
<property>
  <name>beeline.hs2.connection.password</name>
  <value>kyuubi</value>
</property>
</configuration>
```

In case of properties which are present in both `beeline-hs2-connection.xml` and `kyuubi-defaults.conf`, the property
value derived from `beeline-hs2-connection.xml` takes precedence. For example in the below `beeline-hs2-connection.xml`
file provides the value of principal for Beeline connection in a Kerberos enabled environment. In this case the property
value for `beeline.hs2.connection.principal` overrides the value of `kyuubi.kinit.principal` from `kyuubi-defaults.conf`
as far as connection URL is concerned.

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
  <name>beeline.hs2.connection.hosts</name>
  <value>localhost:10009</value>
</property>
<property>
  <name>beeline.hs2.connection.principal</name>
  <value>kyuubi/dummy-hostname@domain.com</value>
</property>
</configuration>
```

In case of properties `beeline.hs2.connection.hosts`, `beeline.hs2.connection.hiveconf` and
`beeline.hs2.connection.hivevar` the property value is a comma-separated list of values. For example the following
`beeline-hs2-connection.xml` provides the `hiveconf` and `hivevar` values in a comma separated format.

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
  <name>beeline.hs2.connection.user</name>
  <value>kyuubi</value>
</property>
<property>
  <name>beeline.hs2.connection.hiveconf</name>
  <value>kyuubi.session.engine.initialize.timeout=PT3M,kyuubi.engine.type=SPARK_SQL</value>
</property>
<property>
  <name>beeline.hs2.connection.hivevar</name>
  <value>testVarName1=value1, testVarName2=value2</value>
</property>
</configuration>
```

When the `beeline-hs2-connection.xml` is present and when no other arguments are provided, Kyuubi Beeline automatically
connects to the URL generated using configuration files. When connection arguments (`-u`, `-n` or `-p`) are provided,
Kyuubi Beeline uses them and does not use `beeline-hs2-connection.xml` to automatically connect. Removing or renaming
the `beeline-hs2-connection.xml` disables this feature.

## Using beeline-site.xml to automatically connect to HiveServer2

In addition to the above method of using `kyuubi-defaults.conf` and `beeline-hs2-connection.xml` for deriving the JDBC
connection URL to use when connecting to Kyuubi from Kyuubi Beeline, a user can optionally add `beeline-site.xml` to
their classpath, and within `beeline-site.xml`, she can specify complete JDBC URLs. A user can also specify multiple
named URLs and use `kyuubi-beeline -c <named_url>` to connect to a specific URL. This is particularly useful when the
same cluster has multiple Kyuubi instances running with different configurations. One of the named URLs is treated as
default (which is the URL that gets used when the user simply types beeline). An example `beeline-site.xml` is shown below:

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
  <name>beeline.hs2.jdbc.url.tcpUrl</name>
  <value>jdbc:kyuubi://localhost:10009/default;user=kyuubi;password=kyuubi</value>
</property>

<property>
  <name>beeline.hs2.jdbc.url.httpUrl</name>
  <value>jdbc:kyuubi://localhost:10009/default;user=kyuubi;password=kyuubi;transportMode=http;httpPath=cliservice</value>
</property>

<property>
  <name>beeline.hs2.jdbc.url.default</name>
  <value>tcpUrl</value>
</property>
</configuration>
```

In the above example, simply typing `kyuubi-beeline` opens a new JDBC connection to
`jdbc:kyuubi://localhost:10009/default;user=kyuubi;password=kyuubi`. If both `beeline-site.xml` and
`beeline-hs2-connection.xml` are present in the classpath, the final URL is created by applying the properties specified
in `beeline-hs2-connection.xml` on top of the URL properties derived from beeline-site.xml. As an example consider the
following `beeline-hs2-connection.xml`:

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
  <name>beeline.hs2.connection.user</name>
  <value>kyuubi</value>
</property>
<property>
  <name>beeline.hs2.connection.password</name>
  <value>kyuubi</value>
</property>
</configuration>
```

Consider the following `beeline-site.xml`:

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
  <name>beeline.hs2.jdbc.url.tcpUrl</name>
  <value>jdbc:kyuubi://localhost:10009/default</value>
</property>

<property>
  <name>beeline.hs2.jdbc.url.httpUrl</name>
  <value>jdbc:kyuubi://localhost:10009/default;transportMode=http;httpPath=cliservice</value>
</property>

<property>
  <name>beeline.hs2.jdbc.url.default</name>
  <value>tcpUrl</value>
</property>
</configuration>
```

In the above example, simply typing `kyuubi-beeline` opens a new JDBC connection to
`jdbc:kyuubi://localhost:10009/default;user=kyuubi;password=kyuubi`. When the user types `kyuubi-beeline -c httpUrl`,
a connection is opened to `jdbc:kyuubi://localhost:10009/default;transportMode=http;httpPath=cliservice;user=kyuubi;password=kyuubi`.
