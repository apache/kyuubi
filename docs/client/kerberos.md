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

<div align=center>

![](../imgs/kyuubi_logo.png)

</div>

# Access kerberized Kyuubi with beeline & BI tools

## Instructions
When Kyuubi is secured by kerberos, we can not connect to Kyuubi simply by providing a JDBC url 
with username and password.
Instead, following steps should be taken.

## Installing and Configuring the Kerberos Clients
Usually, Kerberos client is installed as default. You can validate it using klist tool.

```bash
$ klist -V
Kerberos 5 version 1.15.1
```

If the client is not installed, you should install it ahead based on the OS platform.

`krb5.conf` is a configuration file for tuning up the creation of Kerberos ticket cache.
The default location is `/etc` on Linux,
and we can use `KRB5_CONFIG` environmental variable to overwrite the location of the configuration file.

Replace or configure `krb5.conf` to point to the same KDC as Kyuubi points to.

## Get Kerberos ticket cache
Execute `kinit` command to get Kerberos ticket cache from KDC.

Suppose user principal is `kyuubi_user@KYUUBI.APACHE.ORG`, user keytab located at `/etc/security/keytabs/kyuubi_user.keytab`, 
the command should be:

```bash
$ kinit -kt /etc/security/keytabs/kyuubi_user.keytab kyuubi_user@KYUUBI.APACHE.ORG
```

If the command executes successfully, `klist` command output should be like this:

```bash
$ klist

Ticket cache: FILE:/tmp/krb5cc_1000
Default principal: kyuubi_user@KYUUBI.APACHE.ORG

Valid starting       Expires              Service principal
2021-12-13T18:44:58  2021-12-14T04:44:58  krbtgt/KYUUBI.APACHE.ORG@KYUUBI.APACHE.ORG
    renew until 2021-12-14T18:44:57
```

**Note**: 
If Kyuubi is running on the same host, take care not to overwrite the ticket cache file used by Kyuubi.
As the default ticket cache file is `/tmp/krb5cc_$(id -u)` on Linux, we should either set environment 
variable `KRB5CCNAME` or switch to another OS user before executing `kinit` command.

## Ensure core-site.xml exists in classpath
Like hadoop clients, `hadoop.security.authentication` should be set to `KERBEROS` in `core-site.xml` 
to let Hive JDBC driver use kerberos authentication. `core-site.xml` should be placed under beeline's 
classpath or BI tools' classpath.

### Beeline
Here are the usual locations where `core-site.xml` should exist for different beeline distributions:

Client | Location | Note
--- | --- | ---
Hive beeline | `$HADOOP_HOME/etc/hadoop` | Hive resolves `$HADOOP_HOME` and use `$HADOOP_HOME/bin/hadoop` command to launch beeline. `$HADOOP_HOME/etc/hadoop` is in `hadoop` command's classpath.
Spark beeline | `$HADOOP_CONF_DIR` | In `$SPARK_HOME/conf/spark-env.sh`, `$HADOOP_CONF_DIR` often be set to the directory containing hadoop client configuration files.
Kyuubi beeline | `$HADOOP_CONF_DIR` | In `$KYUUBI_HOME/conf/kyuubi-env.sh`, `$HADOOP_CONF_DIR` often be set to the directory containing hadoop client configuration files.

If `core-site.xml` is not found in above locations, create one with the following content:
```xml
<configuration>
  <property>
    <name>hadoop.security.authentication</name>
    <value>kerberos</value>
  </property>
</configuration>
```

### BI tools
As to BI tools, ways to add `core-site.xml` varies.  
Take DBeaver as an example. We can add files to DBeaver's classpath through its `Global libraries` preference.  
As `Global libraries` only accepts jar files, we have to package `core-site.xml` into a jar file.

```bash
$ jar -c -f core-site.jar core-site.xml
```

## Connect with JDBC URL
The last step is to connect to Kyuubi with the right JDBC URL.  
The JDBC URL should be in format: 

```
jdbc:hive2://<kyuubi_server_a ddress>:<kyuubi_server_port>/<db>;principal=<kyuubi_server_principal>
```

**Note**: 
1. `kyuubi_server_principal` is the value of `kyuubi.kinit.principal` set in `kyuubi-defaults.conf`.
2. As a command line argument, JDBC URL should be quoted to avoid being split into 2 commands by ";".
3. As to DBeaver, `<db>;principal=<kyuubi_server_principal>` should be set as the `Database/Schema` argument.

