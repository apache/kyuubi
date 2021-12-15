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

# Access Kerberized Kyuubi with Beeline & BI Tools

## Instructions
When Kyuubi is secured by Kerberos, you can not connect to Kyuubi simply by providing a JDBC url 
with username and password.
Instead, following steps should be taken.

## Installing and Configuring the Kerberos Clients
Usually, Kerberos client is installed as default. You can validate it using klist tool.

Linux command and output:
```bash
$ klist -V
Kerberos 5 version 1.15.1
```

Mac OS command and output:
```bash
$ klist --version
klist (Heimdal 1.5.1apple1)
Copyright 1995-2011 Kungliga Tekniska Högskolan
Send bug-reports to heimdal-bugs@h5l.org
```

Windows command and output:
```cmd
> klist -V
Kerberos for Windows
```

If the client is not installed, you should install it ahead based on the OS platform.  
We recommend you to install the MIT Kerberos Distribution as all commands in this guide is based on it.  

Kerberos client needs a configuration file for tuning up the creation of Kerberos ticket cache.
Following is the configuration file's default location on different OS:

OS | Path
---| ---
Linux | /etc/krb5.conf
Mac OS | /etc/krb5.conf
Windows | %ProgramData%\MIT\Kerberos5\krb5.ini

If administrative privileges are not granted to you, you can put the configuration file in other place and 
set `KRB5_CONFIG` environment variable to the location of it.

The configuration file should be configured to point to the same KDC as Kyuubi points to.

## Get Kerberos Ticket Cache
Execute `kinit` command to get Kerberos ticket cache from KDC.

Suppose user principal is `kyuubi_user@KYUUBI.APACHE.ORG` and user keytab file name is `kyuubi_user.keytab`, 
the command should be:

```
$ kinit -kt kyuubi_user.keytab kyuubi_user@KYUUBI.APACHE.ORG

(Command is identical on different OS platform)
```

You may also execute `kinit` command with principal and password to get Kerberos ticket cache:

```
$ kinit kyuubi_user@KYUUBI.APACHE.ORG
Password for kyuubi_user@KYUUBI.APACHE.ORG: password 

(Command is identical on different OS platform)
```

If the command executes successfully, `klist` command output should be like this:

```
$ klist

Ticket cache: FILE:/tmp/krb5cc_1000
Default principal: kyuubi_user@KYUUBI.APACHE.ORG

Valid starting       Expires              Service principal
2021-12-13T18:44:58  2021-12-14T04:44:58  krbtgt/KYUUBI.APACHE.ORG@KYUUBI.APACHE.ORG
    renew until 2021-12-14T18:44:57
    
(Command and output are identical on different OS platform)
```

If you are running Kyuubi and executing `kinit` on the same host with the same OS user, the ticket 
cache file used by Kyuubi will be overwritten by the new ticket cache.

To avoid that, you should store the new ticket cache in another place.  
Ticket cache file location can be specified with `-c` argument.

For example,
```
$ kinit -c /tmp/krb5cc_beeline -kt kyuubi_user.keytab kyuubi_user@KYUUBI.APACHE.ORG

(Command is identical on different OS platform)
```

To check the ticket cache, specify the file location with `-c` argument in `klist` command.

For example,
```
$ klist -c /tmp/krb5cc_beeline

(Command is identical on different OS platform)
```

## Add Kerberos Client Configuration File to JVM Search Path
The JVM, which Beeline or BI Tools are running on, also needs to read the Kerberos client configuration file.
However, JVM uses different default locations from Kerberos client, and does not honour `KRB5_CONFIG`
environment variable.

OS | JVM Search Paths
---| ---
Linux | System scope: `/etc/krb5.conf`
Mac OS | User scope: `$HOME/Library/Preferences/edu.mit.Kerberos`<br/>System scope: `/etc/krb5.conf`
Windows | User scoep: `%USERPROFILE%\krb5.ini`<br/>System scope: `%windir%\krb5.ini`

Put a copy of the configuration file to the default location according to your OS platform.

## Add Kerberos Ticket Cache to JVM Search Path
JVM also needs to read the ticket cache to handle the Kerberos authentication.

JVM determines the ticket cache location in the following order:
1. Path specified by `KRB5CCNAME` environment variable
2. `/tmp/krb5cc_%{uid}` on Unix-like OS, e.g. Linux, Mac OS
3. `${user.home}/krb5cc_${user.name}` if `${user.name}` is not null
4. `${user.home}/krb5cc` if `${user.name}` is null

**Note**:  
- `${user.home}` and `${user.name}` are JVM system properties.
- `${user.home}` should be replaced with `${user.dir}` if `${user.home}` is null.

Put the ticket cache file in one of the above locations. 

## Ensure core-site.xml Exists in Classpath
Like hadoop clients, `hadoop.security.authentication` should be set to `KERBEROS` in `core-site.xml` 
to let Hive JDBC driver use Kerberos authentication. `core-site.xml` should be placed under beeline's 
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

### BI Tools
As to BI tools, ways to add `core-site.xml` varies.  
Take DBeaver as an example. We can add files to DBeaver's classpath through its `Global libraries` preference.  
As `Global libraries` only accepts jar files, you should package `core-site.xml` into a jar file.

```bash
$ jar -c -f core-site.jar core-site.xml

(Command is identical on different OS platform)
```

## Connect with JDBC URL
The last step is to connect to Kyuubi with the right JDBC URL.  
The JDBC URL should be in format: 

```
jdbc:hive2://<kyuubi_server_a ddress>:<kyuubi_server_port>/<db>;principal=<kyuubi_server_principal>
```

**Note**:  
- `kyuubi_server_principal` is the value of `kyuubi.kinit.principal` set in `kyuubi-defaults.conf`.
- As a command line argument, JDBC URL should be quoted to avoid being split into 2 commands by ";".
- As to DBeaver, `<db>;principal=<kyuubi_server_principal>` should be set as the `Database/Schema` argument.

