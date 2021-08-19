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

# Kinit Auxiliary Service

In order to work with a kerberos-enabled cluster, Kyuubi provides this kinit auxiliary service.
It will periodically re-kinit with to keep the Ticket Cache fresh.


## Installing and Configuring the Kerberos Clients

Usually, Kerberos client is installed as default. You can validate it using `klist` tool.

```bash
$ klist -V
Kerberos 5 version 1.15.1
```

If the client is not installed, you should install it ahead based on the OS platform that you prepare to run Kyuubi.

`krb5.conf` is a configuration file for tuning up the creation of Kerberos ticket cache.
The default location is `/etc` on Linux,
and we can use `KRB5_CONFIG` environmental variable to overwrite the location of the configuration file.

Replace or configure `krb5.conf` to point to the KDC.

## Kerberos Ticket

Kerberos client is aimed to generate a Ticket Cache file.
Then, Kyuubi can use this Ticket Cache to authenticate with those kerberized services,
e.g. HDFS, YARN, and Hive Metastore server, etc.

A Kerberos ticket cache contains a service and a client principal names,
lifetime indicators, flags, and the credential itself, e.g.

```bash
$ klist

Ticket cache: FILE:/tmp/krb5cc_5441
Default principal: spark/kyuubi.host.name@KYUUBI.APACHE.ORG

Valid starting       Expires              Service principal
2020-11-25T13:17:18  2020-11-26T13:17:18  krbtgt/KYUUBI.APACHE.ORG@KYUUBI.APACHE.ORG
	renew until 2020-12-02T13:17:18
```

Kerberos credentials can be stored in Kerberos ticket cache.
For example, `/tmp/krb5cc_5441` in the above case.

They are valid for relatively short period. So, we always need to refresh it for long-running services like Kyuubi.

## Configurations

Key | Default | Meaning | Since
--- | --- | --- | ---
kyuubi\.kinit<br>\.principal|<div style='width: 80pt;word-wrap: break-word;white-space: normal'>&lt;undefined&gt;</div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>Name of the Kerberos principal.</div>|<div style='width: 20pt'>1.0.0</div>
kyuubi\.kinit\.keytab|<div style='width: 80pt;word-wrap: break-word;white-space: normal'>&lt;undefined&gt;</div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>Location of Kyuubi server's keytab.</div>|<div style='width: 20pt'>1.0.0</div>
kyuubi\.kinit\.interval|<div style='width: 80pt;word-wrap: break-word;white-space: normal'>PT1H</div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>How often will Kyuubi server run `kinit -kt [keytab] [principal]` to renew the local Kerberos credentials cache</div>|<div style='width: 20pt'>1.0.0</div>
kyuubi\.kinit\.max<br>\.attempts|<div style='width: 80pt;word-wrap: break-word;white-space: normal'>10</div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>How many times will `kinit` process retry</div>|<div style='width: 20pt'>1.0.0</div>

When `hadoop.security.authentication` is set to `KERBEROS`, in `$HADOOP_CONF_DIR/core-site` or `$KYUUBI_HOME/conf/kyuubi-defaults.conf`,
it indicates that we are targeting a secured cluster, then we need to specify `kyuubi.kinit.principal` and `kyuubi.kinit.keytab` for authentication.

Kyuubi will use this `principal` to impersonate client users,
so the cluster should enable it to do impersonation for some particular user from some particular hosts.

For example,

```bash
hadoop.proxyuser.<user name in principal>.groups *
hadoop.proxyuser.<user name in principal>.hosts *
```
## Further Readings

- [Hadoop in Secure Mode](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SecureMode.html)
- [Use Kerberos for authentication in Spark](http://spark.apache.org/docs/latest/security.html#kerberos)
