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

# Hadoop Credentials Manager

In order to pass the authentication of a kerberos secured hadoop cluster, kyuubi currently submits 
engines in two ways:
1. Submits with current kerberos user and extra `SparkSubmit` argument `--proxy-user`.
2. Submits with `spark.kerberos.principal` and `spark.kerberos.keytab` specified.

If engine is submitted with `--proxy-user` specified, its delegation tokens of hadoop cluster 
services are obtained by current kerberos user and can not be renewed by itself.  
Thus, engine's lifetime is limited by the lifetime of delegation tokens.  
To remove this limitation, kyuubi renews delegation tokens at server side in Hadoop Credentials Manager.

Engine submitted with principal and keytab can renew delegation tokens by itself. 
But for implementation simplicity, kyuubi server will also renew delegation tokens for it.

## Configurations

### Cluster Services
Kyuubi currently supports renew delegation tokens of Hadoop filesystems and Hive metastore servers.

#### Hadoop client configurations
Set `HADOOP_CONF_DIR` in `$KYUUBI_HOME/conf/kyuubi-env.sh` if it hasn't been set yet, e.g.

```bash
$ echo "export HADOOP_CONF_DIR=/path/to/hadoop/conf" >> $KYUUBI_HOME/conf/kyuubi-env.sh
```
Extra Hadoop filesystems can be specified in `$KYUUBI_HOME/conf/kyuubi-defaults.conf`
by `kyuubi.credentials.hadoopfs.uris` in comma separated list.

#### Hive metastore configurations

##### Via kyuubi-defaults.conf
Specify Hive metastore configurations In `$KYUUBI_HOME/conf/kyuubi-defaults.conf`. Hadoop Credentials 
Manager will load the configurations when initialized.

##### Via hive-site.xml
Place your copy of `hive-site.xml` into `$KYUUBI_HOME/conf`, Kyuubi will load this config file to 
its classpath.

This version of configuration has lower priority than those in `$KYUUBI_HOME/conf/kyuubi-defaults.conf`.

##### Via JDBC Connection URL
Hive configurations specified in JDBC connection URL are ignored by Hadoop Credentials Manager as
Hadoop Credentials Manager is initialized when Kyuubi server starts.

### Credentials Renewal

Key | Default | Meaning | Type | Since
--- | --- | --- | --- | ---
kyuubi\.credentials<br>\.hadoopfs\.enabled|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>true</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>Whether to renew Hadoop filesystem delegation tokens</div>|<div style='width: 30pt'>boolean</div>|<div style='width: 20pt'>1.4.0</div>
kyuubi\.credentials<br>\.hadoopfs\.uris|<div style='width: 65pt;word-wrap: break-word;white-space: normal'></div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>Extra Hadoop filesystem URIs for which to request delegation tokens. The filesystem that hosts fs.defaultFS does not need to be listed here.</div>|<div style='width: 30pt'>seq</div>|<div style='width: 20pt'>1.4.0</div>
kyuubi\.credentials<br>\.hive\.enabled|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>true</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>Whether to renew Hive metastore delegation token</div>|<div style='width: 30pt'>boolean</div>|<div style='width: 20pt'>1.4.0</div>
kyuubi\.credentials<br>\.renewal\.interval|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>PT1H</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>How often Kyuubi renews one user's delegation tokens</div>|<div style='width: 30pt'>duration</div>|<div style='width: 20pt'>1.4.0</div>
kyuubi\.credentials<br>\.renewal\.retry\.wait|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>PT1M</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>How long to wait before retrying to fetch new credentials after a failure.</div>|<div style='width: 30pt'>duration</div>|<div style='width: 20pt'>1.4.0</div>
