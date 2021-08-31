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

# Hadoop DelegationToken Manager

When working with a kerberos-enabled cluster, Kyuubi renews cluster services' delegation tokens 
periodically through a Hadoop DelegationToken Manager for client users.  

## Configurations

### Cluster Services
In order to renew DelegationTokens, Hadoop client configurations and Hive metastore configurations are needed.  
For Hadoop client configurations, set `HADOOP_CONF_DIR` in `$KYUUBI_HOME/conf/kyuubi-env.sh` if it hasn't been set yet, e.g.

```bash
$ echo "export HADOOP_CONF_DIR=/path/to/hadoop/conf" >> $KYUUBI_HOME/conf/kyuubi-env.sh
```
For Hive metastore configurations, either set [Via kyuubi-defaults.conf](../deployment/hive_metastore.md#Via kyuubi-defaults.conf)
or [Via hive-site.xml](../deployment/hive_metastore.md#Via hive-site.xml).  
Note that when set [Via kyuubi-defaults.conf](../deployment/hive_metastore.md#Via kyuubi-defaults.conf),
only _**Hive primitive configurations**_, e.g. `hive.metastore.uris`, are honored.


### DelegationToken Renewal

Key | Default | Meaning | Type | Since
--- | --- | --- | --- | ---
kyuubi\.security<br>\.credentials\.hadoopfs<br>\.enabled|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>true</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>Whether to renew HadoopFS DelegationToken</div>|<div style='width: 30pt'>boolean</div>|<div style='width: 20pt'>1.4.0</div>
kyuubi\.security<br>\.credentials\.hadoopfs<br>\.urls|<div style='width: 65pt;word-wrap: break-word;white-space: normal'></div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>Extra Hadoop filesystem URLs for which to request delegation tokens. The filesystem that hosts fs.defaultFS does not need to be listed here.</div>|<div style='width: 30pt'>seq</div>|<div style='width: 20pt'>1.4.0</div>
kyuubi\.security<br>\.credentials\.hive<br>\.enabled|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>true</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>Whether to renew HiveMetaStore DelegationToken</div>|<div style='width: 30pt'>boolean</div>|<div style='width: 20pt'>1.4.0</div>
kyuubi\.security<br>\.credentials\.renewal<br>\.interval|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>PT1H</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>How often Kyuubi renews one user's DelegationTokens</div>|<div style='width: 30pt'>duration</div>|<div style='width: 20pt'>1.4.0</div>
kyuubi\.security<br>\.credentials\.renewal<br>\.retryWait|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>PT1M</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>How long to wait before retrying to fetch new credentials after a failure.</div>|<div style='width: 30pt'>duration</div>|<div style='width: 20pt'>1.4.0</div>
