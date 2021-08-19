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

# Kyuubi Authentication Mechanism

In a secure cluster, services should be able to identify and authenticate callers.
As the fact that the user claims does not necessarily mean this is true.

The authentication process of Kyuubi is used to verify the user identity that a client used to talk to the Kyuubi server.
Once done, a trusted connection will be set up between the client and server if successful; otherwise, rejected.

**Note** that, this authentication only authenticate whether a user can connect with Kyuubi server or not.
For other secured services that this user wants to interact with, he/she also needs to pass the authentication process of each service, for instance, Hive Metastore, YARN, HDFS.

In `$KYUUBI_HOME/conf/kyuubi-defaults.conf`, specify `kyuubi.authentication` to one of the authentication types listing below.

Key | Default | Meaning | Since
--- | --- | --- | ---
kyuubi\.authentication|<div style='width: 80pt;word-wrap: break-word;white-space: normal'>NONE</div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>Client authentication types.<ul> <li>NOSASL: raw transport.</li> <li>NONE: no authentication check.</li> <li>KERBEROS: Kerberos/GSSAPI authentication.</li> <li>LDAP: Lightweight Directory Access Protocol authentication.</li></ul></div>|<div style='width: 20pt'>1.0.0</div>


Key | Default | Meaning | Since
--- | --- | --- | ---
kyuubi\.authentication|<div style='width: 80pt;word-wrap: break-word;white-space: normal'>NONE</div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>Client authentication types.<ul> <li>NOSASL: raw transport.</li> <li>NONE: no authentication check.</li> <li>KERBEROS: Kerberos/GSSAPI authentication.</li> <li>LDAP: Lightweight Directory Access Protocol authentication.</li></ul></div>|<div style='width: 20pt'>1.0.0</div>
kyuubi\.authentication<br>\.ldap\.base\.dn|<div style='width: 80pt;word-wrap: break-word;white-space: normal'>&lt;undefined&gt;</div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>LDAP base DN.</div>|<div style='width: 20pt'>1.0.0</div>
kyuubi\.authentication<br>\.ldap\.domain|<div style='width: 80pt;word-wrap: break-word;white-space: normal'>&lt;undefined&gt;</div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>LDAP base DN.</div>|<div style='width: 20pt'>1.0.0</div>
kyuubi\.authentication<br>\.ldap\.url|<div style='width: 80pt;word-wrap: break-word;white-space: normal'>&lt;undefined&gt;</div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>SPACE character separated LDAP connection URL(s).</div>|<div style='width: 20pt'>1.0.0</div>
kyuubi\.authentication<br>\.sasl\.qop|<div style='width: 80pt;word-wrap: break-word;white-space: normal'>auth</div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>Sasl QOP enable higher levels of protection for Kyuubi communication with clients.<ul> <li>auth - authentication only (default)</li> <li>auth-int - authentication plus integrity protection</li> <li>auth-conf - authentication plus integrity and confidentiality protection. This is applicable only if Kyuubi is configured to use Kerberos authentication.</li> </ul></div>|<div style='width: 20pt'>1.0.0</div>


#### Using KERBEROS

If you are deploying Kyuubi with a kerberized Hadoop cluster, it is strongly recommended that `kyuubi.authentication` should be set to `KERBEROS` too.

Kerberos is a network authentication protocol that provides the tools of authentication and strong cryptography over the network.
The Kerberos protocol uses strong cryptography so that a client or a server can prove its identity to its server or client across an insecure network connection.
After a client and server have used Kerberos to prove their identity, they can also encrypt all of their communications to assure privacy and data integrity as they go about their business.

The Kerberos architecture is centered around a trusted authentication service called the key distribution center, or KDC.
Users and services in a Kerberos environment are referred to as principals;
each principal shares a secret, such as a password, with the KDC.

Set following for KERBEROS mode:

Key | Default | Meaning | Since
--- | --- | --- | ---
kyuubi\.kinit<br>\.principal|<div style='width: 80pt;word-wrap: break-word;white-space: normal'>&lt;undefined&gt;</div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>Name of the Kerberos principal.</div>|<div style='width: 20pt'>1.0.0</div>
kyuubi\.kinit\.keytab|<div style='width: 80pt;word-wrap: break-word;white-space: normal'>&lt;undefined&gt;</div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>Location of Kyuubi server's keytab.</div>|<div style='width: 20pt'>1.0.0</div>


For example,

- Configure with Kyuubi service principal 
```bash
kyuubi.authentication=KERBEROS
kyuubi.kinit.principal=spark/kyuubi.apache.org@KYUUBI.APACHE.ORG
kyuubi.kinit.keytab=/path/to/kyuuib.keytab
```

- Start Kyuubi
```bash
$ ./bin/kyuubi start
```

- Kinit with user principal and connect using beeline

```bash
$ kinit -kt user.keytab user.principal

$ beeline -u "jdbc:hive2://localhost:10009/;principal=spark/kyuubi.apache.org@KYUUBI.APACHE.ORG"
```
