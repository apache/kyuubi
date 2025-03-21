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

# Configure Kyuubi to use LDAP Authentication

Kyuubi can be configured to enable frontend LDAP authentication for clients, such as the BeeLine, or the JDBC and ODBC drivers.
At present, only simple LDAP authentication mechanism involving username and password is supported. The client sends
a username and password to the Kyuubi server, and the Kyuubi server validates these credentials using an external LDAP service.

## Enable LDAP Authentication

To enable LDAP authentication for Kyuubi, LDAP-related configurations is required to be configured in
`$KYUUBI_HOME/conf/kyuubi-defaults.conf` on each node where Kyuubi server is installed.

For example,

```properties
kyuubi.authentication=LDAP
kyuubi.authentication.ldap.baseDN=dc=org
kyuubi.authentication.ldap.domain=apache.org
kyuubi.authentication.ldap.binddn=uid=kyuubi,OU=Users,DC=apache,DC=org
kyuubi.authentication.ldap.bindpw=kyuubi123123
kyuubi.authentication.ldap.url=ldap://hostname.com:389/
```

## User and Group Filter in LDAP

Kyuubi also supports complex LDAP cases as [Apache Hive](https://cwiki.apache.org/confluence/display/Hive/User+and+Group+Filter+Support+with+LDAP+Atn+Provider+in+HiveServer2#UserandGroupFilterSupportwithLDAPAtnProviderinHiveServer2-UserandGroupFilterSupportwithLDAP) does.

For example,

```properties
# Group Membership
kyuubi.authentication.ldap.groupClassKey=groupOfNames
kyuubi.authentication.ldap.groupDNPattern=CN=%s,OU=Groups,DC=apache,DC=org
kyuubi.authentication.ldap.groupFilter=group1,group2
kyuubi.authentication.ldap.groupMembershipKey=memberUid
# User Search List
kyuubi.authentication.ldap.userDNPattern=CN=%s,CN=Users,DC=apache,DC=org
kyuubi.authentication.ldap.userFilter=hive-admin,hive,hive-test,hive-user
# Custom Query
kyuubi.authentication.ldap.customLDAPQuery=(&(objectClass=group)(objectClass=top)(instanceType=4)(cn=Domain*)), (&(objectClass=person)(|(sAMAccountName=admin)(|(memberOf=CN=Domain Admins,CN=Users,DC=domain,DC=com)(memberOf=CN=Administrators,CN=Builtin,DC=domain,DC=com))))
```

Please refer to [Settings for LDAP authentication in Kyuubi](../configuration/settings.html?highlight=LDAP#authentication)
for all configurations.
