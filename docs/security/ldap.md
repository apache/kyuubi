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

# Configure Kyuubi to Use LDAP Authentication

Kyuubi supports authentication via LDAP. Users can log into Kyuubi using their credentials. Then, Kyuubi can request
LDAP server to verify user's identity and grant right access permissions based on their LDAP roles and groups.

## Enable LDAP Authentication

To enable the LDAP authentication, we need to configure the following properties to `$KYUUBI_HOME/conf/kyuubi-defaults.conf`
on each node where kyuubi server is installed.

```properties example
kyuubi.authentication=LDAP
kyuubi.authentication.ldap.baseDN=dc=com
kyuubi.authentication.ldap.domain=kyuubi.com
kyuubi.authentication.ldap.binddn=uid=kyuubi,OU=Users,DC=apache,DC=org
kyuubi.authentication.ldap.bindpw=kyuubi123123
kyuubi.authentication.ldap.url=ldap://hostname.com:389/
```