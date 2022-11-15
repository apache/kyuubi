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


# Kyuubi Migration Guide

## Upgrading from Kyuubi 1.6 to 1.7 
* In Kyuubi 1.7, `kyuubi.ha.zookeeper.engine.auth.type` does not fallback to `kyuubi.ha.zookeeper.auth.type`.  
  When Kyuubi engine does Kerberos authentication with Zookeeper, user needs to explicitly set `kyuubi.ha.zookeeper.engine.auth.type` to `KERBEROS`.
* Since Kyuubi 1.7, Kyuubi returns engine's information for `GetInfo` request instead of server. To restore the previous behavior, set `kyuubi.server.info.provider` to `SERVER`.

## Upgrading from Kyuubi 1.6.0 to 1.6.1
* Since Kyuubi 1.6.1, `kyuubi.ha.zookeeper.engine.auth.type` does not fallback to `kyuubi.ha.zookeeper.auth.type`.  
  When Kyuubi engine does Kerberos authentication with Zookeeper, user needs to explicitly set `kyuubi.ha.zookeeper.engine.auth.type` to `KERBEROS`.

## Upgrading from Kyuubi 1.5 to 1.6
* Kyuubi engine gets Zookeeper principal & keytab from `kyuubi.ha.zookeeper.auth.principal` & `kyuubi.ha.zookeeper.auth.keytab`.    
  `kyuubi.ha.zookeeper.auth.principal` & `kyuubi.ha.zookeeper.auth.keytab` fallback to `kyuubi.kinit.principal` & `kyuubi.kinit.keytab` when not set.    
  Since Kyuubi 1.6, `kyuubi.kinit.principal` & `kyuubi.kinit.keytab` are filtered out from Kyuubi engine's conf for better security.  
  When Kyuubi engine does Kerberos authentication with Zookeeper, user needs to explicitly set `kyuubi.ha.zookeeper.auth.principal` & `kyuubi.ha.zookeeper.auth.keytab`.

