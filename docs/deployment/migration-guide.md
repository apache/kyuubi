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

## Upgrading from Kyuubi 1.6.0 to 1.7.0
* In Kyuubi 1.6.0, Kyuubi provides the `kyuubi.server.info.provider` option to control get the server or the engine information, in Kyuubi 1.6.0, it defaults to the `SERVER`, after Kyuubi 1.7.0, it defaults to the `ENGINE`, which means that information about engine is returned by default, Setting the option as "SERVER" restores the previous behavior.
