.. Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

Inject Session Conf with Custom Config Advisor
==============================================

.. versionadded:: 1.5.0

The step of inject session conf
-------------------

1. create a custom class which implements the `org.apache.kyuubi.plugin.SessionConfAdvisor`.
2. adding configuration at `kyuubi-defaults.conf`:

   .. code-block:: java

      kyuubi.session.conf.advisor=${classname}

3. compile and put the jar into `$KYUUBI_HOME/jars`

The `org.apache.kyuubi.plugin.SessionConfAdvisor` has a zero-arg constructor and holds one method with user and session conf and return a new conf map back.

.. code-block:: java

   public interface SessionConfAdvisor {
     default Map<String, String> getConfOverlay(String user, Map<String, String> sessionConf) {
       return Collections.EMPTY_MAP;
     }
   }

Note that, the returned conf map will overwrite the original session conf.

Example
-------------------

We have a custom class `CustomSessionConfAdvisor`:

.. code-block:: java

   @Override
   public class CustomSessionConfAdvisor {
     Map<String, String> getConfOverlay(String user, Map<String, String> sessionConf) {
       if ("uly".equals(user)) {
         return Collections.singletonMap("spark.driver.memory", "1G");
       } else {
         return Collections.EMPTY_MAP;
       }
     }
   }

If a user `uly` create a connection with

.. code-block:: java

   jdbc:hive2://localhost:10009/;hive.server2.proxy.user=uly;#spark.driver.memory=2G

the final Spark application will allocate `spark.driver.memory=1G` rather 2G.
