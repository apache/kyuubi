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

Handle Events with Custom Event Handler
=======================================

.. caution:: unstable

Custom Event Handler
--------------------

Kyuubi supports custom event handler. It is usually used to write Kyuubi events to some external systems. For example, Kafka, ElasticSearch, etc.

The steps of injecting custom event handler
--------------------------------------

1. create a custom class which implements the ``org.apache.kyuubi.events.handler.CustomEventHandlerProvider``, and add a file named org.apache.kyuubi.events.handler.CustomEventHandlerProvider in the src/main/resources/META-INF/services folder of jar, its content is the custom class name.
2. compile and put the jar into ``$KYUUBI_HOME/jars``.
3. adding configuration at ``kyuubi-defaults.conf``:

   .. code-block:: java

      kyuubi.engine.event.loggers=CUSTOM

The ``org.apache.kyuubi.events.handler.CustomEventHandlerProvider`` has a zero-arg constructor, it can create a custom EventHandler.

.. code-block:: java

   trait CustomEventHandlerProvider {
     def create(kyuubiConf: KyuubiConf): EventHandler[KyuubiEvent]
   }

Example
-------

We have a custom class ``Fake1EventHandlerProvider``:

.. code-block:: java

   class Fake1EventHandlerProvider extends CustomEventHandlerProvider {
     override def create(kyuubiConf: KyuubiConf): EventHandler[KyuubiEvent] = {
       new Fake1EventHandler(kyuubiConf)
     }
   }

   class Fake1EventHandler(kyuubiConf: KyuubiConf) extends EventHandler[KyuubiEvent] {
     override def apply(kyuubiEvent: KyuubiEvent): Unit = {
       // sending events to external system.
     }
   }

You can send each KyuubiEvent to an external system by ``Fake1EventHandler``.
