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

Manage Applications against Extra Cluster Managers
==================================================

.. versionadded:: 1.6.0

.. caution:: unstable

Inside Kyuubi, the Kyuubi server uses the ``ApplicationManager`` module to manage all applications launched by itself, including different kinds of Kyuubi engines and self-contained applications.

The ``ApplicationManager`` leverages methods provided by application operation implementations derived from ``org.apache.kyuubi.engine.ApplicationOperation`` to monitor the status of those applications and kill abnormal applications in case they get orphaned and may introduce more methods in the future.

An ``ApplicationOperation`` implementation is usually built upon clients or APIs provided by cluster managers, such as Hadoop YARN, Kubernetes, etc.

For now, Kyuubi has already supported several built-in application operations:

- ``JpsApplicationOperation``: an operation that can manage apps with a local process, e.g. a local mode spark application
- ``YarnApplicationOperation``: an operation that can manage apps with a Hadoop Yarn cluster, e.g. a spark on yarn application
- ``KubernetesApplicationOperation``: an operation that can manage apps with a k8s cluster, e.g. a spark on k8s application

Besides those built-in ones, Kyuubi also supports loading custom ``ApplicationOperation`` through the Java `ServiceLoader` (SPI) for extra cluster managers.

The rest of this article will show you the specifications and steps to build and enable a custom operation.

   .. code-block:: scala

      trait ApplicationOperation {

        /**
         * Step for initializing the instance.
         */
        def initialize(conf: KyuubiConf): Unit

        /**
         * Step to clean up the instance
         */
        def stop(): Unit

        /**
         * Called before other method to do a quick skip
         *
         * @param clusterManager the underlying cluster manager or just local instance
         */
        def isSupported(clusterManager: Option[String]): Boolean

        /**
         * Kill the app/engine by the unique application tag
         *
         * @param tag the unique application tag for engine instance.
         *            For example,
         *            if the Hadoop Yarn is used, for spark applications,
         *            the tag will be preset via spark.yarn.tags
         * @return a message contains response describing how the kill process.
         *
         * @note For implementations, please suppress exceptions and always return KillResponse
         */
        def killApplicationByTag(tag: String): KillResponse

        /**
         * Get the engine/application status by the unique application tag
         *
         * @param tag the unique application tag for engine instance.
         * @return [[ApplicationInfo]]
         */
        def getApplicationInfoByTag(tag: String): ApplicationInfo
      }

   .. code-block:: scala

      /**
        * (killed or not, hint message)
        */
      type KillResponse = (Boolean, String)

An ``ApplicationInfo`` is used to represented the application information, including application id, name, state, url address and error message.

   .. code-block:: scala

      object ApplicationState extends Enumeration {
        type ApplicationState = Value
        val PENDING, RUNNING, FINISHED, KILLED, FAILED, ZOMBIE, NOT_FOUND, UNKNOWN = Value
      }

      case class ApplicationInfo(
          id: String,
          name: String,
          state: ApplicationState,
          url: Option[String] = None,
          error: Option[String] = None)

For application state mapping, you can reference the implementation of yarn:

   .. code-block:: scala

      def toApplicationState(state: YarnApplicationState): ApplicationState = state match {
        case YarnApplicationState.NEW => ApplicationState.PENDING
        case YarnApplicationState.NEW_SAVING => ApplicationState.PENDING
        case YarnApplicationState.SUBMITTED => ApplicationState.PENDING
        case YarnApplicationState.ACCEPTED => ApplicationState.PENDING
        case YarnApplicationState.RUNNING => ApplicationState.RUNNING
        case YarnApplicationState.FINISHED => ApplicationState.FINISHED
        case YarnApplicationState.FAILED => ApplicationState.FAILED
        case YarnApplicationState.KILLED => ApplicationState.KILLED
        case _ =>
          warn(s"The yarn driver state: $state is not supported, " +
            "mark the application state as UNKNOWN.")
          ApplicationState.UNKNOWN
      }

Build A Custom Application Operation
------------------------------------

- reference kyuubi-server

   .. code-block:: xml

      <dependency>
         <groupId>org.apache.kyuubi</groupId>
         <artifactId>kyuubi-server_2.12</artifactId>
         <version>1.5.2-incubating</version>
         <scope>provided</scope>
      </dependency>

- create a custom class which implements the ``org.apache.kyuubi.engine.ApplicationOperation``.

- create a directory META-INF.services and a file with ``org.apache.kyuubi.engine.ApplicationOperation``:

   .. code-block:: java

      META-INF.services/org.apache.kyuubi.engine.ApplicationOperation

   then add your fully-qualified name of custom application operation into the file.


Enable Custom Application Operation
-----------------------------------

.. note:: Kyuubi uses Java SPI to load the custom Application Operation

- compile and put the jar into ``$KYUUBI_HOME/jars``
