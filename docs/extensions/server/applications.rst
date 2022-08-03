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

Kyuubi supports configuring custom ``org.apache.kyuubi.engine.ApplicationOperation`` for extra cluster manager which provides an ability to control application, including getting information, killing.

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

      /**
        * (killed or not, hint message)
        */
      type KillResponse = (Boolean, String)

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

For now, Kyuubi has already supported three built-in application operations: ``JpsApplicationOperation``, ``YarnApplicationOperation`` and ``KubernetesApplicationOperation``.
