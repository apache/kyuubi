/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.plugin.lineage.helper

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SPARK_VERSION
import org.apache.spark.kyuubi.lineage.SparkContextHelper

import org.apache.kyuubi.util.SemanticVersion

object SparkListenerHelper {

  lazy val SPARK_RUNTIME_VERSION: SemanticVersion = SemanticVersion(SPARK_VERSION)

  def currentUser: String = UserGroupInformation.getCurrentUser.getShortUserName

  def sessionUser: Option[String] =
    Option(SparkContextHelper.globalSparkContext.getLocalProperty(KYUUBI_SESSION_USER))

  final val KYUUBI_SESSION_USER = "kyuubi.session.user"
}
