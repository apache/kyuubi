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

package org.apache.kyuubi.plugin.spark.authz.util

import scala.util.{Failure, Success, Try}

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SPARK_VERSION
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

private[authz] object AuthZUtils {

  /**
   * fixme error handling need improve here
   */
  def getFieldVal[T](o: Any, name: String): T = {
    Try {
      val field = o.getClass.getDeclaredField(name)
      field.setAccessible(true)
      field.get(o)
    } match {
      case Success(value) => value.asInstanceOf[T]
      case Failure(e) =>
        val candidates = o.getClass.getDeclaredFields.map(_.getName).mkString("[", ",", "]")
        throw new RuntimeException(s"$name not in $candidates", e)
    }
  }

  def invoke(
      obj: AnyRef,
      methodName: String,
      args: (Class[_], AnyRef)*): AnyRef = {
    val (types, values) = args.unzip
    val method = obj.getClass.getDeclaredMethod(methodName, types: _*)
    method.setAccessible(true)
    method.invoke(obj, values: _*)
  }

  /**
   * Get the active session user
   * @param spark spark context instance
   * @return the user name
   */
  def getAuthzUgi(spark: SparkContext): UserGroupInformation = {
    // kyuubi.session.user is only used by kyuubi
    val user = spark.getLocalProperty("kyuubi.session.user")
    if (user != null && user != UserGroupInformation.getCurrentUser.getShortUserName) {
      UserGroupInformation.createRemoteUser(user)
    } else {
      UserGroupInformation.getCurrentUser
    }
  }

  def hasResolvedHiveTable(plan: LogicalPlan): Boolean = {
    plan.nodeName == "HiveTableRelation" && plan.resolved
  }

  def getHiveTable(plan: LogicalPlan): CatalogTable = {
    getFieldVal[CatalogTable](plan, "tableMeta")
  }

  def hasResolvedDatasourceTable(plan: LogicalPlan): Boolean = {
    plan.nodeName == "LogicalRelation" && plan.resolved
  }

  def getDatasourceTable(plan: LogicalPlan): Option[CatalogTable] = {
    getFieldVal[Option[CatalogTable]](plan, "catalogTable")
  }

  /**
   * Given a Kyuubi/Spark/Hive version string,
   * return the (major version number, minor version number).
   * E.g., for 2.0.1-SNAPSHOT, return (2, 0).
   */
  def majorMinorVersion(version: String): (Int, Int) = {
    """^(\d+)\.(\d+)(\..*)?$""".r.findFirstMatchIn(version) match {
      case Some(m) =>
        (m.group(1).toInt, m.group(2).toInt)
      case None =>
        throw new IllegalArgumentException(s"Tried to parse '$version' as a project" +
          s" version string, but it could not find the major and minor version numbers.")
    }
  }

  /**
   * Given a Kyuubi/Spark/Hive version string, return the major version number.
   * E.g., for 2.0.1-SNAPSHOT, return 2.
   */
  def majorVersion(version: String): Int = majorMinorVersion(version)._1

  /**
   * Given a Kyuubi/Spark/Hive version string, return the minor version number.
   * E.g., for 2.0.1-SNAPSHOT, return 0.
   */
  def minorVersion(version: String): Int = majorMinorVersion(version)._2

  def isSparkVersionAtMost(ver: String): Boolean = {
    val runtimeMajor = majorVersion(SPARK_VERSION)
    val targetMajor = majorVersion(ver)
    (runtimeMajor < targetMajor) || {
      val runtimeMinor = minorVersion(SPARK_VERSION)
      val targetMinor = minorVersion(ver)
      runtimeMajor == targetMajor && runtimeMinor <= targetMinor
    }
  }

  def isSparkVersionAtLeast(ver: String): Boolean = {
    val runtimeMajor = majorVersion(SPARK_VERSION)
    val targetMajor = majorVersion(ver)
    (runtimeMajor > targetMajor) || {
      val runtimeMinor = minorVersion(SPARK_VERSION)
      val targetMinor = minorVersion(ver)
      runtimeMajor == targetMajor && runtimeMinor >= targetMinor
    }
  }

  def isSparkVersionEqualTo(ver: String): Boolean = {
    val runtimeMajor = majorVersion(SPARK_VERSION)
    val targetMajor = majorVersion(ver)
    val runtimeMinor = minorVersion(SPARK_VERSION)
    val targetMinor = minorVersion(ver)
    runtimeMajor == targetMajor && runtimeMinor == targetMinor
  }
}
