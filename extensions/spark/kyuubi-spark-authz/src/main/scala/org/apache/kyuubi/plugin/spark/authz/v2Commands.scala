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

package org.apache.kyuubi.plugin.spark.authz

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import org.apache.kyuubi.plugin.spark.authz.OperationType.{CREATEDATABASE, OperationType, QUERY}
import org.apache.kyuubi.plugin.spark.authz.PrivilegesBuilder.databasePrivileges
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils.{getFieldVal, isSparkVersionAtLeast, isSparkVersionAtMost, quote}

object v2Commands extends Enumeration {
  import scala.language.implicitConversions

  implicit def valueToV2CommandBuilder(x: Value): V2Command =
    x.asInstanceOf[V2Command]

  def accept(commandName: String): Boolean = {
    try {
      val cmd = v2Commands.withName(commandName)

      !(StringUtils.isNotBlank(cmd.mostVer) && isSparkVersionAtMost(cmd.mostVer)) &&
      !(StringUtils.isNotBlank(cmd.leastVer) && isSparkVersionAtLeast(cmd.leastVer))
    } catch {
      case e: NoSuchElementException => false
    }
  }

  private val defaultBuildInput: (
      LogicalPlan,
      ArrayBuffer[PrivilegeObject]) => Unit = (p, inputObjs) => {}

  private val defaultBuildOutput: (LogicalPlan, ArrayBuffer[PrivilegeObject]) => Unit =
    (p, outputObjs) => {}

  case class V2Command(
      leastVer: String = "",
      mostVer: String = "",
      buildInput: (LogicalPlan, ArrayBuffer[PrivilegeObject]) => Unit = defaultBuildInput,
      buildOutput: (LogicalPlan, ArrayBuffer[PrivilegeObject]) => Unit = defaultBuildOutput,
      operType: OperationType = QUERY)
    extends super.Val {

    def handle(
        plan: LogicalPlan,
        inputObjs: ArrayBuffer[PrivilegeObject],
        outputObjs: ArrayBuffer[PrivilegeObject]): Unit = {
      this.buildInput(plan, inputObjs)
      this.buildOutput(plan, outputObjs)
    }
  }

  // commands

  val CreateNamespace: V2Command = V2Command(
    operType = CREATEDATABASE,
    buildOutput = (plan, outputObjs) => {
      if (isSparkVersionAtLeast("3.3")) {
        val resolvedNamespace = getFieldVal[Any](plan, "name")
        val databases = getFieldVal[Seq[String]](resolvedNamespace, "nameParts")
        outputObjs += databasePrivileges(quote(databases))
      } else {
        val namespace = getFieldVal[Seq[String]](plan, "namespace")
        outputObjs += databasePrivileges(quote(namespace))
      }
    })

}
