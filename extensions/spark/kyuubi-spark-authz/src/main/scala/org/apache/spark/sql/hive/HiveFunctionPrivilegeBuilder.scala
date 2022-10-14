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

package org.apache.spark.sql.hive

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import org.apache.kyuubi.plugin.spark.authz.{PrivilegeObject, PrivilegeObjectActionType}
import org.apache.kyuubi.plugin.spark.authz.PrivilegeObjectType.FUNCTION

object HiveFunctionPrivilegeBuilder {

  def build(
      plan: LogicalPlan,
      spark: SparkSession): (Seq[PrivilegeObject], Seq[PrivilegeObject]) = {
    val inputObjs = new ArrayBuffer[PrivilegeObject]
    plan transformAllExpressions {
      case hiveSimpeUDF @ HiveSimpleUDF(name, _, _) =>
        buildFunctionPrivilegeObject(name, inputObjs, spark)
        hiveSimpeUDF
      case hiveGenericUDF @ HiveGenericUDF(name, _, _) =>
        buildFunctionPrivilegeObject(name, inputObjs, spark)
        hiveGenericUDF
      case hiveUDAFFunction @ HiveUDAFFunction(name, _, _, _, _, _) =>
        buildFunctionPrivilegeObject(name, inputObjs, spark)
        hiveUDAFFunction
      case hiveGenericUDTF @ HiveGenericUDTF(name, _, _) =>
        buildFunctionPrivilegeObject(name, inputObjs, spark)
        hiveGenericUDTF
    }
    (inputObjs, Seq.empty)
  }

  private def buildFunctionPrivilegeObject(
      functionFullName: String,
      inputs: ArrayBuffer[PrivilegeObject],
      spark: SparkSession): Unit = {

    val (database, functionName) = getFunctionInfo(functionFullName, spark)
    if (database != null && spark.sessionState.catalog.isPersistentFunction(
        FunctionIdentifier(functionName, Some(database)))) {
      inputs += functionPrivileges(database, functionName)
    }
  }

  private def getFunctionInfo(functionName: String, spark: SparkSession) = {
    val items = functionName.split("\\.")
    if (items.length == 2) {
      (items(0), items(1))
    } else {
      Try {
        spark.sessionState.catalog.lookupFunctionInfo(FunctionIdentifier(functionName))
      } match {
        case Success(functionInfo) => (functionInfo.getDb, functionInfo.getName)
        case Failure(_) => (null, functionName)
      }
    }
  }

  private def functionPrivileges(
      db: String,
      functionName: String): PrivilegeObject = {
    PrivilegeObject(FUNCTION, PrivilegeObjectActionType.OTHER, db, functionName)
  }
}
