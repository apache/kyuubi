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

package org.apache.kyuubi.plugin.spark.authz.serde

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

case class FunctionCommandSpec(
    classname: String,
    functionDescs: Seq[FunctionDesc],
    opType: String) extends CommandSpec

object FunctionCommandSpec {
  def main(args: Array[String]): Unit = {
    val CreateFunction = {
      val cmd = "org.apache.spark.sql.execution.command.CreateFunctionCommand"
      val functionTypeDesc = FunctionTypeDesc(
        "isTemp",
        classOf[TempMarkerFunctionTypeExtractor].getSimpleName,
        Seq("TEMP"))
      val databaseDesc = DatabaseDesc(
        "databaseName", classOf[StringOptionDatabaseExtractor].getSimpleName)
      val functionDesc = FunctionDesc(
        "functionName",
        "StringFunctionExtractor",
        Some(databaseDesc),
        Some(functionTypeDesc))
      FunctionCommandSpec(cmd, Seq(functionDesc), "CREATEFUNCTION")
    }

    val DescribeFunction = {
      val cmd = "org.apache.spark.sql.execution.command.DescribeFunctionCommand"
      val skips = Seq("TEMP", "SYSTEM")
      val functionTypeDesc1 = FunctionTypeDesc("info", "ExpressionInfoFunctionTypeExtractor", skips)
      val functionDesc1 = FunctionDesc(
        "info",
        "ExpressionInfoFunctionExtractor",
        functionTypeDesc = Some(functionTypeDesc1),
        isInput = true)

      val functionTypeDesc2 =
        FunctionTypeDesc("functionName", "FunctionIdentifierFunctionTypeExtractor", skips)
      val functionDesc2 = FunctionDesc(
        "functionName",
        "FunctionIdentifierFunctionExtractor",
        functionTypeDesc = Some(functionTypeDesc2),
        isInput = true)
      FunctionCommandSpec(cmd, Seq(functionDesc1, functionDesc2), "DESCFUNCTION")
    }

    val DropFunction = {
      val cmd = "org.apache.spark.sql.execution.command.DropFunctionCommand"
      CreateFunction.copy(cmd, opType = "DROPFUNCTION")
    }

    val RefreshFunction = {
      val cmd = "org.apache.spark.sql.execution.command.RefreshFunctionCommand"
      val databaseDesc = DatabaseDesc("databaseName", "StringOptionDatabaseExtractor")
      val functionDesc = FunctionDesc(
        "functionName",
        "StringFunctionExtractor",
        Some(databaseDesc))
      FunctionCommandSpec(cmd, Seq(functionDesc), "RELOADFUNCTION")
    }

    val pluginHome = getClass.getProtectionDomain.getCodeSource.getLocation.getPath
      .split("target").head
    val writer = {
      val p =
        Paths.get(pluginHome, "src", "main", "resources", "function_command_spec.json")
      Files.newBufferedWriter(p, StandardCharsets.UTF_8)
    }
    val data = Array(
      CreateFunction,
      DropFunction,
      DescribeFunction,
      RefreshFunction)
    mapper.writerWithDefaultPrettyPrinter().writeValue(writer, data)
    writer.close()
  }
}
