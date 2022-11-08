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
package org.apache.kyuubi.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.AddJarsCommand

case class AddRepeatJarCloseSession(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {

    plan match {
      case i @ AddJarsCommand(paths) =>
        val checkExistsResult = findJarNameIfExists(session.sparkContext, paths)
        if (checkExistsResult.nonEmpty) {
          session.close()
          throw new IllegalStateException(s"${checkExistsResult.mkString(",")} " +
            s"has been added ,all sql that after this add jar command will be discard, " +
            s"please check sql and submit again")
        } else {
          plan
        }
      case _ => plan
    }
    plan
  }

  private def findJarNameIfExists(sparkContext: SparkContext,
                                 paths: Seq[String]): Seq[String] = {
    val existsJarFileNames = sparkContext.listJars().map(_.split("/").tail.apply(0))
    val addFileNames = paths.map(_.split("/").tail.apply(0))
    val existsJars = existsJarFileNames.intersect(addFileNames)
    existsJars
  }

}
