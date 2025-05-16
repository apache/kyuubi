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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{SparkPlan, UnionExec}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec

object WriteUtils {
  def isWrite(session: SparkSession, plan: SparkPlan): Boolean = {
    plan match {
      case _: DataWritingCommandExec => true
      case _: V2TableWriteExec => true
      case u: UnionExec if u.children.nonEmpty => u.children.forall(isWrite(session, _))
      case _ => false
    }
  }
}
