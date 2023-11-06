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

import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.slf4j.LoggerFactory

import org.apache.kyuubi.plugin.spark.authz.OperationType
import org.apache.kyuubi.plugin.spark.authz.OperationType.OperationType

/**
 * A command specification contains
 *  - different [[Descriptor]]s for specific implementations. It's a list to cover:
 *    - A command may have multiple object to describe, such as create table A like B
 *    - An object descriptor may vary through spark versions, it wins at least once if one of
 *      the descriptors matches
 *  - the classname of a command which this spec point to
 *  - the [[OperationType]] of this command which finally maps to an access privilege
 */
trait CommandSpec extends {
  @JsonIgnore
  final protected val LOG = LoggerFactory.getLogger(getClass)
  def classname: String
  def opType: String
  final def operationType: OperationType = OperationType.withName(opType)
}

trait CommandSpecs[T <: CommandSpec] {
  def specs: Seq[T]
}

/**
 * A specification describe a database command
 *
 * @param classname the database command classname
 * @param databaseDescs a list of database descriptors
 * @param opType operation type, e.g. CREATEDATABASE
 */
case class DatabaseCommandSpec(
    classname: String,
    databaseDescs: Seq[DatabaseDesc],
    opType: String = "QUERY") extends CommandSpec {}

/**
 * A specification describe a function command
 *
 * @param classname the database command classname
 * @param functionDescs a list of function descriptors
 * @param opType operation type, e.g. DROPFUNCTION
 */
case class FunctionCommandSpec(
    classname: String,
    functionDescs: Seq[FunctionDesc],
    opType: String) extends CommandSpec

/**
 * A specification describe a table command
 *
 * @param classname the database command classname
 * @param tableDescs a list of table descriptors
 * @param opType operation type, e.g. DROPFUNCTION
 * @param queryDescs the query descriptors a table command may have
 */
case class TableCommandSpec(
    classname: String,
    tableDescs: Seq[TableDesc],
    opType: String = OperationType.QUERY.toString,
    queryDescs: Seq[QueryDesc] = Nil,
    uriDescs: Seq[UriDesc] = Nil) extends CommandSpec {
  def queries: LogicalPlan => Seq[LogicalPlan] = plan => {
    queryDescs.flatMap { qd =>
      try {
        qd.extract(plan)
      } catch {
        case e: Exception =>
          LOG.debug(qd.error(plan, e))
          None
      }
    }
  }
}

case class ScanSpec(
    classname: String,
    scanDescs: Seq[ScanDesc],
    functionDescs: Seq[FunctionDesc] = Seq.empty,
    uriDescs: Seq[UriDesc] = Seq.empty) extends CommandSpec {
  override def opType: String = OperationType.QUERY.toString
  def tables: (LogicalPlan, SparkSession) => Seq[Table] = (plan, spark) => {
    scanDescs.flatMap { td =>
      try {
        td.extract(plan, spark)
      } catch {
        case e: Exception =>
          LOG.debug(td.error(plan, e))
          None
      }
    }
  }

  def uris: LogicalPlan => Seq[Uri] = plan => {
    uriDescs.flatMap { ud =>
      try {
        ud.extract(plan)
      } catch {
        case e: Exception =>
          LOG.debug(ud.error(plan, e))
          None
      }
    }
  }

  def functions: (Expression) => Seq[Function] = (expr) => {
    functionDescs.flatMap { fd =>
      try {
        Some(fd.extract(expr))
      } catch {
        case e: Exception =>
          LOG.debug(fd.error(expr, e))
          None
      }
    }
  }
}
