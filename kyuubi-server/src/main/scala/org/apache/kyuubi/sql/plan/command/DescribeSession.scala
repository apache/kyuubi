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

package org.apache.kyuubi.sql.plan.command

import scala.collection.mutable.ListBuffer

import org.apache.hive.service.rpc.thrift.TTypeId

import org.apache.kyuubi.operation.IterableFetchIterator
import org.apache.kyuubi.session.KyuubiSession
import org.apache.kyuubi.sql.schema.{Column, Row, Schema}

/**
 * A runnable node for description the current session.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   [DESC|DESCRIBE] SESSION;
 * }}}
 */
case class DescribeSession() extends RunnableCommand {

  override def run(kyuubiSession: KyuubiSession): Unit = {
    val rows = Seq(kyuubiSession).map { session =>
      val values = new ListBuffer[String]()
      values += session.handle.identifier.toString
      values += session.user
      values += session.asInstanceOf[KyuubiSession].sessionType.toString
      Row(values.toList)
    }
    iter = new IterableFetchIterator(rows)
  }

  override def resultSchema: Schema = {
    Schema(DescribeSession.outputCols().toList)
  }

  override def name(): String = "Describe Session Node"
}

object DescribeSession {

  def outputCols(): Seq[Column] = {
    Seq(
      Column("id", TTypeId.STRING_TYPE, Some("Kyuubi session identify")),
      Column("user", TTypeId.STRING_TYPE, Some("Kyuubi session user")),
      Column("type", TTypeId.STRING_TYPE, Some("Kyuubi session type")))
  }
}
