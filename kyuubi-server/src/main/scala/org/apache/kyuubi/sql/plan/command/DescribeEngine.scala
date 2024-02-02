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

import org.apache.kyuubi.operation.IterableFetchIterator
import org.apache.kyuubi.session.{KyuubiSession, KyuubiSessionImpl}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTypeId
import org.apache.kyuubi.sql.schema.{Column, Row, Schema}

/**
 * A runnable node for description the current session engine.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   [DESC|DESCRIBE] ENGINE;
 * }}}
 */
case class DescribeEngine() extends RunnableCommand {

  override def run(kyuubiSession: KyuubiSession): Unit = {
    val rows = Seq(kyuubiSession.asInstanceOf[KyuubiSessionImpl]).map { session =>
      lazy val client = session.client
      val values = new ListBuffer[String]()
      values += client.engineId.getOrElse("")
      values += client.engineName.getOrElse("")
      values += client.engineUrl.getOrElse("")
      session.getEngineNode match {
        case Some(nodeInfo) =>
          values += s"${nodeInfo.host}:${nodeInfo.port}"
          values += nodeInfo.version.getOrElse("")
          values += nodeInfo.attributes.mkString(",")
        case None =>
          values += ("", "", "")
      }
      Row(values.toList)
    }
    iter = new IterableFetchIterator(rows)
  }

  override def resultSchema: Schema = {
    Schema(DescribeEngine.outputCols().toList)
  }

  override def name(): String = "Describe Engine Node"
}

object DescribeEngine {

  def outputCols(): Seq[Column] = {
    Seq(
      Column("ENGINE_ID", TTypeId.STRING_TYPE, Some("Kyuubi engine identify")),
      Column("ENGINE_NAME", TTypeId.STRING_TYPE, Some("Kyuubi engine name")),
      Column("ENGINE_URL", TTypeId.STRING_TYPE, Some("Kyuubi engine url")),
      Column("ENGINE_INSTANCE", TTypeId.STRING_TYPE, Some("Kyuubi engine instance host and port")),
      Column("ENGINE_VERSION", TTypeId.STRING_TYPE, Some("Kyuubi engine version")),
      Column("ENGINE_ATTRIBUTES", TTypeId.STRING_TYPE, Some("Kyuubi engine attributes")))
  }
}
