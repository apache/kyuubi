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

package org.apache.kyuubi.parser

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.engine.EngineType
import org.apache.kyuubi.parser.node.PassThroughNode
import org.apache.kyuubi.parser.node.runnable.{AlterEngineConfNode, KillEngineNode, LaunchEngineNode}
import org.apache.kyuubi.parser.node.translate.{RenameTableNode, TranslateUtils}

class KyuubiParserSuite extends KyuubiFunSuite {

  private var parser: KyuubiParser = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    parser = new KyuubiParser()
  }

  test("Parse Pass Through Node") {
    val node = parser.parsePlan("SELECT * FROM T1;")

    assert(node.isInstanceOf[PassThroughNode])
    assert(node.name() == "Pass Through Node")

    val node2 = parser.parsePlan("INSERT INTO")
    val node3 = parser.parsePlan("A B C")
    val node4 = parser.parsePlan("A.. B C")
    assert(node2.isInstanceOf[PassThroughNode])
    assert(node3.isInstanceOf[PassThroughNode])
    assert(node4.isInstanceOf[PassThroughNode])
  }

  test("Parse Kill Engine Node") {
    val node = parser.parsePlan("DROP ENGINE")

    assert(node.isInstanceOf[KillEngineNode])
    assert(node.name() == "Kill Engine Node")
  }

  test("Parse launch Engine Node") {
    val node = parser.parsePlan("CREATE ENGINE")

    assert(node.isInstanceOf[LaunchEngineNode])
    assert(node.name() == "Launch Engine Node")
  }

  test("Parse Alter session conf Node") {
    val node = parser.parsePlan("ALTER SESSION SET (spark.driver.memory=1)")

    assert(node.isInstanceOf[AlterEngineConfNode])
    assert(node.name() == "Alter Engine Conf")
    assert(node.asInstanceOf[AlterEngineConfNode].config == Map("spark.driver.memory" -> "1"))

    val node2 = parser.parsePlan("ALTER SESSION SET (spark.driver.memory=\"1G\")")
    assert(node2.isInstanceOf[AlterEngineConfNode])
    assert(node2.asInstanceOf[AlterEngineConfNode].config == Map("spark.driver.memory" -> "1G"))

    val node3 =
      parser.parsePlan("ALTER SESSION SET (spark.driver.memory=\"1G\", spark.driver.cores=16)")
    assert(node3.isInstanceOf[AlterEngineConfNode])
    assert(node3.asInstanceOf[AlterEngineConfNode].config == Map(
      "spark.driver.memory" -> "1G",
      "spark.driver.cores" -> "16"))
  }

  test("Parse Rename Table Node") {
    val node = parser.parsePlan("RENAME TABLE T1 TO T2")

    assert(node.isInstanceOf[RenameTableNode])
    val renameTableNode = node.asInstanceOf[RenameTableNode]
    assert(renameTableNode.from == "T1")
    assert(renameTableNode.to == "T2")

    EngineType.values.foreach { engineType =>
      if (EngineType.JDBC != engineType) {
        val translatedSQL = TranslateUtils.translate(renameTableNode, engineType)
        assert(translatedSQL == "ALTER TABLE T1 RENAME TO T2")
      }
    }
  }
}
