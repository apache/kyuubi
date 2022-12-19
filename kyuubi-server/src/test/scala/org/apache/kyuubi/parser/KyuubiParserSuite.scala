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
import org.apache.kyuubi.sql.parser.KyuubiParser
import org.apache.kyuubi.sql.plan.PassThroughNode
import org.apache.kyuubi.sql.plan.command.DescribeSession

class KyuubiParserSuite extends KyuubiFunSuite {

  private var parser: KyuubiParser = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    parser = new KyuubiParser()
  }

  test("Parse PassThroughNode") {
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

  test("Describe session") {
    val node = parser.parsePlan("KYUUBI DESC SESSION")

    assert(node.isInstanceOf[DescribeSession])
    assert(node.name() == "Describe Session Node")
  }
}
