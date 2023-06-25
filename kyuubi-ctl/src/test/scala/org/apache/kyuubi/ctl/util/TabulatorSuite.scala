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

package org.apache.kyuubi.ctl.util

import org.apache.kyuubi.KyuubiFunSuite

class TabulatorSuite extends KyuubiFunSuite {
  test("format rows have null") {
    val rows: Array[Array[String]] = Array(Array("1", ""), Array(null, "2"))
    // scalastyle:off
    val expected =
      """
        |╔═════╤════╗
        |║ c1  │ c2 ║
        |╠═════╪════╣
        |║ 1   │    ║
        |╟─────┼────╢
        |║ N/A │ 2  ║
        |╚═════╧════╝
        |""".stripMargin
    // scalastyle:on
    val result = Tabulator.format("test", Array("c1", "c2"), rows)
    assert(result.contains(expected))
  }
}
