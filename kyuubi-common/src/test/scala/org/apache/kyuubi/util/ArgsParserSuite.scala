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
package org.apache.kyuubi.util

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf

class ArgsParserSuite extends KyuubiFunSuite {

  test("test args parser") {
    val args = Array[String]("--conf", "k1=v1", "--conf", " k2 = v2")
    val conf = new KyuubiConf()
    ArgsParser.fromCommandLineArgs(args, conf)
    assert(conf.getOption("k1").get == "v1")
    assert(conf.getOption("k2").get == "v2")
  }

  test("test empty args parse") {
    val args = Array[String]()
    val conf = new KyuubiConf()
    ArgsParser.fromCommandLineArgs(args, conf)
    assert(conf.getAll.size == 1)
  }

}
