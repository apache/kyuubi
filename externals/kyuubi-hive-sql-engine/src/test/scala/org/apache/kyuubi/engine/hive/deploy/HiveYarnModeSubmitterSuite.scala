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
package org.apache.kyuubi.engine.hive.deploy

import java.io.File

import scala.collection.mutable.ListBuffer

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiFunSuite, SCALA_COMPILE_VERSION}
import org.apache.kyuubi.util.JavaUtils

class HiveYarnModeSubmitterSuite extends KyuubiFunSuite {
  val hiveEngineHome: String = JavaUtils.getCodeSourceLocation(getClass).split("/target")(0)

  test("hadoop class path") {
    val jars = new ListBuffer[File]
    val classpath =
      s"$hiveEngineHome/target/scala-$SCALA_COMPILE_VERSION/jars/*:" +
        s"$hiveEngineHome/target/kyuubi-hive-sql-engine-$SCALA_COMPILE_VERSION-$KYUUBI_VERSION.jar"
    HiveYarnModeSubmitter.parseClasspath(classpath, jars)
    assert(jars.nonEmpty)
    assert(jars.exists(
      _.getName == s"kyuubi-hive-sql-engine-$SCALA_COMPILE_VERSION-$KYUUBI_VERSION.jar"))
  }

}
