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

package org.apache.kyuubi.service

import java.nio.file.Paths

// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.util.AssertionUtils._
import org.apache.kyuubi.util.GoldenFileUtils._

class CheckServerSPISuite extends AnyFunSuite {
  // scalastyle:on

  test("check server SPI service file sorted") {
    Seq(
      "org.apache.hadoop.security.token.TokenIdentifier",
      "org.apache.kyuubi.credentials.HadoopDelegationTokenProvider",
      "org.apache.kyuubi.engine.ApplicationOperation")
      .foreach { fileName =>
        val filePath = Paths.get(
          s"${getCurrentModuleHome(this)}/src/main/resources/META-INF/services/$fileName")
        assertFileContentSorted(filePath)
      }
  }
}
