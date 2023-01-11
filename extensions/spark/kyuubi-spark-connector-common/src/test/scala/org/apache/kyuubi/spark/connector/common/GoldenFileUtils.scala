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

package org.apache.kyuubi.spark.connector.common

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

object GoldenFileUtils {

  val LICENSE_HEADER: String =
    """/*
      | * Licensed to the Apache Software Foundation (ASF) under one or more
      | * contributor license agreements.  See the NOTICE file distributed with
      | * this work for additional information regarding copyright ownership.
      | * The ASF licenses this file to You under the Apache License, Version 2.0
      | * (the "License"); you may not use this file except in compliance with
      | * the License.  You may obtain a copy of the License at
      | *
      | *    http://www.apache.org/licenses/LICENSE-2.0
      | *
      | * Unless required by applicable law or agreed to in writing, software
      | * distributed under the License is distributed on an "AS IS" BASIS,
      | * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
      | * See the License for the specific language governing permissions and
      | * limitations under the License.
      | */""".stripMargin + "\n\n"

  private val regenerateGoldenFiles: Boolean = sys.env.get("KYUUBI_UPDATE").contains("1")

  private val baseResourcePath: Path =
    Paths.get(
      Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)
        .getResource(".")
        .getPath,
      Seq("..", "..", "..", "src", "main", "resources"): _*)

  private def fileToString(file: Path): String = {
    new String(Files.readAllBytes(file), StandardCharsets.UTF_8)
  }

  def generateGoldenFiles(
      dirctory: String,
      sqlName: String,
      schema: String,
      sumHash: String): (String, String) = {
    val goldenSchemaFile = Paths.get(
      baseResourcePath.toFile.getAbsolutePath,
      dirctory,
      s"${sqlName.stripSuffix(".sql")}.output.schema")

    val goldenHashFile = Paths.get(
      baseResourcePath.toFile.getAbsolutePath,
      dirctory,
      s"${sqlName.stripSuffix(".sql")}.output.hash")
    if (regenerateGoldenFiles) {
      Files.write(goldenSchemaFile, schema.getBytes)
      Files.write(goldenHashFile, sumHash.getBytes)
    }
    val expectedSchema = fileToString(goldenSchemaFile)
    val expectedHash = fileToString(goldenHashFile)
    (expectedSchema, expectedHash)
  }

}
