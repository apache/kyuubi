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

import java.nio.file.{Files, Path, StandardOpenOption}

import scala.collection.JavaConverters._

import org.apache.kyuubi.util.AssertionUtils._

object GoldenFileUtils {
  def isRegenerateGoldenFiles: Boolean = sys.env.get("KYUUBI_UPDATE").contains("1")

  /**
   * Verify the golden file content when KYUUBI_UPDATE env is not equals to 1,
   * or regenerate the golden file content when KYUUBI_UPDATE env is equals to 1.
   *
   * @param path the path of file
   * @param lines the expected lines for validation or regeneration
   * @param regenScript the script for regeneration, used for hints when verification failed
   */
  def verifyOrRegenerateGoldenFile(
      path: Path,
      lines: Iterable[String],
      regenScript: String): Unit = {
    if (isRegenerateGoldenFiles) {
      Files.write(
        path,
        lines.asJava,
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING)
    } else {
      assertFileContent(path, lines, regenScript)
    }
  }
}
