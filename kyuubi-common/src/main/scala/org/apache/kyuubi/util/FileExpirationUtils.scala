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

import java.nio.file.{Path, Paths}
import java.util
import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.convert.ImplicitConversions.`collection asJava`

import org.apache.kyuubi.Utils

object FileExpirationUtils {
  private lazy val isCleanupShutdownHookInstalled = {
    addFilesCleanupOnExitShutdownHook()
    new AtomicBoolean(true)
  }

  private lazy val files: util.Set[String] =
    Collections.synchronizedSet(new util.LinkedHashSet[String]())

  private def addFilesCleanupOnExitShutdownHook(): Unit = {
    Utils.addShutdownHook(() => {
      new util.LinkedHashSet(files)
        .forEach(pathStr => Utils.deleteDirectoryRecursively(Paths.get(pathStr).toFile))
      files.clear()
    })
  }

  def deleteFileOnExit(path: Path): Unit = {
    if (path != null) {
      isCleanupShutdownHookInstalled.get()
      files.add(path.toString)
    }
  }

  def removeFromFileList(paths: String*): Unit = {
    files.removeAll(paths)
  }

}
