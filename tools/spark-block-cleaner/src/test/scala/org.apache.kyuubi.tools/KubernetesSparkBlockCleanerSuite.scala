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

package org.apache.kyuubi.tools

import java.io.File
import java.nio.file.Files
import java.util.UUID

import org.apache.kyuubi.{KyuubiFunSuite, Utils}

class KubernetesSparkBlockCleanerSuite extends KyuubiFunSuite {
  import KubernetesSparkBlockCleanerConstants._

  private val rootDir = Utils.createTempDir()
  private val cacheDir = Seq("1", "2").map(rootDir.resolve)
  private val block1 = new File(cacheDir.head.toFile, s"blockmgr-${UUID.randomUUID.toString}")
  private val block2 = new File(cacheDir.head.toFile, s"blockmgr-${UUID.randomUUID.toString}")

  // do not remove
  private val subDir1 = new File(block1, "01")
  // do not remove
  private val data11 = new File(subDir1, "shuffle_0_0_0")
  // remove
  private val data12 = new File(subDir1, "shuffle_0_0_1")

  // remove
  private val subDir2 = new File(block2, "02")
  // remove
  private val data21 = new File(subDir1, "shuffle_0_1_0")

  private def deleteRecursive(path: File): Unit = {
    path.listFiles.foreach { f =>
      if (f.isDirectory) {
        deleteRecursive(f)
      } else {
        f.delete()
      }
    }
    path.delete()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    cacheDir.foreach(Files.createDirectories(_))

    // create some dir
    Files.createDirectories(block1.toPath)
    // hash sub dir
    Files.createDirectory(subDir1.toPath)
    data11.createNewFile()
    data11.setLastModified(System.currentTimeMillis() - 10)
    data12.createNewFile()
    Files.write(data12.toPath, "111".getBytes())
    data12.setLastModified(System.currentTimeMillis() - 10000000)

    Files.createDirectories(block2.toPath)
    Files.createDirectory(subDir2.toPath)
    subDir2.setLastModified(System.currentTimeMillis() - 10000000)
    data21.createNewFile()
    data21.setLastModified(System.currentTimeMillis() - 10000000)
  }

  override def afterAll(): Unit = {
    deleteRecursive(block1)
    deleteRecursive(block2)

    super.afterAll()
  }

  private def updateEnv(name: String, value: String): Unit = {
    val env = System.getenv
    val field = env.getClass.getDeclaredField("m")
    field.setAccessible(true)
    field.get(env).asInstanceOf[java.util.Map[String, String]].put(name, value)
  }

  test("test clean") {
    updateEnv(CACHE_DIRS_KEY, cacheDir.mkString(","))
    updateEnv(FILE_EXPIRED_TIME_KEY, "600000")
    updateEnv(SCHEDULE_INTERNAL, "1")
    updateEnv("kyuubi.testing", "true")

    KubernetesSparkBlockCleaner.main(Array.empty)

    assert(block1.exists())
    assert(subDir1.exists())
    assert(data11.exists())
    assert(!data12.exists())

    assert(block2.exists())
    assert(!subDir2.exists())
    assert(!data21.exists())
  }
}
