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

package org.apache.kyuubi.log

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.operation.log.SeekableBufferedReader

class SeekableBufferedReaderSuite extends KyuubiFunSuite {

  private val tmpDir = Utils.createTempDir()
  private val f1 = tmpDir.resolve("f1")
  private val f2 = tmpDir.resolve("f2")
  private val f3 = tmpDir.resolve("f3")

  override def beforeAll(): Unit = {
    super.beforeAll()

    val f1w = Files.newBufferedWriter(f1, StandardCharsets.UTF_8)
    0.until(10).foreach { i =>
      f1w.write(i + "\n")
    }
    f1w.flush()
    f1w.close()

    val f2w = Files.newBufferedWriter(f2, StandardCharsets.UTF_8)
    0.until(20).foreach { i =>
      f2w.write(i + "\n")
    }
    f2w.flush()
    f2w.close()

    val f3w = Files.newBufferedWriter(f3, StandardCharsets.UTF_8)
    0.until(30).foreach { i =>
      f3w.write(i + "\n")
    }
    f3w.flush()
    f3w.close()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    Utils.deleteDirectoryRecursively(tmpDir.toFile)
  }

  test("one file") {
    var reader = new SeekableBufferedReader(f1 :: Nil)
    var res = reader.readLine(0, 5).toSeq
    assert(res.size == 5)
    0.until(5).zipWithIndex.foreach { case (v, i) =>
      assert(res(i).toInt == v)
    }
    reader.close()

    // seek by from
    reader = new SeekableBufferedReader(f1 :: Nil)
    res = reader.readLine(2, 3).toSeq
    assert(res.size == 3)
    2.until(5).zipWithIndex.foreach { case (v, i) =>
      assert(res(i).toInt == v)
    }
    reader.close()

    // from + size > file lines
    reader = new SeekableBufferedReader(f1 :: Nil)
    res = reader.readLine(4, 8).toSeq
    assert(res.size == 6)
    4.until(10).zipWithIndex.foreach { case (v, i) =>
      assert(res(i).toInt == v)
    }
    reader.close()

    // from > file lines
    reader = new SeekableBufferedReader(f1 :: Nil)
    res = reader.readLine(11, 1).toSeq
    assert(res.isEmpty)
    reader.close()
  }

  test("three files") {
    var reader = new SeekableBufferedReader(f1 :: f2 :: f3 :: Nil)
    var res = reader.readLine(0, 11).toSeq
    assert(res.size == 11)
    0.until(10).zipWithIndex.foreach { case (v, i) =>
      assert(res(i).toInt == v)
    }
    assert(res(10).toInt == 0)
    reader.close()

    reader = new SeekableBufferedReader(f1 :: f2 :: f3 :: Nil)
    res = reader.readLine(11, 21).toSeq
    assert(res.size == 21)
    1.until(20).zipWithIndex.foreach { case (v, i) =>
      assert(res(i).toInt == v)
    }
    assert(res(19).toInt == 0)
    assert(res(20).toInt == 1)
    reader.close()

    reader = new SeekableBufferedReader(f1 :: f2 :: f3 :: Nil)
    res = reader.readLine(5, 100).toSeq
    assert(res.size == 55)
    5.until(10).zipWithIndex.foreach { case (v, i) =>
      assert(res(i).toInt == v)
    }
    0.until(20).zipWithIndex.foreach { case (v, i) =>
      assert(res(i + 5).toInt == v)
    }
    0.until(30).zipWithIndex.foreach { case (v, i) =>
      assert(res(i + 25).toInt == v)
    }
    reader.close()

    reader = new SeekableBufferedReader(f1 :: f2 :: f3 :: Nil)
    res = reader.readLine(100, 100).toSeq
    assert(res.isEmpty)
    reader.close()
  }
}
