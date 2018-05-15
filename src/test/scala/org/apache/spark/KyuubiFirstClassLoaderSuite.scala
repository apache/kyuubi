/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.net.URLClassLoader

class KyuubiFirstClassLoaderSuite extends SparkFunSuite {

  val urls1 = List(TestUtils.createJarWithClasses(
    classNames = Seq("FakeClass1", "FakeClass2", "FakeClass3"),
    toStringValue = "2")).toArray
  val urls2 = List(TestUtils.createJarWithClasses(
    classNames = Seq("FakeClass1"),
    classNamesWithBase = Seq(("FakeClass2", "FakeClass3")), // FakeClass3 is in parent
    toStringValue = "1",
    classpathUrls = urls1)).toArray

  test("kyuubi class loader first") {
    val parentLoader = new URLClassLoader(urls1, null)
    val classLoader = new KyuubiFirstClassLoader(urls2, parentLoader)
    val fakeClass = classLoader.loadClass("FakeClass2").newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "1")
    val fakeClass2 = classLoader.loadClass("FakeClass2").newInstance()
    assert(fakeClass.getClass === fakeClass2.getClass)
    classLoader.close()
    parentLoader.close()
  }

  test("kyuubi class loader first can fall back") {
    val parentLoader = new URLClassLoader(urls1, null)
    val classLoader = new KyuubiFirstClassLoader(urls2, parentLoader)
    val fakeClass = classLoader.loadClass("FakeClass3").newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "2")
    classLoader.close()
    parentLoader.close()
  }

  test("kyuubi class loader first can fail") {
    val parentLoader = new URLClassLoader(urls1, null)
    val classLoader = new KyuubiFirstClassLoader(urls2, parentLoader)
    intercept[java.lang.ClassNotFoundException] {
      classLoader.loadClass("FakeClassDoesNotExist").newInstance()
    }
    classLoader.close()
    parentLoader.close()
  }

}
