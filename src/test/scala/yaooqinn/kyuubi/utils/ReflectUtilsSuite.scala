/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package yaooqinn.kyuubi.utils

import org.apache.spark.SparkFunSuite

/**
 *
 */
class ReflectUtilsSuite extends SparkFunSuite {

  test("reflect utils init class without param") {
    try {
      val testClassInstance =
        ReflectUtils.instantiateClassByName(classOf[TestClass0].getName)
      assert(testClassInstance.asInstanceOf[TestClass0].isInstanceOf[TestClass0])
    } catch {
      case e: Exception => throw e
    }
  }

  test("reflect utils init class with one param") {
    try {
      val testClassInstance =
        ReflectUtils.instantiateClass(
          classOf[TestClass1].getName, Seq(classOf[TestClass0]), Seq(new TestClass0))
      assert(testClassInstance.asInstanceOf[TestClass1].isInstanceOf[TestClass1])
    } catch {
      case e: Exception => throw e
    }
  }

  test("reflect utils init class with multiple params") {
    try {
      val testClassInstance =
        ReflectUtils.instantiateClass(
          classOf[TestClass2].getName,
          Seq(classOf[String], classOf[TestClass0]),
          Seq("arg1", new TestClass0))
      assert(testClassInstance.asInstanceOf[TestClass2].isInstanceOf[TestClass2])
    } catch {
      case e: Exception => throw e
    }
  }

  test("reflect utils fail init class not exist ") {
    intercept[ClassNotFoundException](
      ReflectUtils.instantiateClassByName("yaooqinn.kyuubi.NonExistTestClass"))
  }

  test("find class by name") {
    intercept[ClassNotFoundException](
      ReflectUtils.findClass("yaooqinn.kyuubi.NonExistTestClass"))

    assert(ReflectUtils.findClass(classOf[TestClass0].getName) == classOf[TestClass0])
  }

  test("invoke static method") {
      val clz = ReflectUtils.findClass(className = classOf[TestClass0].getName)
      assert(ReflectUtils.invokeStaticMethod(clz, "staticTest").asInstanceOf[Int] === 1)
  }

}


class TestClass0()
class TestClass1(arg1: TestClass0)
class TestClass2(arg1: String, arg2: TestClass0)

object TestClass0 {
  def staticTest(): Int = 1
}

