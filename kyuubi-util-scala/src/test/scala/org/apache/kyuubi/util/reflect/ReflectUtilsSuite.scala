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
package org.apache.kyuubi.util.reflect

// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.util.AssertionUtils._
import org.apache.kyuubi.util.reflect.ReflectUtils._
class ReflectUtilsSuite extends AnyFunSuite {
  // scalastyle:on

  private val obj1 = new ClassA
  private val obj2 = new ClassB

  test("check class loadable") {
    assert(isClassLoadable(getClass.getName))
    assert(!isClassLoadable("org.apache.kyuubi.NonExistClass"))
  }

  test("check invokeAs on base class") {
    assertResult("method1")(invokeAs[String](obj1, "method1"))
    assertResult("method2")(invokeAs[String](obj1, "method2"))
  }
  test("check invokeAs on sub class") {
    assertResult("method1")(invokeAs[String](obj2, "method1"))
    assertResult("method2")(invokeAs[String]((classOf[ClassA], obj2), "method2"))
    assertResult("method3")(invokeAs[String](obj2, "method3"))
    assertResult("method4")(invokeAs[String](obj2, "method4"))
  }

  test("check invokeAs on object and static class") {
    assertResult("method5")(invokeAs[String](ObjectA, "method5"))
    assertResult("method6")(invokeAs[String](ObjectA, "method6"))
    assertResult("method5")(invokeAs[String]("org.apache.kyuubi.util.reflect.ObjectA", "method5"))
  }

  test("check getField on base class") {
    assertResult("field0")(getField[String](obj1, "field0"))
    assertResult("field1")(getField[String](obj1, "field1"))
    assertResult("field2")(getField[String](obj1, "field2"))
  }

  test("check getField on subclass") {
    assertResult("field0")(getField[String]((classOf[ClassA], obj2), "field0"))
    assertResult("field1")(getField[String]((classOf[ClassA], obj2), "field1"))
    assertResult("field2")(getField[String]((classOf[ClassA], obj2), "field2"))
    assertResult("field3")(getField[String](obj2, "field3"))
    assertResult("field4")(getField[String](obj2, "field4"))
  }

  test("check getField on object and static class") {
    assertResult("field5")(getField[String](ObjectA, "field5"))
    assertResult("field6")(getField[String](ObjectA, "field6"))
  }

  test("test invokeAs method not found exception") {
    interceptEquals[RuntimeException] {
      invokeAs[String](
        ObjectA,
        "methodNotExists",
        (classOf[String], "arg1"),
        (classOf[String], "arg2"))
    }("Method methodNotExists(java.lang.String, java.lang.String) not found " +
      "in class org.apache.kyuubi.util.reflect.ObjectA$ " +
      "[equals(java.lang.Object), field5(), field6(), getClass(), hashCode(), method5(), " +
      "method6(), notify(), notifyAll(), toString(), wait(), wait(long), wait(long, int)]")
  }
}

class ClassA(val field0: String = "field0") {
  val field1 = "field1"
  private val field2 = "field2"

  def method1(): String = "method1"
  private def method2(): String = "method2"
}

class ClassB extends ClassA {
  val field3 = "field3"
  private val field4 = "field4"

  def method3(): String = "method3"
  private def method4(): String = "method4"
}

object ObjectA {
  val field5 = "field5"
  private val field6 = "field6"

  def method5(): String = "method5"
  private def method6(): String = "method6"
}
