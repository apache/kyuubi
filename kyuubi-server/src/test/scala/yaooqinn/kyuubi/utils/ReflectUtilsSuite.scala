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

package yaooqinn.kyuubi.utils

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

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
    val testClassInstance =
      ReflectUtils.newInstance(
        classOf[TestClass1].getName, Seq(classOf[TestClass0]), Seq(new TestClass0))
    assert(testClassInstance.asInstanceOf[TestClass1].isInstanceOf[TestClass1])
    val e = intercept[IllegalArgumentException](ReflectUtils.newInstance(null, null, null))
    assert(e.getMessage.contains("class name could not be null!"))
    val e2 = intercept[IllegalArgumentException](
      ReflectUtils.newInstance(
        classOf[TestClass1].getName, Seq(classOf[TestClass0]), Seq(new TestClass0, "dummy")))
    assert(e2.getMessage.contains("each params should have a class type!"))
  }

  test("reflect utils init class with multiple params") {
    try {
      val testClassInstance =
        ReflectUtils.newInstance(
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
    assert(ReflectUtils.findClass("yaooqinn.kyuubi.NonExistTestClass", silence = true) === null)
  }

  test("invoke static method") {
      val clz = ReflectUtils.findClass(className = classOf[TestClass0].getName)
      assert(ReflectUtils.invokeStaticMethod(clz, "staticTest").asInstanceOf[Int] === 1)
    intercept[NoSuchMethodException](ReflectUtils.invokeStaticMethod(clz, "dummy"))
  }

  test("invoke method") {
    val t = new TestClass3
    assert(ReflectUtils.invokeMethod(t, "test") === 1)
    intercept[NoSuchMethodException](ReflectUtils.invokeMethod(t, "dummy"))
  }

  test("testSuperField") {
    val t = new TestClass3
    ReflectUtils.setSuperField(t, "name", "child")
    assert(ReflectUtils.getSuperField(t, "name").asInstanceOf[String] === "child")
    ReflectUtils.setAncestorField(t, 1, "name", "child2")
    assert(ReflectUtils.getAncestorField(t, 1, "name").asInstanceOf[String] === "child2")
  }

  test("testGetFieldValue") {
    val o = new TestTrait
    assert(ReflectUtils.getFieldValue(o, "name") === "super")
    assert(ReflectUtils.getFieldValue(TestClass0, "testObj") === "1")
    assert(ReflectUtils.getFieldValue(TestClass0, "testInt") === 1)
    intercept[NoSuchFieldException](ReflectUtils.getFieldValue(o, "age"))
  }

  test("testSetFieldValue") {
    val o = new TestTrait
    ReflectUtils.setFieldValue(o, "name", "test")
    ReflectUtils.setFieldValue(o, "num", 2)

    assert(ReflectUtils.getFieldValue(o, "name") === "test")
    assert(ReflectUtils.getFieldValue(o, "num") === 2)

    ReflectUtils.setFieldValue(TestClass0, "testObj", "test")
    ReflectUtils.setFieldValue(TestClass0, "testInt", 2)

    assert(ReflectUtils.getFieldValue(TestClass0, "testObj") === "test")
    assert(ReflectUtils.getFieldValue(TestClass0, "testInt") === 2)
    assert(TestClass0.testObj === "test")
    assert(TestClass0.testInt === 2)

    intercept[NoSuchFieldException](ReflectUtils.setFieldValue(o, "nume", "test"))
    intercept[IllegalArgumentException](ReflectUtils.setFieldValue(o, "num", "test"))
  }

  test("test reflect module") {
    val rule1 =
      ReflectUtils.reflectModule(className = "yaooqinn.kyuubi.TestRule", silent = true)
    assert(rule1.get.isInstanceOf[Rule[_]])
    val rule2 =
      ReflectUtils.reflectModule(className = "yaooqinn.kyuubi.TestRule2", silent = true)
    assert(rule2.isEmpty)

    val e = intercept[ScalaReflectionException](
      ReflectUtils.reflectModule(className = "yaooqinn.kyuubi.TestRule2", silent = false))
    assert(e.getMessage.contains("not found"))
  }

  test("reflect static method scala") {
    val m1 = ReflectUtils.reflectStaticMethodScala("yaooqinn.kyuubi.utils.TestClass0", "staticTest")
    assert(m1() === 1)
    val m2 =
      ReflectUtils.reflectStaticMethodScala("yaooqinn.kyuubi.utils.TestClass0", "staticTest2")
    assert(m2(1) === 2)
    intercept[IllegalArgumentException](m2("1"))
    intercept[ScalaReflectionException](ReflectUtils.
      reflectStaticMethodScala("yaooqinn.kyuubi.utils.TestClass0", "staticTest3"))
  }
}

class TestTrait {
  private val name: String = "super"
  private val num: Int = 1
}

class TestClass0()
class TestClass1(arg1: TestClass0)
class TestClass2(arg1: String, arg2: TestClass0)
class TestClass3 extends TestTrait {
  def test: Long = 1L
}

object TestClass0 {
  def staticTest(): Int = 1
  def staticTest2(x: Int): Int = x + 1
  val testInt = 1
  val testObj = "1"
}
