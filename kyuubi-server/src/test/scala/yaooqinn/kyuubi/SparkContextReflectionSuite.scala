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

package yaooqinn.kyuubi

import java.lang.reflect.InvocationTargetException

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}

import yaooqinn.kyuubi.utils.ReflectUtils

class SparkContextReflectionSuite extends SparkFunSuite {

  test("SparkContext initialization with default constructor") {
    val conf = new SparkConf(loadDefaults = true).setMaster("local").setAppName("sc_init")
    val sc = ReflectUtils
      .newInstance(classOf[SparkContext].getName, Seq(classOf[SparkConf]), Seq(conf))
      .asInstanceOf[SparkContext]
    assert(sc.isInstanceOf[SparkContext])
    sc.stop()
  }

  test("SparkContext initialization with this()") {
    intercept[InvocationTargetException](ReflectUtils
      .instantiateClassByName(classOf[SparkContext].getName)
      .asInstanceOf[SparkContext])
  }

  test("SparkContext initialization with app name & master & conf") {
    val conf = new SparkConf(loadDefaults = true)
    val sc = ReflectUtils
      .newInstance(
        classOf[SparkContext].getName,
        Seq(classOf[String], classOf[String], classOf[SparkConf]),
        Seq("local", "sc_init", conf))
      .asInstanceOf[SparkContext]
    assert(sc.isInstanceOf[SparkContext])
    sc.stop()
  }

  test("Initializing 2 SparkContext with Reflection") {
    val conf1 = new SparkConf(loadDefaults = true)
      .setMaster("local").setAppName("sc1").set("spark.driver.allowMultipleContexts", "true")
    val sc1 = ReflectUtils
      .newInstance(classOf[SparkContext].getName, Seq(classOf[SparkConf]), Seq(conf1))
      .asInstanceOf[SparkContext]
    val conf2 = new SparkConf(loadDefaults = true)
      .setMaster("local").setAppName("sc2").set("spark.driver.allowMultipleContexts", "true")
    val sc2 = ReflectUtils
      .newInstance(classOf[SparkContext].getName, Seq(classOf[SparkConf]), Seq(conf2))
      .asInstanceOf[SparkContext]

    assert(sc1 !== sc2)
    sc1.stop()
    sc2.stop()
  }
}
