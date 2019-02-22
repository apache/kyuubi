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

package org.apache.spark.scheduler.cluster

import scala.collection.mutable

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{KyuubiSparkUtil, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.KyuubiConf._
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.scalatest.BeforeAndAfterEach

import yaooqinn.kyuubi.utils.ReflectUtils

class KyuubiSparkExecutorUtilsSuite
  extends SparkFunSuite with BeforeAndAfterEach {
  import KyuubiSparkUtil._

  val conf: SparkConf = new SparkConf(true)
    .setAppName(this.getClass.getSimpleName)
    .setMaster("local")
  KyuubiSparkUtil.setupCommonConfig(conf)

  var sc: SparkContext = _

  override def beforeEach(): Unit = {
    sc = new SparkContext(conf)
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    if (sc != null) {
      sc.stop()
    }
    super.afterEach()
  }

  test("populate tokens for non CoarseGrainedSchedulerBackend") {
    val taskSchedulerImpl = new TaskSchedulerImpl(sc, 4)
    val backend = new LocalSchedulerBackend(conf, taskSchedulerImpl, 1)
    ReflectUtils.setFieldValue(sc, "_schedulerBackend", backend)
    val user = UserGroupInformation.getCurrentUser
    KyuubiSparkExecutorUtils.populateTokens(sc, user)
  }

  test("populate tokens for CoarseGrainedSchedulerBackend") {
    val taskSchedulerImpl = new TaskSchedulerImpl(sc, 4)
    val backend = new CoarseGrainedSchedulerBackend(taskSchedulerImpl, sc.env.rpcEnv)
    ReflectUtils.setFieldValue(sc, "_schedulerBackend", backend)
    val user = UserGroupInformation.getCurrentUser
    KyuubiSparkExecutorUtils.populateTokens(sc, user)
  }

  test("populate tokens for YarnClientSchedulerBackend") {
    val taskSchedulerImpl = new TaskSchedulerImpl(sc, 4)
    val backend = new YarnClientSchedulerBackend(taskSchedulerImpl, sc)
    ReflectUtils.setFieldValue(sc, "_schedulerBackend", backend)
    val user = UserGroupInformation.getCurrentUser
    KyuubiSparkExecutorUtils.populateTokens(sc, user)
  }

  test("populate tokens for YarnClusterSchedulerBackend") {
    val taskSchedulerImpl = new TaskSchedulerImpl(sc, 4)
    val backend = new YarnClusterSchedulerBackend(taskSchedulerImpl, sc)
    ReflectUtils.setFieldValue(sc, "_schedulerBackend", backend)
    val user = UserGroupInformation.getCurrentUser
    KyuubiSparkExecutorUtils.populateTokens(sc, user)
  }

  test("populate tokens for StandaloneSchedulerBackend") {
    val taskSchedulerImpl = new TaskSchedulerImpl(sc, 4)
    val backend = new StandaloneSchedulerBackend(taskSchedulerImpl, sc, null)
    ReflectUtils.setFieldValue(sc, "_schedulerBackend", backend)
    val user = UserGroupInformation.getCurrentUser
    KyuubiSparkExecutorUtils.populateTokens(sc, user)
  }

  test("get executor data map") {
    val taskSchedulerImpl = new TaskSchedulerImpl(sc, 4)
    val backend = new CoarseGrainedSchedulerBackend(taskSchedulerImpl, sc.env.rpcEnv)
    val executorDataMap = ReflectUtils.getFieldValue(backend,
      "org$apache$spark$scheduler$cluster$CoarseGrainedSchedulerBackend$$executorDataMap")
    assert(executorDataMap.asInstanceOf[mutable.HashMap[String, ExecutorData]].values.isEmpty)
    sc.stop()
  }

  test("create update token class via reflection") {
    val className = conf.get(BACKEND_SESSION_TOKEN_UPDATE_CLASS)
    assert(classIsLoadable(className) ===
      (majorVersion(SPARK_VERSION) == 2 && minorVersion(SPARK_VERSION) >= 3))

    if (classIsLoadable(className)) {
      val tokens1 = Array(0.toByte)
      val tokens2 = Array(1, 2, 3, 4).map(_.toByte)
      val msg1 = ReflectUtils.newInstance(className, Seq(classOf[Array[Byte]]), Seq(tokens1))
      assert(ReflectUtils.getFieldValue(msg1, "tokens") === tokens1)
      val msg2 = ReflectUtils.newInstance(className, Seq(classOf[Array[Byte]]), Seq(tokens2))
      assert(ReflectUtils.getFieldValue(msg2, "tokens") === tokens2)
    }
  }
}
