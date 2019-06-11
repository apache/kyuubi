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

package yaooqinn.kyuubi.metrics

import org.apache.spark.{KyuubiConf, KyuubiSparkUtil, SparkConf, SparkFunSuite}

class MetricsSystemSuite extends SparkFunSuite {

  test(" reporter type CONSOLE") {
    val conf = new SparkConf()
    KyuubiSparkUtil.setupCommonConfig(conf)
    val m = MetricsSystem.init(conf)
    m.foreach(_.registerGauge("test1", 1, 0))
    m.foreach(_.registerGauge("test2", null, 0))
    MetricsSystem.close()
  }

  test(" reporter type JMX") {
    val conf = new SparkConf()
    KyuubiSparkUtil.setupCommonConfig(conf)
    conf.set(KyuubiConf.METRICS_REPORTER.key, "JMX")
    val m = MetricsSystem.init(conf)
    m.foreach(_.registerGauge("test1", 1, 0))
    m.foreach(_.registerGauge("test2", null, 0))
    MetricsSystem.close()
  }

  test(" reporter type JSON") {
    val conf = new SparkConf()
    KyuubiSparkUtil.setupCommonConfig(conf)
    conf.set(KyuubiConf.METRICS_REPORTER.key, "JSON")
    val m = MetricsSystem.init(conf)
    m.foreach(_.registerGauge("test1", 1, 0))
    m.foreach(_.registerGauge("test2", null, 0))
    MetricsSystem.close()
  }

  test(" reporter type OTHER") {
    val conf = new SparkConf()
    KyuubiSparkUtil.setupCommonConfig(conf)
    conf.set(KyuubiConf.METRICS_REPORTER.key, "OTHER")
    val m = MetricsSystem.init(conf)
    m.foreach(_.registerGauge("test1", 1, 0))
    m.foreach(_.registerGauge("test2", null, 0))
    MetricsSystem.close()
  }

}
