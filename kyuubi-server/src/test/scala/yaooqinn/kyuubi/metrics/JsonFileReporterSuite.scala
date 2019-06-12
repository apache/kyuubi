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

import com.codahale.metrics.MetricRegistry
import org.apache.spark.{KyuubiConf, KyuubiSparkUtil, SparkConf, SparkFunSuite}

class JsonFileReporterSuite extends SparkFunSuite {

  test("json file report test") {
    val registry = new MetricRegistry()
    val conf = new SparkConf()
    KyuubiSparkUtil.setupCommonConfig(conf)
    conf.set(KyuubiConf.METRICS_REPORTER.key, "JSON")
    conf.set(KyuubiConf.METRICS_REPORT_INTERVAL.key, "1ms")
    val reporter = new JsonFileReporter(conf, registry)
    reporter.start()
    Thread.sleep(5000)
    reporter.close()

    conf.set(KyuubiConf.METRICS_REPORT_LOCATION.key, "hdfs://user/test/report.json")
    val reporter2 = new JsonFileReporter(conf, registry)
    reporter2.start()
    Thread.sleep(5000)
    reporter2.close()

  }

}
