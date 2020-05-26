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

import java.io.{BufferedWriter, Closeable, IOException, OutputStreamWriter}
import java.util.{Timer, TimerTask}
import java.util.concurrent.TimeUnit

import scala.util.Try
import scala.util.control.NonFatal

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.json.MetricsModule
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.kyuubi.Logging
import org.apache.spark.{KyuubiSparkUtil, SparkConf}
import org.apache.spark.KyuubiConf._

private[metrics] class JsonFileReporter(conf: SparkConf, registry: MetricRegistry)
  extends Closeable with Logging {

  private val jsonMapper = new ObjectMapper().registerModule(
    new MetricsModule(TimeUnit.MILLISECONDS, TimeUnit.MILLISECONDS, false))
  private val timer = new Timer(true)
  private val interval = KyuubiSparkUtil.timeStringAsMs(conf.get(METRICS_REPORT_INTERVAL))
  private val path = conf.get(METRICS_REPORT_LOCATION)
  private val hadoopConf = KyuubiSparkUtil.newConfiguration(conf)

  def start(): Unit = {
    timer.schedule(new TimerTask {
      var bw: BufferedWriter = _
      override def run(): Unit = try {
        val json = jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(registry)
        val tmpPath = new Path(path + ".tmp")
        val tmpPathUri = tmpPath.toUri
        val fs = if (tmpPathUri.getScheme == null && tmpPathUri.getAuthority == null) {
          FileSystem.getLocal(hadoopConf)
        } else {
          FileSystem.get(tmpPathUri, hadoopConf)
        }
        fs.delete(tmpPath, true)
        bw = new BufferedWriter(new OutputStreamWriter(fs.create(tmpPath, true)))
        bw.write(json)
        bw.close()
        fs.setPermission(tmpPath, FsPermission.createImmutable(Integer.parseInt("644", 8).toShort))
        val finalPath = new Path(path)
        fs.rename(tmpPath, finalPath)
        fs.setPermission(finalPath,
          FsPermission.createImmutable(Integer.parseInt("644", 8).toShort))
      } catch {
        case NonFatal(e) => error("Error writing metrics to json file" + path, e)
      } finally {
        if (bw != null) {
          Try(bw.close())
        }
      }
    }, 0, interval)
  }

  override def close(): Unit = {
    timer.cancel()
  }
}
