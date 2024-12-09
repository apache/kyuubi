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

package org.apache.spark.kyuubi.jvm.quake

import java.io.File
import java.lang.management.ManagementFactory
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import SparkJVMQuake._
import com.sun.management.HotSpotDiagnosticMXBean
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.kyuubi.jvm.quake.SparkJVMQuakeConf._
import org.apache.spark.util.ThreadUtils

class SparkJVMQuake(conf: SparkConf, heapDumpEnabled: Boolean) extends Logging {
  private val appId = conf.get("spark.app.id", "app-id")
  private val killThreshold = TimeUnit.SECONDS.toNanos(conf.get(JVM_QUAKE_KILL_THRESHOLD))
  private val exitCode = conf.get(JVM_QUAKE_EXIT_CODE)
  private val checkInterval = TimeUnit.SECONDS.toMillis(conf.get(JVM_QUAKE_CHECK_INTERVAL))
  private val runTimeWeight = conf.get(JVM_QUAKE_RUN_TIME_WEIGHT)
  private[quake] val heapPath = s"${conf.get(JVM_QUAKE_HEAP_DUMP_PATH)}/$appId"

  private[quake] var (lastExitTime, lastGCTime) = getLastGCInfo
  private[quake] var bucket: Long = 0L
  private var heapDumping: Boolean = false

  private[quake] val heapDumpFileName = s"${getPid()}.hprof"

  private val scheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("kyuubi-jvm-quake")

  private[quake] def run(): Unit = {
    val (currentExitTime, currentGcTime) = getLastGCInfo
    if (currentExitTime != lastExitTime && !heapDumping) {
      val gcTime = currentGcTime - lastGCTime
      val runTime = currentExitTime - lastExitTime - gcTime

      bucket = Math.max(0, bucket + gcTime - BigDecimal(runTime * runTimeWeight).toLong)

      if (bucket > killThreshold) {
        logError(s"JVM GC has reached the threshold!!!" +
          s" (bucket: ${bucket / 1000000000}s, killThreshold: ${killThreshold / 1000000000}s)")
        if (heapDumpEnabled) {
          heapDumping = true
          saveHeap()
        }
        System.exit(exitCode)
      }

      lastExitTime = currentExitTime
      lastGCTime = currentGcTime
    }
  }

  def start(): Unit = {
    scheduler.scheduleAtFixedRate(
      () => SparkJVMQuake.this.run(),
      0,
      checkInterval,
      TimeUnit.MILLISECONDS)
  }

  def stop(): Unit = {
    scheduler.shutdown()
  }

  private[quake] def saveHeap(): Unit = {
    try {
      val saveDir = new File(heapPath)
      if (!saveDir.exists()) {
        saveDir.mkdirs()
      }
      val heapDumpFile = new File(saveDir, heapDumpFileName)
      if (heapDumpFile.exists()) {
        logInfo(s"Heap exits $heapDumpFile")
      } else {
        logInfo(s"Starting heap dump at $heapDumpFile")
        val server = ManagementFactory.getPlatformMBeanServer
        val mxBean = ManagementFactory.newPlatformMXBeanProxy(
          server,
          "com.sun.management:type=HotSpotDiagnostic",
          classOf[HotSpotDiagnosticMXBean])
        mxBean.dumpHeap(heapDumpFile.getAbsolutePath, false)
      }
    } catch {
      case e: Exception =>
        logError(s"Failed to dump process(${getPid()}) heap to $heapPath", e)
    }
  }
}

object SparkJVMQuake {

  private[this] var monitor: Option[SparkJVMQuake] = None

  def start(sparkConf: SparkConf, heapDumpEnabled: Boolean): Unit = {
    monitor = Some(new SparkJVMQuake(sparkConf, heapDumpEnabled))
    monitor.foreach(_.start())
  }

  def stop(): Unit = {
    monitor.foreach(_.stop())
  }

  def getLastGCInfo: (Long, Long) = {
    val mxBeans = ManagementFactory.getGarbageCollectorMXBeans.asScala
    val lastGCInfos = mxBeans
      .filter(_.isInstanceOf[com.sun.management.GarbageCollectorMXBean])
      .map(_.asInstanceOf[com.sun.management.GarbageCollectorMXBean])
      .flatMap(bean => Option(bean.getLastGcInfo))
    if (lastGCInfos.isEmpty) {
      (0L, 0L)
    } else {
      (lastGCInfos.map(_.getEndTime).max * 1000000, mxBeans.map(_.getCollectionTime).sum * 1000000)
    }
  }

  def getPid(): Long = {
    ManagementFactory.getRuntimeMXBean.getName.split("@")(0).toLong
  }
}
