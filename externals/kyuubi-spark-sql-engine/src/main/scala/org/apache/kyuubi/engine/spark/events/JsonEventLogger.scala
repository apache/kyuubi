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

package org.apache.kyuubi.engine.spark.events

import java.io.{IOException, PrintWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.nio.file.attribute.{PosixFilePermission, PosixFilePermissions}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.spark.events.JsonEventLogger.{JSON_LOG_DIR_PERM, JSON_LOG_FILE_PERM}
import org.apache.kyuubi.service.AbstractService

/**
 * This event logger logs Kyuubi engine events in JSON file format.
 * The hierarchical directory structure is {ENGINE_EVENT_JSON_LOG_PATH}/{eventType}/{logName}.json
 * The {eventType} is based on core concepts of the Kyuubi systems, e.g. engine/session/statement
 * @param logName the engine id formed of appId + attemptId(if any)
 */
class JsonEventLogger(logName: String)
  extends AbstractService("JsonEventLogger") with EventLogger with Logging {

  private var logRoot: Path = _
  private val writers = new scala.collection.mutable.HashMap[String, PrintWriter]()

  private def getOrUpdate(event: KyuubiEvent): PrintWriter = synchronized {
    writers.getOrElseUpdate(event.eventType, {
      val eventDir = Files.createDirectories(Paths.get(logRoot.toString, event.eventType))
      Files.setPosixFilePermissions(eventDir, JSON_LOG_DIR_PERM)
      val eventPath = Files.createFile(Paths.get(eventDir.toString, logName +  ".json"))
      // TODO: make it support Hadoop compatible filesystems
      val newWriter = new PrintWriter(Files.newBufferedWriter(eventPath, StandardCharsets.UTF_8))
      Files.setPosixFilePermissions(eventPath, JSON_LOG_FILE_PERM)
      newWriter
    })
  }

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    logRoot = Paths.get(conf.get(KyuubiConf.ENGINE_EVENT_JSON_LOG_PATH)).toAbsolutePath
    Files.setPosixFilePermissions(logRoot, PosixFilePermissions.fromString("rwxrwxr--"))
    super.initialize(conf)
  }

  override def stop(): Unit = synchronized {
    writers.foreach { case (name, writer) =>
      try {
        writer.close()
      } catch {
        case e: IOException => error(s"File to close $name's event writer", e)
      }
    }
    super.stop()
  }

  override def logEvent(kyuubiEvent: KyuubiEvent): Unit = kyuubiEvent match {
    case e: EngineEvent =>
      val writer = getOrUpdate(e)
      // scalastyle:off println
      writer.println(e.toJson)
      // scalastyle:on println
      writer.flush()
    case statement: StatementEvent =>
      val writer = getOrUpdate(statement)
      // scalastyle:off println
      writer.println(statement.toJson)
      // scalastyle:on println
      writer.flush()
    case _ => // TODO: add extra events handling here
  }
}

object JsonEventLogger {
  val JSON_LOG_DIR_PERM: java.util.Set[PosixFilePermission] =
    PosixFilePermissions.fromString("rwxrwxr--")
  val JSON_LOG_FILE_PERM: java.util.Set[PosixFilePermission] =
    PosixFilePermissions.fromString("rwxr--r--")
}
