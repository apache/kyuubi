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
package org.apache.kyuubi.events.handler

import java.util.ServiceLoader

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.events.KyuubiEvent

object EventHandlerLoader extends Logging {

  def loadCustom(kyuubiConf: KyuubiConf): Seq[EventHandler[KyuubiEvent]] = {
    val providers = ArrayBuffer[CustomEventHandlerProvider]()
    ServiceLoader.load(
      classOf[CustomEventHandlerProvider],
      Utils.getContextOrKyuubiClassLoader)
      .iterator()
      .asScala
      .foreach(provider => providers += provider)

    providers.map { provider =>
      Try {
        provider.create(kyuubiConf)
      } match {
        case Success(value) =>
          value
        case Failure(exception) =>
          warn(
            s"Failed to create an EventHandler by the ${provider.getClass.getName}," +
              s" it will be ignored.",
            exception)
          null
      }
    }.filter(_ != null)
  }
}
