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

import java.io.{ByteArrayOutputStream, DataOutputStream}

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.KyuubiConf._
import org.apache.spark.SparkContext

import yaooqinn.kyuubi.utils.ReflectUtils._

/**
 * Tool for methods used for Kyuubi to talking to Spark Executors
 */
object KyuubiSparkExecutorUtils {

  /**
   * Populate the tokens contained in the current KyuubiSession's ugi to the all the alive
   * executors associated with its own SparkContext.
   *
   * @param sc The SparkContext with its runtime environment which contains all the executors,
   *           associated with the current KyuubiSession and UserGroupInformation.
   * @param user the UserGroupInformation associated with the current KyuubiSession
   */
  def populateTokens(sc: SparkContext, user: UserGroupInformation): Unit = {
    val schedulerBackend = sc.schedulerBackend
    schedulerBackend match {
      case backend: CoarseGrainedSchedulerBackend =>
        try {
          val byteStream = new ByteArrayOutputStream
          val dataStream = new DataOutputStream(byteStream)
          user.getCredentials.writeTokenStorageToStream(dataStream)
          val tokens = byteStream.toByteArray
          val executorField =
            classOf[CoarseGrainedSchedulerBackend].getName.replace('.', '$') + "$$executorDataMap"
          val executors = backend match {
            case _: YarnClientSchedulerBackend | _: YarnClusterSchedulerBackend |
                 _: StandaloneSchedulerBackend =>
              getAncestorField(backend, 2, executorField)
            case _ => getFieldValue(backend, executorField)
          }
          val msg = newInstance(sc.conf.get(BACKEND_SESSION_TOKEN_UPDATE_CLASS),
            Seq(classOf[Array[Byte]]), Seq(tokens))
          executors.asInstanceOf[mutable.HashMap[String, ExecutorData]]
            .values.foreach(_.executorEndpoint.send(msg))
        } catch {
          case NonFatal(e) => warn(s"Failed to populate secured tokens to executors", e)
        }
      case _ =>
    }
  }
}
