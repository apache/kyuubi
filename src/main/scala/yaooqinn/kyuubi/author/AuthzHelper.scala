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

package yaooqinn.kyuubi.author

import org.apache.spark.KyuubiConf._
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

import yaooqinn.kyuubi.Logging
import yaooqinn.kyuubi.utils.ReflectUtils

private[kyuubi] class AuthzHelper(conf: SparkConf) extends Logging {

  val rule: Seq[Rule[LogicalPlan]] = {
    try {
      val authzMethod = conf.get(AUTHORIZATION_METHOD.key)
      val maybeRule = ReflectUtils.reflectModule(authzMethod, silent = true)
      maybeRule match {
        case Some(authz) => Seq(authz.asInstanceOf[Rule[LogicalPlan]])
        case _ => Nil
      }
    } catch {
      case e: ClassCastException =>
        error(e.getMessage, e)
        Nil
      case _: NoSuchElementException =>
        error(s"${AUTHORIZATION_METHOD.key} is not configured")
        Nil
    }
  }
}

private[kyuubi] object AuthzHelper extends Logging {

  private[this] var instance: Option[AuthzHelper] = None

  def get: Option[AuthzHelper] = instance

  def init(conf: SparkConf): Unit = {
    if (conf.get(AUTHORIZATION_ENABLE.key).toBoolean) {
      instance = Some(new AuthzHelper(conf))
    }
  }
}
