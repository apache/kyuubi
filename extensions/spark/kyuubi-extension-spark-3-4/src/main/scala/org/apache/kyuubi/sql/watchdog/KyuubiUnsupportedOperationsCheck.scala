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

package org.apache.kyuubi.sql.watchdog

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ScriptTransformation}

import org.apache.kyuubi.sql.{KyuubiSQLConf, KyuubiSQLExtensionException}

object KyuubiUnsupportedOperationsCheck extends (LogicalPlan => Unit) with SQLConfHelper {
  override def apply(plan: LogicalPlan): Unit =
    conf.getConf(KyuubiSQLConf.SCRIPT_TRANSFORMATION_ENABLED) match {
      case false => plan foreach {
          case _: ScriptTransformation =>
            throw new KyuubiSQLExtensionException("Script transformation is not allowed")
          case _ =>
        }
      case true =>
    }
}
