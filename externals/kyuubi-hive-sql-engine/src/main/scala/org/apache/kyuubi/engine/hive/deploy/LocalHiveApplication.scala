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
package org.apache.kyuubi.engine.hive.deploy

import java.lang.reflect.Modifier

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.hive.HiveSQLEngine

private[deploy] class LocalHiveApplication extends HiveApplication with Logging {

  override def start(args: Array[String], conf: KyuubiConf): Unit = {
    val mainMethod = classOf[HiveSQLEngine].getMethod("main", new Array[String](0).getClass)
    if (!Modifier.isStatic(mainMethod.getModifiers)) {
      throw new IllegalStateException("The main method must be static.")
    }

    val systemProperties = conf.getAll
    systemProperties.foreach { case (k, v) =>
      sys.props(k) = v
    }

    mainMethod.invoke(null, args)
  }
}
