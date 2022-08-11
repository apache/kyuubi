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

package org.apache.kyuubi.util

import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.config.KyuubiConf

object Validator {

  def validateConf(kyuubiConf: KyuubiConf): Unit = {
    validateSparkK8sConf(kyuubiConf)
  }

  private def validateSparkK8sConf(kyuubiConf: KyuubiConf): Unit = {
    val prefix = kyuubiConf.getOption(KUBERNETES_EXECUTOR_POD_NAME_PREFIX).orNull
    if (prefix != null && !isValidExecutorPodNamePrefix(prefix)) {
      throw new KyuubiException(s"'$prefix' " +
        s"in spark.kubernetes.executor.podNamePrefix is invalid." +
        s" must conform https://kubernetes.io/docs/concepts/overview/working-with-objects" +
        "/names/#dns-subdomain-names and the value length <= 237")
    }
  }

  private val dns1123LabelFmt = "[a-z0-9]([-a-z0-9]*[a-z0-9])?"

  private val podConfValidator = (s"^$dns1123LabelFmt(\\.$dns1123LabelFmt)*$$").r.pattern

  val KUBERNETES_DNS_SUBDOMAIN_NAME_MAX_LENGTH = 253

  private def isValidExecutorPodNamePrefix(prefix: String): Boolean = {
    // 6 is length of '-exec-'
    val reservedLen = Int.MaxValue.toString.length + 6
    val validLength = prefix.length + reservedLen <= KUBERNETES_DNS_SUBDOMAIN_NAME_MAX_LENGTH
    validLength && podConfValidator.matcher(prefix).matches()
  }

  val KUBERNETES_EXECUTOR_POD_NAME_PREFIX = "spark.kubernetes.executor.podNamePrefix"
}
