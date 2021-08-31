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

package org.apache.kyuubi.security

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf

trait HadoopDelegationTokenProvider extends Logging {

  /**
   * Name of the service to provide delegation tokens. This name should be unique. Kyuubi will
   * internally use this name to differentiate delegation token providers.
   */
  def serviceName: String

  /**
   * Returns true if delegation tokens are required for this service. By default, it is based on
   * whether Hadoop security is enabled.
   */
  def delegationTokensRequired(hadoopConf: Configuration): Boolean

  /**
   * Obtain delegation tokens for this service.
   *
   * @param hadoopConf Configuration of current Hadoop Compatible system.
   * @param owner      DelegationToken owner.
   * @param creds      Credentials to add tokens and security keys to.
   */
  def obtainDelegationTokens(
      hadoopConf: Configuration,
      kyuubiConf: KyuubiConf,
      owner: String,
      creds: Credentials): Unit

}
