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

package org.apache.kyuubi.service.authentication.ldap

import org.apache.kyuubi.config.KyuubiConf

/**
 * Factory for the filter.
 */
trait FilterFactory {

  /**
   * Returns an instance of the corresponding filter.
   *
   * @param conf Hive properties used to configure the filter.
   * @return Some(filter) or None if this filter doesn't support provided set of properties
   */
  def getInstance(conf: KyuubiConf): Option[Filter]
}
