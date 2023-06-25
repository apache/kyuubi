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

import javax.security.sasl.AuthenticationException

import org.apache.kyuubi.config.KyuubiConf

/**
 * A factory that produces a [[Filter]] that is implemented as a chain of other filters.
 * The chain of filters are created as a result of [[ChainFilterFactory#getInstance]] method call.
 * The resulting object filters out all users that don't pass all chained filters.
 * The filters will be applied in the order they are mentioned in the factory constructor.
 */

class ChainFilterFactory(chainedFactories: FilterFactory*) extends FilterFactory {
  override def getInstance(conf: KyuubiConf): Option[Filter] = {
    val maybeFilters = chainedFactories.map(_.getInstance(conf))
    val filters = maybeFilters.flatten
    if (filters.isEmpty) None else Some(new ChainFilter(filters))
  }
}

class ChainFilter(chainedFilters: Seq[Filter]) extends Filter {
  @throws[AuthenticationException]
  override def apply(client: DirSearch, user: String): Unit = {
    chainedFilters.foreach(_.apply(client, user))
  }
}
