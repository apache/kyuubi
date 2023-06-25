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

package org.apache.kyuubi.spark.connector.hive

import java.util.Locale

sealed trait ExternalCatalogSharePolicy {

  /**
   * String name of the share policy
   */
  def name: String
}

/**
 * Indicate to an external catalog is shared globally with the HiveCatalogs
 * with the same catalogName.
 */
case object OneForAllPolicy extends ExternalCatalogSharePolicy { val name = "ONE_FOR_ALL" }

/**
 * Indicate to an external catalog is used by only one HiveCatalog.
 */
case object OneForOnePolicy extends ExternalCatalogSharePolicy { val name = "ONE_FOR_ONE" }

object ExternalCatalogSharePolicy {

  /**
   * Returns the share policy from the given string.
   */
  def fromString(policy: String): ExternalCatalogSharePolicy =
    policy.toUpperCase(Locale.ROOT) match {
      case OneForAllPolicy.name => OneForAllPolicy
      case OneForOnePolicy.name => OneForOnePolicy
      case _ => throw new IllegalArgumentException(s"Unknown share policy: $policy. Accepted " +
          "policies are 'ONE_FOR_ONE', 'ONE_FOR_ALL'.")
    }
}
