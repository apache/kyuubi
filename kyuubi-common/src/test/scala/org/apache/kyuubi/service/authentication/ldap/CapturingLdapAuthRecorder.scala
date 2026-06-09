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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.kyuubi.service.authentication.LdapAuthMetricsRecorder

/**
 * Test-only [[LdapAuthMetricsRecorder]] that tracks every call. Shared across suites that
 * want to assert which recorder methods were invoked (e.g. classifier-dispatch tests that
 * verify a bad-password authenticate lands as `invalid_credentials`).
 *
 * Lives in the `ldap` test package so any test suite in this package can use it without
 * each suite redefining the same helper.
 */
final private[ldap] class CapturingLdapAuthRecorder extends LdapAuthMetricsRecorder {
  val successes = new AtomicInteger(0)
  val cacheHits = new AtomicInteger(0)
  val cacheMisses = new AtomicInteger(0)
  val invalidCreds = new AtomicInteger(0)
  val invalidInput = new AtomicInteger(0)
  val infrastructure = new AtomicInteger(0)

  override def recordSuccess(): Unit = { successes.incrementAndGet(); () }
  override def recordCacheHit(): Unit = { cacheHits.incrementAndGet(); () }
  override def recordCacheMiss(): Unit = { cacheMisses.incrementAndGet(); () }
  override def recordFailure(reason: String): Unit = {
    reason match {
      case LdapAuthFailureClassifier.INVALID_CREDENTIALS => invalidCreds.incrementAndGet()
      case LdapAuthFailureClassifier.INVALID_INPUT => invalidInput.incrementAndGet()
      case _ => infrastructure.incrementAndGet()
    }
    ()
  }
}
