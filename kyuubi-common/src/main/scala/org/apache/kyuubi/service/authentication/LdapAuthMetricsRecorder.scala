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

package org.apache.kyuubi.service.authentication

/**
 * Callback hook so the LDAP auth path (in `kyuubi-common`) can publish authentication
 * events to a metrics system (in `kyuubi-server`, via `kyuubi-metrics`) without
 * `kyuubi-common` taking a build-time dependency on `kyuubi-metrics`.
 *
 * Why this is a trait rather than a direct call to `MetricsSystem.incCount`: cumulative
 * LDAP auth counters must surface in Prometheus as `# TYPE counter`, not `# TYPE gauge`,
 * so that `rate()` and `increase()` correctly handle process restarts. The earlier design
 * exposed `AtomicLong` values as `Gauge`s, which produced misleading negative rates on
 * Kyuubi restarts. With this recorder, `kyuubi-server` plugs in an implementation that
 * delegates to `MetricsSystem.incCount`, which goes through Dropwizard's `Counter` type.
 *
 * Implementations must be thread-safe; `recordXxx` is invoked from request threads on
 * every authentication call.
 *
 * The default registered implementation is [[LdapAuthMetricsRecorder.NoOp]]; the real one
 * is installed by `KyuubiServer.start` after `MetricsSystem` has started.
 */
// Visibility is `private[kyuubi]` rather than `private[authentication]` because the
// production implementation lives in `kyuubi-server` (KyuubiServer.installLdapAuthMetricsRecorder)
// where it can delegate to `MetricsSystem.incCount`. The auth path that invokes it lives
// here in `kyuubi-common`.
private[kyuubi] trait LdapAuthMetricsRecorder {

  /** Successful authentication (cache hit OR cache-miss success). */
  def recordSuccess(): Unit

  /**
   * Failed authentication. `reason` is one of the constants in
   * [[org.apache.kyuubi.service.authentication.ldap.LdapAuthFailureClassifier]] so it
   * becomes a Prometheus metric-name suffix. Unknown reasons map to `infrastructure` at
   * the caller.
   */
  def recordFailure(reason: String): Unit

  /** Cache short-circuited an LDAP round-trip for this request. */
  def recordCacheHit(): Unit

  /**
   * This request observed an empty cache entry when it arrived. Per-request, not
   * per-LDAP-roundtrip: under request coalescing, multiple concurrent identical requests
   * each see an empty entry and each fire recordCacheMiss, even though Guava executes
   * only one underlying LDAP authentication. Use cache.miss for request-rate sizing and
   * the success / failure counters for actual auth volume.
   */
  def recordCacheMiss(): Unit
}

private[kyuubi] object LdapAuthMetricsRecorder {

  /**
   * The recorder installed by default. All `recordXxx` calls are no-ops, so the LDAP auth
   * path is metric-agnostic and remains usable in `kyuubi-common`-only contexts (tests,
   * `kyuubi-ctl`, etc.) where `MetricsSystem` is not present.
   */
  object NoOp extends LdapAuthMetricsRecorder {
    override def recordSuccess(): Unit = ()
    override def recordFailure(reason: String): Unit = ()
    override def recordCacheHit(): Unit = ()
    override def recordCacheMiss(): Unit = ()
  }
}
