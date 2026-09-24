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

package org.apache.kyuubi.plugin.spark.authz

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.LongAdder

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.slf4j.LoggerFactory

/**
 * Handling for plan nodes that fall outside the plugin's recognition machinery
 * ("paranoid mode").
 *
 * The privilege builder classifies plan nodes by pattern-matching against the command,
 * scan and function spec files. Historically a node that fell through every match was
 * silently treated as not-authz-relevant, i.e. the plugin failed open. This object
 * centralizes the policy applied when such a node is encountered:
 *
 *  - `allow`: legacy behavior, log at DEBUG only
 *  - `warn`: log at WARN once per (class name, violation kind) per JVM (default)
 *  - `deny`: throw [[AccessControlException]], failing the query closed
 *
 * configured via `spark.kyuubi.authz.unclassifiedNode.behavior`.
 *
 * The behavior is read from the application's [[org.apache.spark.SparkConf]], never from
 * the session configuration: `deny` is an authorization boundary, so the subject of the
 * authorization decision must not be able to move it. `SparkConf` is fixed when the engine
 * starts and is shared by every session in the application, so a client can reach it
 * through neither SQL `SET` nor the Spark Connect config API.
 *
 * Nodes that are genuinely not authz-relevant are declared in
 * `known_harmless_spec.json`, each with a human-reviewed reason.
 */
object ParanoidMode {

  final private val LOG = LoggerFactory.getLogger(getClass)

  final val UNCLASSIFIED_NODE_BEHAVIOR_KEY = "spark.kyuubi.authz.unclassifiedNode.behavior"

  object Behavior extends Enumeration {
    type Behavior = Value
    val ALLOW: Value = Value("allow")
    val WARN: Value = Value("warn")
    val DENY: Value = Value("deny")
  }

  /** The kinds of classification violations, used in log/error messages and dedup keys. */
  object ViolationKind extends Enumeration {
    type ViolationKind = Value

    /** A `Command` with no entry in any command spec file (fail-open layer 1). */
    val UNCLASSIFIED_COMMAND: Value = Value("unclassified command")

    /** A leaf plan node not matched by any scan spec (fail-open layer 2). */
    val UNCLASSIFIED_LEAF: Value = Value("unclassified leaf node")

    /** A node matched by a scan spec but not resolved, so no privileges were extracted. */
    val UNRESOLVED_SCAN: Value = Value("unresolved scan node")

    /**
     * A node whose class name has a command spec entry, encountered on the query path:
     * the spec exists but the dispatch never reaches it (the CALL-on-Spark-4 shape).
     */
    val UNREACHABLE_SPEC: Value = Value("command spec not reachable from dispatch")

    /**
     * A node that executes during analysis (e.g. Spark 4's `ExecutableDuringAnalysis`):
     * by the time authorization rules run it may already have produced side effects.
     */
    val ANALYSIS_TIME_EXECUTION: Value = Value("node executes during analysis")

    /**
     * A matched spec whose extractors all failed against the current plan shape
     * (fail-open layer 3, typically Spark version drift).
     */
    val EXTRACTION_FAILURE: Value = Value("spec matched but extraction failed")
  }

  import Behavior._
  import ViolationKind.ViolationKind

  def behavior(spark: SparkSession): Behavior = {
    // SparkConf, not SparkSession.conf: see the note on session-level overrides above.
    val raw = spark.sparkContext.getConf.get(UNCLASSIFIED_NODE_BEHAVIOR_KEY, WARN.toString)
    Behavior.values.find(_.toString.equalsIgnoreCase(raw.trim)).getOrElse {
      throw new IllegalArgumentException(
        s"Invalid value '$raw' for $UNCLASSIFIED_NODE_BEHAVIOR_KEY," +
          s" expected one of: ${Behavior.values.mkString(", ")}")
    }
  }

  // WARN-mode logging is deduplicated on (plan class name, violation kind) per JVM
  private val warned = ConcurrentHashMap.newKeySet[(String, ViolationKind)]()

  // Violation counts by kind, kept regardless of behavior. Exposed for tests and as a
  // cheap metric hook until a proper metrics source is wired up.
  private[authz] val violationCounts = new ConcurrentHashMap[ViolationKind, LongAdder]()

  private[authz] def violationCount(kind: ViolationKind): Long = {
    Option(violationCounts.get(kind)).map(_.sum()).getOrElse(0L)
  }

  private[authz] def resetForTesting(): Unit = {
    warned.clear()
    violationCounts.clear()
  }

  /**
   * Report a plan node that the privilege builder could not classify, applying the
   * configured behavior. In `deny` mode this throws and the query fails closed.
   */
  def onViolation(
      spark: SparkSession,
      plan: LogicalPlan,
      kind: ViolationKind,
      detail: String = "",
      cause: Option[Throwable] = None): Unit = {
    violationCounts.computeIfAbsent(kind, _ => new LongAdder).increment()

    val classname = plan.getClass.getName
    def message: String = {
      val detailPart = if (detail.nonEmpty) s"; $detail" else ""
      val causePart = cause.map(e => s"; cause: $e").getOrElse("")
      s"Plan node $classname is not covered by authorization: $kind$detailPart$causePart." +
        s" Classify it with a command/scan spec, or add it to known_harmless_spec.json" +
        s" with a reason if it is not authz-relevant."
    }

    behavior(spark) match {
      case ALLOW =>
        if (LOG.isDebugEnabled) {
          LOG.debug(message)
        }
      case WARN =>
        if (warned.add((classname, kind))) {
          LOG.warn(s"$message (Further occurrences of this class will not be logged." +
            s" Set $UNCLASSIFIED_NODE_BEHAVIOR_KEY=deny to fail closed.)")
        }
      case DENY =>
        throw new AccessControlException(
          s"$message ($UNCLASSIFIED_NODE_BEHAVIOR_KEY=deny)",
          cause)
    }
  }
}
