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

package org.apache.kyuubi.plugin.spark.authz.serde

import scala.collection.mutable.ArrayBuffer

import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.slf4j.LoggerFactory

import org.apache.kyuubi.plugin.spark.authz.OperationType
import org.apache.kyuubi.plugin.spark.authz.OperationType.OperationType

/**
 * A command specification contains
 *  - different [[Descriptor]]s for specific implementations. It's a list to cover:
 *    - A command may have multiple object to describe, such as create table A like B
 *    - An object descriptor may vary through spark versions, it wins at least once if one of
 *      the descriptors matches
 *  - the classname of a command which this spec point to
 *  - the [[OperationType]] of this command which finally maps to an access privilege
 */
private[serde] object SparkVersionAudit {

  /**
   * Validates a spec's audited-Spark-versions list. Versions are exact `major.minor`
   * pairs, deliberately enumerated rather than expressed as ranges: range boundaries
   * invite misreadings ("less than 4.0, exclusive" read as inclusive), while an explicit
   * list has no boundary to misread, and a new Spark minor is unaudited by default until
   * a human adds it.
   */
  def validate(classname: String, versions: Seq[String]): Unit = {
    versions.foreach { v =>
      require(
        v.matches("""\d+\.\d+"""),
        s"""spec for $classname: verified Spark version '$v' must be an
           | exact major.minor pair (e.g. "3.5"): explicit enumeration,
           | no ranges or wildcards""".stripMargin)
    }
  }
}

trait CommandSpec extends {
  @JsonIgnore
  final protected val LOG = LoggerFactory.getLogger(getClass)
  def classname: String
  def opType: String

  /**
   * The exact Spark `major.minor` versions this spec was authored or re-reviewed against.
   * For command and scan specs this is ADVISORY metadata: the spec still engages on other
   * versions (a spec imposes checks, so staying active on an unaudited version is the
   * safe direction: drift is caught by the extraction-failure checks and the build-time
   * coverage suite). Contrast [[HarmlessNodeSpec.verifiedSparkVersions]], where an
   * unverified version makes the entry inert, because an allowlist entry grants silence.
   * Empty means not yet audited per-version.
   */
  def verifiedSparkVersions: Seq[String]

  final def operationType: OperationType = OperationType.withName(opType)
}

trait CommandSpecs[T <: CommandSpec] {
  def specs: Seq[T]
}

/**
 * A specification describe a database command
 *
 * @param classname the database command classname
 * @param databaseDescs a list of database descriptors
 * @param opType operation type, e.g. CREATEDATABASE
 */
case class DatabaseCommandSpec(
    classname: String,
    databaseDescs: Seq[DatabaseDesc],
    opType: String = OperationType.QUERY.toString,
    uriDescs: Seq[UriDesc] = Nil,
    verifiedSparkVersions: Seq[String] = Nil) extends CommandSpec {
  SparkVersionAudit.validate(classname, verifiedSparkVersions)
}

/**
 * A specification describe a function command
 *
 * @param classname the database command classname
 * @param functionDescs a list of function descriptors
 * @param opType operation type, e.g. DROPFUNCTION
 */
case class FunctionCommandSpec(
    classname: String,
    functionDescs: Seq[FunctionDesc],
    opType: String,
    verifiedSparkVersions: Seq[String] = Nil) extends CommandSpec {
  SparkVersionAudit.validate(classname, verifiedSparkVersions)
}

/**
 * A specification describe a table command
 *
 * @param classname the database command classname
 * @param tableDescs a list of table descriptors
 * @param opType operation type, e.g. DROPFUNCTION
 * @param queryDescs the query descriptors a table command may have
 */
case class TableCommandSpec(
    classname: String,
    tableDescs: Seq[TableDesc],
    opType: String = OperationType.QUERY.toString,
    queryDescs: Seq[QueryDesc] = Nil,
    uriDescs: Seq[UriDesc] = Nil,
    verifiedSparkVersions: Seq[String] = Nil) extends CommandSpec {
  SparkVersionAudit.validate(classname, verifiedSparkVersions)

  def queries: LogicalPlan => Seq[LogicalPlan] = plan => {
    queryDescs.flatMap { qd =>
      try {
        qd.extract(plan)
      } catch {
        case e: Exception =>
          LOG.debug(qd.error(plan, e))
          None
      }
    }
  }
}

/**
 * A specification for a plan node that should be unconditionally denied.
 *
 * @param classname the fully-qualified plan node classname
 * @param message   the error message surfaced in the AccessControlException
 */
case class DeniedPlanNodeSpec(classname: String, message: String)

/**
 * A specification declaring that a plan node class is not authorization-relevant, so its
 * appearance during privilege building is not a classification violation (see
 * [[org.apache.kyuubi.plugin.spark.authz.ParanoidMode]]).
 *
 * The "known" and "harmless" assertions are only as good as the review behind them, and a
 * class is free to change shape under the same fully qualified name in the next Spark
 * release (exactly what CALL did between Spark 3 and 4). Each entry therefore names the
 * Spark minor versions it has been reviewed against: as an explicit enumeration, not a
 * range: ranges invite boundary misreadings ("less than 4.0, exclusive" read as
 * inclusive), while a list has no boundary to misread, and a new Spark minor is unverified
 * by default until a human adds it.
 *
 * @param classname the fully qualified plan node classname
 * @param reason why this node is harmless: a required, human-reviewed justification
 * @param verifiedSparkVersions the exact Spark `major.minor` versions the reason was
 *                              reviewed against; on any other version the entry is inert
 *                              and the node counts as unclassified
 */
case class HarmlessNodeSpec(
    classname: String,
    reason: String,
    verifiedSparkVersions: Seq[String]) {
  require(classname.nonEmpty, "harmless node spec requires a classname")
  require(
    reason.trim.nonEmpty,
    s"harmless node spec for $classname requires a reason: each allowlist entry must be" +
      s" a reviewed decision, not a reflexive silencing")
  require(
    verifiedSparkVersions.nonEmpty,
    s"harmless node spec for $classname requires at least one verified Spark version:" +
      s" 'harmless' is an assertion about a class on a specific Spark version")
  SparkVersionAudit.validate(classname, verifiedSparkVersions)

  def appliesTo(sparkMajorMinor: String): Boolean = {
    verifiedSparkVersions.contains(sparkMajorMinor)
  }
}

case class ScanSpec(
    classname: String,
    scanDescs: Seq[ScanDesc],
    functionDescs: Seq[FunctionDesc] = Seq.empty,
    uriDescs: Seq[UriDesc] = Seq.empty,
    verifiedSparkVersions: Seq[String] = Nil) extends CommandSpec {
  SparkVersionAudit.validate(classname, verifiedSparkVersions)

  override def opType: String = OperationType.QUERY.toString
  def tables: (LogicalPlan, SparkSession) => Seq[Table] = (plan, spark) => {
    tablesWithFailures(plan, spark)._1
  }

  /**
   * Like [[tables]], but also returns the extraction failures so the caller can tell a scan
   * that legitimately has no table from one whose extractors all broke (fail-open layer 3).
   */
  def tablesWithFailures: (LogicalPlan, SparkSession) => (Seq[Table], Seq[Throwable]) =
    (plan, spark) => {
      val failures = ArrayBuffer[Throwable]()
      val tables = scanDescs.flatMap { td =>
        try {
          td.extract(plan, spark)
        } catch {
          case e: Exception =>
            LOG.debug(td.error(plan, e))
            failures += e
            None
        }
      }
      (tables, failures)
    }

  def uris: LogicalPlan => Seq[Uri] = plan => {
    urisWithFailures(plan)._1
  }

  /** Like [[uris]], but also returns the extraction failures. */
  def urisWithFailures: LogicalPlan => (Seq[Uri], Seq[Throwable]) = plan => {
    val failures = ArrayBuffer[Throwable]()
    val uris = uriDescs.flatMap { ud =>
      try {
        ud.extract(plan)
      } catch {
        case e: Exception =>
          LOG.debug(ud.error(plan, e))
          failures += e
          None
      }
    }
    (uris, failures)
  }

  def functions: Expression => Seq[Function] = expr => {
    functionDescs.flatMap { fd =>
      try {
        Some(fd.extract(expr))
      } catch {
        case e: Exception =>
          LOG.debug(fd.error(expr, e))
          None
      }
    }
  }
}
