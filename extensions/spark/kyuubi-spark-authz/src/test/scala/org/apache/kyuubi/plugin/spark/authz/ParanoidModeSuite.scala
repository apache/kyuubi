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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{LeafCommand, LeafNode, LogicalPlan, Project}
import org.apache.spark.sql.types.IntegerType
// scalastyle:off
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.plugin.spark.authz.ParanoidMode.{Behavior, UNCLASSIFIED_NODE_BEHAVIOR_KEY, ViolationKind}
import org.apache.kyuubi.plugin.spark.authz.serde.{HarmlessNodeSpec, KNOWN_HARMLESS_NODES, ScanDesc, ScanSpec}

/** A leaf relation the plugin has no classification for. */
case class UnclassifiedTestRelation() extends LeafNode {
  override def output: Seq[Attribute] = Seq(AttributeReference("id", IntegerType)())
}

/** A command the plugin has no classification for. */
case class UnclassifiedTestCommand() extends LeafCommand

class ParanoidModeSuite extends AnyFunSuite with BeforeAndAfterEach with BeforeAndAfterAll {
  // scalastyle:on

  private lazy val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName(getClass.getSimpleName)
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override def afterAll(): Unit = {
    // suites share a JVM: a leaked default session would be picked up by the next
    // suite's getOrCreate(), silently dropping its extensions
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    super.afterAll()
  }

  override def afterEach(): Unit = {
    spark.conf.unset(UNCLASSIFIED_NODE_BEHAVIOR_KEY)
    ParanoidMode.resetForTesting()
    super.afterEach()
  }

  private def withBehavior(behavior: String)(f: => Unit): Unit = {
    spark.conf.set(UNCLASSIFIED_NODE_BEHAVIOR_KEY, behavior)
    try f
    finally spark.conf.unset(UNCLASSIFIED_NODE_BEHAVIOR_KEY)
  }

  private def build(plan: LogicalPlan): Unit = {
    PrivilegesBuilder.build(plan, spark)
  }

  test("default behavior is warn") {
    assert(ParanoidMode.behavior(spark) === Behavior.WARN)
  }

  test("behavior values are parsed case-insensitively") {
    withBehavior("DENY") {
      assert(ParanoidMode.behavior(spark) === Behavior.DENY)
    }
    withBehavior("Allow") {
      assert(ParanoidMode.behavior(spark) === Behavior.ALLOW)
    }
  }

  test("invalid behavior value is rejected loudly") {
    withBehavior("yolo") {
      val e = intercept[IllegalArgumentException](ParanoidMode.behavior(spark))
      assert(e.getMessage.contains(UNCLASSIFIED_NODE_BEHAVIOR_KEY))
      assert(e.getMessage.contains("yolo"))
    }
  }

  test("deny: an unclassified leaf relation fails closed") {
    withBehavior("deny") {
      val e = intercept[AccessControlException](build(UnclassifiedTestRelation()))
      assert(e.getMessage.contains(classOf[UnclassifiedTestRelation].getName))
      assert(e.getMessage.contains(UNCLASSIFIED_NODE_BEHAVIOR_KEY))
      assert(ParanoidMode.violationCount(ViolationKind.UNCLASSIFIED_LEAF) === 1)
    }
  }

  test("deny: an unclassified command fails closed") {
    withBehavior("deny") {
      val e = intercept[AccessControlException](build(UnclassifiedTestCommand()))
      assert(e.getMessage.contains(classOf[UnclassifiedTestCommand].getName))
      assert(ParanoidMode.violationCount(ViolationKind.UNCLASSIFIED_COMMAND) === 1)
    }
  }

  test("deny: an unclassified node cannot hide under a constant projection") {
    // A Project whose output has no relation to its input is pruned for privilege
    // building, but the subtree still executes and must still be classified.
    withBehavior("deny") {
      val plan = Project(
        Seq(Alias(Literal(1), "x")()),
        UnclassifiedTestRelation())
      intercept[AccessControlException](build(plan))
    }
  }

  test("warn: unclassified nodes pass but every occurrence is counted") {
    withBehavior("warn") {
      build(UnclassifiedTestRelation())
      build(UnclassifiedTestRelation())
      assert(ParanoidMode.violationCount(ViolationKind.UNCLASSIFIED_LEAF) === 2)
    }
  }

  test("allow: legacy behavior, unclassified nodes pass silently") {
    withBehavior("allow") {
      build(UnclassifiedTestRelation())
      build(UnclassifiedTestCommand())
    }
  }

  test("deny: allowlisted nodes pass") {
    withBehavior("deny") {
      // OneRowRelation under a Project
      build(spark.sql("SELECT 1").queryExecution.optimizedPlan)
      // Range, LocalRelation
      build(spark.range(3).queryExecution.optimizedPlan)
      build(spark.sql("VALUES (1, 'a'), (2, 'b')").queryExecution.optimizedPlan)
      assert(ParanoidMode.violationCount(ViolationKind.UNCLASSIFIED_LEAF) === 0)
    }
  }

  test("deny: ordinary multi-operator queries recurse freely") {
    withBehavior("deny") {
      val df = spark.range(10).filter("id > 1")
        .join(spark.range(5), "id")
        .groupBy("id").count()
      build(df.queryExecution.optimizedPlan)
      build(df.queryExecution.analyzed)
    }
  }

  test("scan spec extraction failures are surfaced, not swallowed") {
    val spec = ScanSpec(
      classOf[UnclassifiedTestRelation].getName,
      Seq(ScanDesc("noSuchField", "LogicalRelationTableExtractor")))
    val (tables, failures) = spec.tablesWithFailures(UnclassifiedTestRelation(), spark)
    assert(tables.isEmpty)
    assert(failures.nonEmpty)
  }

  test("allowlist entries require a reason") {
    val e = intercept[IllegalArgumentException](
      HarmlessNodeSpec("some.Classname", "  ", Seq("3.5")))
    assert(e.getMessage.contains("reason"))
    intercept[IllegalArgumentException](HarmlessNodeSpec("", "a reason", Seq("3.5")))
  }

  test("allowlist entries require explicitly enumerated Spark versions, not ranges") {
    val e = intercept[IllegalArgumentException](
      HarmlessNodeSpec("some.Classname", "a reason", Nil))
    assert(e.getMessage.contains("verified Spark version"))
    // no range or wildcard syntax, exact major.minor pairs only
    Seq("<4.0", "3.x", "3", "3.5.1", "3.5+").foreach { bad =>
      val ex = intercept[IllegalArgumentException](
        HarmlessNodeSpec("some.Classname", "a reason", Seq(bad)))
      assert(ex.getMessage.contains(bad))
    }
  }

  test("an allowlist entry only applies to the Spark minors it was reviewed against") {
    val spec = HarmlessNodeSpec("some.Classname", "a reason", Seq("3.4", "3.5"))
    assert(spec.appliesTo("3.5"))
    assert(spec.appliesTo("3.4"))
    // fail closed on anything not explicitly listed, in either direction
    assert(!spec.appliesTo("3.3"))
    assert(!spec.appliesTo("4.0"))
  }

  test("the shipped allowlist is loadable and every entry carries a reason and versions") {
    assert(KNOWN_HARMLESS_NODES.nonEmpty)
    assert(KNOWN_HARMLESS_NODES.contains(
      "org.apache.spark.sql.catalyst.plans.logical.LocalRelation"))
    KNOWN_HARMLESS_NODES.values.foreach { spec =>
      assert(spec.reason.trim.nonEmpty, s"missing reason for ${spec.classname}")
      assert(
        spec.verifiedSparkVersions.nonEmpty,
        s"missing verified Spark versions for ${spec.classname}")
    }
  }
}
