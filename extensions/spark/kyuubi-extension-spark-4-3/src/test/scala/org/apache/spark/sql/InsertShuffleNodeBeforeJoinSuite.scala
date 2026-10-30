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

package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeReference, SortOrder, TransformExpression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.connector.catalog.functions.BucketFunction
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.GroupPartitionsExec
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, REPARTITION_BY_COL, ShuffleExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types.{IntegerType, LongType}

import org.apache.kyuubi.sql.{KyuubiEnsureRequirements, KyuubiSQLConf}

class InsertShuffleNodeBeforeJoinSuite extends KyuubiSparkSQLExtensionTest {
  override protected def beforeAll(): Unit = {
    super.beforeAll()
    setupData()
  }

  override def sparkConf(): SparkConf = {
    super.sparkConf()
      .set(
        StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        "org.apache.kyuubi.sql.KyuubiSparkSQLExtension")
  }

  test("force shuffle before join") {
    def checkShuffleNodeNum(sqlString: String, num: Int): Unit = {
      var expectedResult: Seq[Row] = Seq.empty
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        expectedResult = sql(sqlString).collect()
      }
      val df = sql(sqlString)
      checkAnswer(df, expectedResult)
      assert(
        collect(df.queryExecution.executedPlan) {
          case shuffle: ShuffleExchangeLike if shuffle.shuffleOrigin == ENSURE_REQUIREMENTS =>
            shuffle
        }.size == num)
    }

    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      KyuubiSQLConf.FORCE_SHUFFLE_BEFORE_JOIN.key -> "true") {
      Seq("SHUFFLE_HASH", "MERGE").foreach { joinHint =>
        // positive case
        checkShuffleNodeNum(
          s"""
             |SELECT /*+ $joinHint(t2, t3) */ t1.c1, t1.c2, t2.c1, t3.c1 from t1
             | JOIN t2 ON t1.c1 = t2.c1
             | JOIN t3 ON t1.c1 = t3.c1
             | """.stripMargin,
          4)

        // negative case
        checkShuffleNodeNum(
          s"""
             |SELECT /*+ $joinHint(t2, t3) */ t1.c1, t1.c2, t2.c1, t3.c1 from t1
             | JOIN t2 ON t1.c1 = t2.c1
             | JOIN t3 ON t1.c2 = t3.c2
             | """.stripMargin,
          4)
      }

      checkShuffleNodeNum(
        """
          |SELECT t1.c1, t2.c1, t3.c2 from t1
          | JOIN t2 ON t1.c1 = t2.c1
          | JOIN (
          |  SELECT c2, count(*) FROM t1 GROUP BY c2
          | ) t3 ON t1.c1 = t3.c2
          | """.stripMargin,
        5)

      checkShuffleNodeNum(
        """
          |SELECT t1.c1, t2.c1, t3.c1 from t1
          | JOIN t2 ON t1.c1 = t2.c1
          | JOIN (
          |  SELECT c1, count(*) FROM t1 GROUP BY c1
          | ) t3 ON t1.c1 = t3.c1
          | """.stripMargin,
        5)
    }
  }

  test("ensure requirements projects keyed partitioning onto the operation keys") {
    val operationKey = AttributeReference("operationKey", IntegerType)()
    val extraKey = AttributeReference("extraKey", IntegerType)()
    val child = RequirementsTestPlan(outputPartitioning = KeyedPartitioning(
      Seq(extraKey, operationKey),
      Seq(InternalRow(1, 1), InternalRow(2, 1), InternalRow(3, 2))))
    val distribution = ClusteredDistribution(Seq(operationKey), requiredNumPartitions = Some(2))
    val parent = RequirementsTestPlan(
      children = Seq(child),
      requiredChildDistribution = Seq(distribution),
      requiredChildOrdering = Seq(Nil))

    withSQLConf(SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
      val newChild = KyuubiEnsureRequirements(parent).children.head
      assert(newChild.isInstanceOf[GroupPartitionsExec])
      assert(newChild.asInstanceOf[GroupPartitionsExec].joinKeyPositions.contains(Seq(1)))
      assert(newChild.outputPartitioning.numPartitions == 2)
      assert(newChild.outputPartitioning.satisfies(distribution))
    }
  }

  test("ensure requirements groups duplicate keys to the required partition count") {
    val operationKey = AttributeReference("operationKey", IntegerType)()
    val child = RequirementsTestPlan(outputPartitioning = KeyedPartitioning(
      Seq(operationKey),
      Seq(InternalRow(1), InternalRow(1), InternalRow(2))))

    Seq(2, 5).foreach { requiredCount =>
      val distribution =
        ClusteredDistribution(Seq(operationKey), requiredNumPartitions = Some(requiredCount))
      val parent = RequirementsTestPlan(
        children = Seq(child),
        requiredChildDistribution = Seq(distribution),
        requiredChildOrdering = Seq(Nil))
      val newChild = KyuubiEnsureRequirements(parent).children.head
      if (requiredCount == 2) {
        assert(newChild.isInstanceOf[GroupPartitionsExec])
      } else {
        assert(newChild.isInstanceOf[ShuffleExchangeExec])
      }
      assert(newChild.outputPartitioning.numPartitions == requiredCount)
      assert(newChild.outputPartitioning.satisfies(distribution))
    }
  }

  test("ensure requirements accepts a single ordered keyed partition") {
    val operationKey = AttributeReference("operationKey", IntegerType)()
    val child = RequirementsTestPlan(outputPartitioning =
      KeyedPartitioning(Seq(operationKey), Seq(InternalRow(1))))
    val parent = RequirementsTestPlan(
      children = Seq(child),
      requiredChildDistribution = Seq(OrderedDistribution(Seq(SortOrder(operationKey, Ascending)))),
      requiredChildOrdering = Seq(Nil))

    withSQLConf(SQLConf.V2_BUCKETING_SORTING_ENABLED.key -> "true") {
      assert(KyuubiEnsureRequirements(parent).children.head eq child)
    }
  }

  test("ensure requirements reduces both sides onto common partition keys") {
    val leftKey = AttributeReference("leftKey", LongType)()
    val rightKey = AttributeReference("rightKey", LongType)()
    val left = RequirementsTestPlan(outputPartitioning = KeyedPartitioning(
      Seq(TransformExpression(BucketFunction, Seq(leftKey), Some(12))),
      Seq(InternalRow(1), InternalRow(5), InternalRow(9))))
    val right = RequirementsTestPlan(outputPartitioning = KeyedPartitioning(
      Seq(TransformExpression(BucketFunction, Seq(rightKey), Some(8))),
      Seq(InternalRow(1), InternalRow(5))))
    val join = SortMergeJoinExec(Seq(leftKey), Seq(rightKey), Inner, None, left, right)

    withSQLConf(
      SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
      SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true",
      SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "false") {
      val planned = KyuubiEnsureRequirements(join)
      val groupings = planned.collect { case grouping: GroupPartitionsExec => grouping }
      assert(groupings.size == 2)
      groupings.foreach { grouping =>
        assert(grouping.reducers.exists(_.exists(_.isDefined)))
        assert(grouping.groupedPartitions.map(_._1.row.getInt(0)) == Seq(1))
      }
      assert(planned.collect { case shuffle: ShuffleExchangeExec => shuffle }.isEmpty)
      val replanned = KyuubiEnsureRequirements(planned)
      assert(replanned.collect { case grouping: GroupPartitionsExec => grouping }.size == 2)
      assert(replanned.collect { case shuffle: ShuffleExchangeExec => shuffle }.isEmpty)
    }
  }

  test("ensure requirements preserves join key order and explicit repartitions") {
    val firstKey = AttributeReference("firstKey", IntegerType)()
    val secondKey = AttributeReference("secondKey", IntegerType)()
    val partitioning = HashPartitioning(Seq(firstKey, secondKey), 10)
    val child = RequirementsTestPlan(outputPartitioning = partitioning)
    val exchange = ShuffleExchangeExec(partitioning, child, REPARTITION_BY_COL)
    val join = SortMergeJoinExec(
      Seq(secondKey, firstKey),
      Seq(secondKey, firstKey),
      Inner,
      None,
      exchange,
      child)

    val planned = KyuubiEnsureRequirements(join).asInstanceOf[SortMergeJoinExec]
    assert(planned.leftKeys == Seq(secondKey, firstKey))
    assert(planned.rightKeys == Seq(secondKey, firstKey))
    assert(planned.left.collect { case shuffle: ShuffleExchangeExec => shuffle }.nonEmpty)
    assert(KyuubiEnsureRequirements(exchange).isInstanceOf[ShuffleExchangeExec])
  }

  test("ensure requirements preserves partially clustered alignment when reapplied") {
    val leftKey = AttributeReference("leftKey", IntegerType)()
    val rightKey = AttributeReference("rightKey", IntegerType)()
    val left = RequirementsTestPlan(outputPartitioning =
      KeyedPartitioning(Seq(leftKey), Seq(InternalRow(1))))
    val right = RequirementsTestPlan(outputPartitioning =
      KeyedPartitioning(Seq(rightKey), Seq(InternalRow(1), InternalRow(1), InternalRow(2))))
    val join = SortMergeJoinExec(Seq(leftKey), Seq(rightKey), Inner, None, left, right)

    withSQLConf(
      SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
      SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "true") {
      val planned = KyuubiEnsureRequirements(join)
      val replanned = KyuubiEnsureRequirements(planned)
      val originalGroupings = planned.collect { case grouping: GroupPartitionsExec => grouping }
      val newGroupings = replanned.collect { case grouping: GroupPartitionsExec => grouping }
      assert(originalGroupings.size == 2)
      assert(newGroupings.size == 2)
      assert(originalGroupings.map(_.distributePartitions) == Seq(false, true))
      assert(newGroupings.map(_.distributePartitions) == Seq(false, true))
      assert(newGroupings.map(_.groupedPartitions) == originalGroupings.map(_.groupedPartitions))
    }
  }

  private case class RequirementsTestPlan(
      override val children: Seq[SparkPlan] = Nil,
      override val outputPartitioning: Partitioning = UnknownPartitioning(0),
      override val requiredChildDistribution: Seq[Distribution] = Nil,
      override val requiredChildOrdering: Seq[Seq[SortOrder]] = Nil) extends SparkPlan {
    override def output: Seq[Attribute] = Seq.empty
    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("RequirementsTestPlan cannot execute")
    override protected def withNewChildrenInternal(
        newChildren: IndexedSeq[SparkPlan]): SparkPlan = copy(children = newChildren)
  }
}
