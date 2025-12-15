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

package org.apache.kyuubi.engine.spark.operation

import java.lang.{Boolean => JBoolean}
import java.sql.Statement
import java.util.Locale

import scala.util.Try

import org.apache.spark.{KyuubiSparkContextHelper, TaskContext}
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.execution.{CollectLimitExec, LocalTableScanExec, QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.metric.SparkMetricsTestUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.kyuubi.SparkDatasetHelper
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.spark.{SparkSQLEngine, WithSparkSQLEngine}
import org.apache.kyuubi.engine.spark.session.SparkSessionImpl
import org.apache.kyuubi.operation.SparkDataTypeTests
import org.apache.kyuubi.util.reflect.{DynFields, DynMethods}

class SparkArrowbasedOperationSuite extends WithSparkSQLEngine with SparkDataTypeTests
  with SparkMetricsTestUtils {

  override protected def jdbcUrl: String = getJdbcUrl

  override def withKyuubiConf: Map[String, String] = Map.empty

  override def jdbcVars: Map[String, String] = {
    Map(KyuubiConf.OPERATION_RESULT_FORMAT.key -> resultFormat)
  }

  override def resultFormat: String = "arrow"

  override def beforeEach(): Unit = {
    super.beforeEach()
    withJdbcStatement() { statement =>
      checkResultSetFormat(statement, "arrow")
    }
    spark.catalog.listTables()
      .collect()
      .foreach { table =>
        if (table.isTemporary) {
          spark.catalog.dropTempView(table.name)
        } else {
          spark.sql(s"DROP TABLE IF EXISTS ${table.name}")
        }
        ()
      }
  }

  test("detect resultSet format") {
    withJdbcStatement() { statement =>
      checkResultSetFormat(statement, "arrow")
      statement.executeQuery(s"set ${KyuubiConf.OPERATION_RESULT_FORMAT.key}=thrift")
      checkResultSetFormat(statement, "thrift")
    }
  }

  test("Spark session timezone format") {
    withJdbcStatement() { statement =>
      def check(expect: String): Unit = {
        val query =
          """
            |SELECT
            |  from_utc_timestamp(
            |    from_unixtime(
            |      1670404535000 / 1000, 'yyyy-MM-dd HH:mm:ss'
            |    ),
            |    'GMT+08:00'
            |  )
            |""".stripMargin
        val resultSet = statement.executeQuery(query)
        assert(resultSet.next())
        assert(resultSet.getString(1) == expect)
      }

      def setTimeZone(timeZone: String): Unit = {
        val rs = statement.executeQuery(s"set spark.sql.session.timeZone=$timeZone")
        assert(rs.next())
      }

      Seq("true", "false").foreach { timestampAsString =>
        statement.executeQuery(
          s"set ${KyuubiConf.ARROW_BASED_ROWSET_TIMESTAMP_AS_STRING.key}=$timestampAsString")
        checkArrowBasedRowSetTimestampAsString(statement, timestampAsString)
        setTimeZone("UTC")
        check("2022-12-07 17:15:35.0")
        setTimeZone("GMT+8")
        check("2022-12-08 01:15:35.0")
      }
    }
  }

  test("assign a new execution id for arrow-based result") {
    val listener = new SQLMetricsListener
    withJdbcStatement() { statement =>
      withSparkListener(listener) {
        val result = statement.executeQuery("select 1 as c1")
        assert(result.next())
        assert(result.getInt("c1") == 1)
      }
    }

    assert(listener.queryExecution.analyzed.isInstanceOf[Project])
  }

  test("arrow-based query metrics") {
    // TODO: the issue is being investigated Fu Chen
    assume(SPARK_ENGINE_RUNTIME_VERSION < "4.1")
    val listener = new SQLMetricsListener
    withJdbcStatement() { statement =>
      withSparkListener(listener) {
        val result = statement.executeQuery("select 1 as c1")
        assert(result.next())
        assert(result.getInt("c1") == 1)
      }
    }

    val metrics = listener.queryExecution.executedPlan.collectLeaves().head.metrics
    assert(metrics.contains("numOutputRows"))
    assert(metrics("numOutputRows").value === 1)
  }

  test("SparkDatasetHelper.executeArrowBatchCollect should return expect row count") {
    val returnSize = Seq(
      0, // spark optimizer guaranty the `limit != 0`, it's just for the sanity check
      7, // less than one partition
      10, // equal to one partition
      13, // between one and two partitions, run two jobs
      20, // equal to two partitions
      29, // between two and three partitions
      1000, // all partitions
      1001) // more than total row count

    def runAndCheck(sparkPlan: SparkPlan, expectSize: Int): Unit = {
      val arrowBinary = SparkDatasetHelper.executeArrowBatchCollect(sparkPlan)
      val rows = fromBatchIterator(
        arrowBinary.iterator,
        sparkPlan.schema,
        "",
        true,
        true, // spark.sql.execution.arrow.useLargeVarTypes
        KyuubiSparkContextHelper.dummyTaskContext())
      assert(rows.size == expectSize)
    }

    val excludedRules = Seq(
      "org.apache.spark.sql.catalyst.optimizer.EliminateLimits",
      "org.apache.spark.sql.catalyst.optimizer.OptimizeLimitZero",
      "org.apache.spark.sql.execution.adaptive.AQEPropagateEmptyRelation").mkString(",")
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> excludedRules,
      SQLConf.ADAPTIVE_OPTIMIZER_EXCLUDED_RULES.key -> excludedRules) {
      // aqe
      // outermost AdaptiveSparkPlanExec
      spark.range(1000)
        .repartitionByRange(100, col("id"))
        .createOrReplaceTempView("t_1")
      spark.sql("select * from t_1")
        .foreachPartition { p: Iterator[Row] =>
          assert(p.length == 10)
          ()
        }
      returnSize.foreach { size =>
        val df = spark.sql(s"select * from t_1 limit $size")
        val headPlan = df.queryExecution.executedPlan.collectLeaves().head
        assert(headPlan.isInstanceOf[AdaptiveSparkPlanExec])
        val finalPhysicalPlan = headPlan.asInstanceOf[AdaptiveSparkPlanExec].finalPhysicalPlan
        assert(finalPhysicalPlan.isInstanceOf[CollectLimitExec])
        if (size > 1000) {
          runAndCheck(df.queryExecution.executedPlan, 1000)
        } else {
          runAndCheck(df.queryExecution.executedPlan, size)
        }
      }

      // outermost CollectLimitExec
      spark.range(0, 1000, 1, numPartitions = 100)
        .createOrReplaceTempView("t_2")
      spark.sql("select * from t_2")
        .foreachPartition { p: Iterator[Row] =>
          assert(p.length == 10)
          ()
        }
      returnSize.foreach { size =>
        val df = spark.sql(s"select * from t_2 limit $size")
        val plan = df.queryExecution.executedPlan
        assert(plan.isInstanceOf[CollectLimitExec])
        if (size > 1000) {
          runAndCheck(df.queryExecution.executedPlan, 1000)
        } else {
          runAndCheck(df.queryExecution.executedPlan, size)
        }
      }
    }
  }

  test("aqe should work properly") {

    val s = spark
    import s.implicits._

    spark.sparkContext.parallelize(
      (1 to 100).map(i => TestData(i, i.toString))).toDF()
      .createOrReplaceTempView("testData")
    spark.sparkContext.parallelize(
      TestData2(1, 1) ::
        TestData2(1, 2) ::
        TestData2(2, 1) ::
        TestData2(2, 2) ::
        TestData2(3, 1) ::
        TestData2(3, 2) :: Nil,
      2).toDF()
      .createOrReplaceTempView("testData2")

    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> "5",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        """
          |SELECT * FROM(
          |  SELECT * FROM testData join testData2 ON key = a where value = '1'
          |) LIMIT 1
          |""".stripMargin)
      val smj = plan.collect { case smj: SortMergeJoinExec => smj }
      val bhj = (adaptivePlan match {
        // SPARK-51008 (4.0.0) adds ResultQueryStageExec
        case queryStage: QueryStageExec => queryStage.plan
        case plan => plan
      }).collect { case bhj: BroadcastHashJoinExec => bhj }
      assert(smj.size == 1)
      assert(bhj.size == 1)
    }
  }

  test("result offset support") {
    assume(SPARK_ENGINE_RUNTIME_VERSION >= "3.4")
    var numStages = 0
    val listener = new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        numStages = jobStart.stageInfos.length
      }
    }
    withJdbcStatement() { statement =>
      withSparkListener(listener) {
        withPartitionedTable("t_3") {
          statement.executeQuery("select * from t_3 limit 10 offset 10")
        }
      }
    }
    // the extra shuffle be introduced if the `offset` > 0
    assert(numStages == 2)
  }

  test("arrow serialization should not introduce extra shuffle for outermost limit") {
    var numStages = 0
    val listener = new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        numStages = jobStart.stageInfos.length
      }
    }
    withJdbcStatement() { statement =>
      withSparkListener(listener) {
        withPartitionedTable("t_3") {
          statement.executeQuery("select * from t_3 limit 1000")
        }
      }
    }
    // Should be only one stage since there is no shuffle.
    assert(numStages == 1)
  }

  test("CommandResultExec should not trigger job") {
    val listener = new JobCountListener
    val l2 = new SQLMetricsListener
    val nodeName = spark.sql("SHOW TABLES").queryExecution.executedPlan.getClass.getName
    assert(nodeName == "org.apache.spark.sql.execution.CommandResultExec")
    withJdbcStatement("table_1") { statement =>
      statement.executeQuery("CREATE TABLE table_1 (id bigint) USING parquet")
      withSparkListener(listener) {
        withSparkListener(l2) {
          val resultSet = statement.executeQuery("SHOW TABLES")
          assert(resultSet.next())
          assert(resultSet.getString("tableName") == "table_1")
        }
      }
    }

    assert(l2.queryExecution.executedPlan.getClass.getName ==
      "org.apache.spark.sql.execution.CommandResultExec")
    assert(listener.numJobs == 0)
  }

  test("LocalTableScanExec should not trigger job") {
    val listener = new JobCountListener
    withJdbcStatement("view_1") { statement =>
      withSparkListener(listener) {
        withAllSessions { s =>
          import s.implicits._
          Seq((1, "a")).toDF("c1", "c2").createOrReplaceTempView("view_1")
          val plan = s.sql("select * from view_1").queryExecution.executedPlan
          assert(plan.isInstanceOf[LocalTableScanExec])
        }
        val resultSet = statement.executeQuery("select * from view_1")
        assert(resultSet.next())
        assert(!resultSet.next())
      }
    }
    assert(listener.numJobs == 0)
  }

  test("LocalTableScanExec metrics") {
    val listener = new SQLMetricsListener
    withJdbcStatement("view_1") { statement =>
      withSparkListener(listener) {
        withAllSessions { s =>
          import s.implicits._
          Seq((1, "a")).toDF("c1", "c2").createOrReplaceTempView("view_1")
        }
        val result = statement.executeQuery("select * from view_1")
        assert(result.next())
        assert(!result.next())
      }
    }

    val metrics = listener.queryExecution.executedPlan.collectLeaves().head.metrics
    assert(metrics.contains("numOutputRows"))
    assert(metrics("numOutputRows").value === 1)
  }

  test("post LocalTableScanExec driver-side metrics") {
    val expectedMetrics = Map(
      0L -> (("LocalTableScan", Map("number of output rows" -> "2"))))
    withTables("view_1") {
      val s = spark
      import s.implicits._
      Seq((1, "a"), (2, "b")).toDF("c1", "c2").createOrReplaceTempView("view_1")
      val df = spark.sql("SELECT * FROM view_1")
      val metrics = getSparkPlanMetrics(df)
      assert(metrics == expectedMetrics)
    }
  }

  test("post CommandResultExec driver-side metrics") {
    spark.sql("show tables").show(truncate = false)
    val expectedMetrics = Map(
      0L -> (("CommandResult", Map("number of output rows" -> "2"))))
    withTables("table_1", "table_2") {
      spark.sql("CREATE TABLE table_1 (id bigint) USING parquet")
      spark.sql("CREATE TABLE table_2 (id bigint) USING parquet")
      val df = spark.sql("SHOW TABLES")
      val metrics = getSparkPlanMetrics(df)
      assert(metrics == expectedMetrics)
    }
  }

  private def checkResultSetFormat(statement: Statement, expectFormat: String): Unit = {
    val query =
      s"""
         |SELECT '$${hivevar:${KyuubiConf.OPERATION_RESULT_FORMAT.key}}' AS col
         |""".stripMargin
    val resultSet = statement.executeQuery(query)
    assert(resultSet.next())
    assert(resultSet.getString("col") === expectFormat)
  }

  private def checkArrowBasedRowSetTimestampAsString(
      statement: Statement,
      expect: String): Unit = {
    val query =
      s"""
         |SELECT '$${hivevar:${KyuubiConf.ARROW_BASED_ROWSET_TIMESTAMP_AS_STRING.key}}' AS col
         |""".stripMargin
    val resultSet = statement.executeQuery(query)
    assert(resultSet.next())
    assert(resultSet.getString("col") === expect)
  }

  // since all the new sessions have their owner listener bus, we should register the listener
  // in the current session.
  private def withSparkListener[T](listener: QueryExecutionListener)(body: => T): T = {
    withAllSessions(s => s.listenerManager.register(listener))
    try {
      val result = body
      KyuubiSparkContextHelper.waitListenerBus(spark)
      result
    } finally {
      withAllSessions(s => s.listenerManager.unregister(listener))
    }
  }

  // since all the new sessions have their owner listener bus, we should register the listener
  // in the current session.
  private def withSparkListener[T](listener: SparkListener)(body: => T): T = {
    withAllSessions(s => s.sparkContext.addSparkListener(listener))
    try {
      val result = body
      KyuubiSparkContextHelper.waitListenerBus(spark)
      result
    } finally {
      withAllSessions(s => s.sparkContext.removeSparkListener(listener))
    }
  }

  private def withPartitionedTable[T](viewName: String)(body: => T): T = {
    withAllSessions { spark =>
      spark.range(0, 1000, 1, numPartitions = 100)
        .createOrReplaceTempView(viewName)
    }
    try {
      body
    } finally {
      withAllSessions { spark =>
        spark.sql(s"DROP VIEW IF EXISTS $viewName")
      }
    }
  }

  private def withAllSessions(op: SparkSession => Unit): Unit = {
    SparkSQLEngine.currentEngine.get
      .backendService
      .sessionManager
      .allSessions()
      .map(_.asInstanceOf[SparkSessionImpl].spark)
      .foreach(op(_))
  }

  private def runAdaptiveAndVerifyResult(query: String): (SparkPlan, SparkPlan) = {
    val dfAdaptive = spark.sql(query)
    val planBefore = dfAdaptive.queryExecution.executedPlan
    val result = dfAdaptive.collect()
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = spark.sql(query)
      QueryTest.checkAnswer(df, df.collect().toSeq)
    }
    val planAfter = dfAdaptive.queryExecution.executedPlan
    val adaptivePlan = planAfter.asInstanceOf[AdaptiveSparkPlanExec].executedPlan
    val exchanges = adaptivePlan.collect {
      case e: Exchange => e
    }
    assert(exchanges.isEmpty, "The final plan should not contain any Exchange node.")
    (dfAdaptive.queryExecution.sparkPlan, adaptivePlan)
  }

  /**
   * Sets all SQL configurations specified in `pairs`, calls `f`, and then restores all SQL
   * configurations.
   */
  protected def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val conf = SQLConf.get
    val (keys, values) = pairs.unzip
    val currentValues = keys.map { key =>
      if (conf.contains(key)) {
        Some(conf.getConfString(key))
      } else {
        None
      }
    }
    (keys, values).zipped.foreach { (k, v) =>
      if (SQLConf.isStaticConfigKey(k)) {
        throw new KyuubiException(s"Cannot modify the value of a static config: $k")
      }
      conf.setConfString(k, v)
    }
    try f
    finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => conf.setConfString(key, value)
        case (key, None) => conf.unsetConf(key)
      }
    }
  }

  private def withTables[T](tableNames: String*)(f: => T): T = {
    try {
      f
    } finally {
      tableNames.foreach { name =>
        if (name.toUpperCase(Locale.ROOT).startsWith("VIEW")) {
          spark.sql(s"DROP VIEW IF EXISTS $name")
        } else {
          spark.sql(s"DROP TABLE IF EXISTS $name")
        }
      }
    }
  }

  private lazy val fromBatchIteratorMethod = DynMethods.builder("fromBatchIterator")
    .hiddenImpl( // SPARK-51079: Spark 4.0 or later
      "org.apache.spark.sql.execution.arrow.ArrowConverters$",
      classOf[Iterator[Array[Byte]]],
      classOf[StructType],
      classOf[String],
      classOf[Boolean],
      classOf[Boolean],
      classOf[TaskContext])
    .hiddenImpl( // SPARK-43528: Spark 3.5
      "org.apache.spark.sql.execution.arrow.ArrowConverters$",
      classOf[Iterator[Array[Byte]]],
      classOf[StructType],
      classOf[String],
      classOf[Boolean],
      classOf[TaskContext])
    .hiddenImpl( // for Spark 3.4 or previous
      "org.apache.spark.sql.execution.arrow.ArrowConverters$",
      classOf[Iterator[Array[Byte]]],
      classOf[StructType],
      classOf[String],
      classOf[TaskContext])
    .buildChecked()

  private lazy val arrowConvertersObject = DynFields.builder()
    .impl("org.apache.spark.sql.execution.arrow.ArrowConverters$", "MODULE$")
    .buildStaticChecked[Any]()
    .get()

  def fromBatchIterator(
      arrowBatchIter: Iterator[Array[Byte]],
      schema: StructType,
      timeZoneId: String,
      errorOnDuplicatedFieldNames: JBoolean,
      largeVarTypes: JBoolean,
      context: TaskContext): Iterator[InternalRow] =
    Try { // SPARK-51079: Spark 4.0 or later
      fromBatchIteratorMethod.invokeChecked[Iterator[InternalRow]](
        arrowConvertersObject,
        arrowBatchIter,
        schema,
        timeZoneId,
        errorOnDuplicatedFieldNames,
        largeVarTypes,
        context)
    }.recover { case _: Exception => // SPARK-43528: Spark 3.5
      fromBatchIteratorMethod.invokeChecked[Iterator[InternalRow]](
        arrowConvertersObject,
        arrowBatchIter,
        schema,
        timeZoneId,
        errorOnDuplicatedFieldNames,
        context)
    }.recover { case _: Exception => // for Spark 3.4 or previous
      fromBatchIteratorMethod.invokeChecked[Iterator[InternalRow]](
        arrowConvertersObject,
        arrowBatchIter,
        schema,
        timeZoneId,
        context)
    }.get

  class JobCountListener extends SparkListener {
    var numJobs = 0
    override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
      numJobs += 1
    }
  }

  class SQLMetricsListener extends QueryExecutionListener {
    var queryExecution: QueryExecution = _
    override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
      queryExecution = qe
    }
    override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
  }
}

case class TestData(key: Int, value: String)
case class TestData2(a: Int, b: Int)
