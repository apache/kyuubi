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

package org.apache.kyuubi.sql.watchdog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, Expression}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.slf4j.LoggerFactory

import org.apache.kyuubi.sql.KyuubiSQLConf

case class DangerousJoinInterceptor(session: SparkSession) extends SparkStrategy {
  import DangerousJoinInterceptor._

  private val logger = LoggerFactory.getLogger(classOf[DangerousJoinInterceptor])

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val conf = session.sessionState.conf
    if (!conf.getConf(KyuubiSQLConf.DANGEROUS_JOIN_ENABLED)) {
      return Nil
    }
    val ratio = conf.getConf(KyuubiSQLConf.DANGEROUS_JOIN_BROADCAST_RATIO)
    val threshold = conf.autoBroadcastJoinThreshold
    plan.foreach {
      case join: Join =>
        detect(join, threshold, ratio).foreach { reason =>
          val entry = DangerousJoinCounter.Entry(
            sqlText = plan.toString(),
            joinType = join.joinType.sql,
            reason = reason,
            leftSize = join.left.stats.sizeInBytes,
            rightSize = join.right.stats.sizeInBytes,
            broadcastThreshold = threshold,
            broadcastRatio = ratio)
          DangerousJoinCounter.add(entry)
          logger.warn(s"$KYUUBI_LOG_KEY=${entry.toJson}")
          if (conf.getConf(KyuubiSQLConf.DANGEROUS_JOIN_ACTION) == REJECT) {
            throw new KyuubiDangerousJoinException(entry.toJson)
          }
        }
      case _ =>
    }
    Nil
  }

  private def detect(join: Join, threshold: Long, ratio: Double): Option[String] = {
    if (threshold <= 0) {
      return None
    }
    val leftSize = join.left.stats.sizeInBytes
    val rightSize = join.right.stats.sizeInBytes
    val hasEquiJoin = isEquiJoin(join)
    if (hasEquiJoin) {
      if (isCartesianCondition(join.condition)) {
        Some("Cartesian")
      } else if (minSize(leftSize, rightSize) > BigInt((threshold * ratio).toLong)) {
        Some("OversizedBroadcastFallback")
      } else {
        None
      }
    } else {
      if (leftSize > threshold && rightSize > threshold) {
        Some("Cartesian")
      } else if (cannotSelectBuildSide(leftSize, rightSize, threshold)) {
        Some("SecondBNLJ")
      } else {
        None
      }
    }
  }

  private def isEquiJoin(join: Join): Boolean = {
    join match {
      case ExtractEquiJoinKeys(_, _, _, _, _, _, _, _) => true
      case _ => false
    }
  }

  private def isCartesianCondition(condition: Option[Expression]): Boolean = {
    condition.forall(!containsJoinKey(_))
  }

  private def containsJoinKey(expr: Expression): Boolean = {
    expr match {
      case EqualTo(l: AttributeReference, r: AttributeReference) =>
        l.qualifier.nonEmpty && r.qualifier.nonEmpty && l.qualifier != r.qualifier
      case And(l, r) => containsJoinKey(l) || containsJoinKey(r)
      case _ => false
    }
  }

  private def minSize(leftSize: BigInt, rightSize: BigInt): BigInt = {
    if (leftSize <= rightSize) leftSize else rightSize
  }

  private def cannotSelectBuildSide(
      leftSize: BigInt,
      rightSize: BigInt,
      threshold: Long): Boolean = {
    leftSize > threshold && rightSize > threshold
  }
}

object DangerousJoinInterceptor {
  val WARN = "WARN"
  val REJECT = "REJECT"
  val KYUUBI_LOG_KEY = "KYUUBI_LOG_KEY"
}
