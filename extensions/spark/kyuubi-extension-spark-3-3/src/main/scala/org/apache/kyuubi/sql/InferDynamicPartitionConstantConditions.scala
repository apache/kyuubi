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

package org.apache.kyuubi.sql

import scala.annotation.tailrec

import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, Cast, EqualTo, Expression, In, Literal, NamedExpression, Or}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical._

object InferDynamicPartitionConstantConditions {

  def infer(
      dynamicPartitionColumns: Seq[Attribute],
      query: LogicalPlan): Map[Attribute, Seq[Literal]] = {
    extractAttributeConditions(query).flatMap {
      case (k, v) =>
        dynamicPartitionColumns.find(c => c.exprId == k.exprId && c.name == k.name).map(_ -> v)
      case _ => None
    }
  }

  private def extractAttributeConditions(
      plan: LogicalPlan): Map[Attribute, Seq[Literal]] = {
    plan match {
      case p: Project =>
        val aliasMap = getAliasMap(p.projectList)
        extractAttributeConditions(p.child).map {
          case (attr, literals) =>
            aliasMap.getOrElse(attr, attr) -> literals
        }
      case f: Filter =>
        val attributeConditions = getExprAttributeConditions(f.condition)
        attributeConditions ++ extractAttributeConditions(f.child).map {
          case (k, v) =>
            k -> (v ++ attributeConditions.getOrElse(k, Nil))
        }
      case agg: Aggregate =>
        val aliasMap = getAliasMap(agg.aggregateExpressions)
        extractAttributeConditions(agg.child).map {
          case (attr, literals) =>
            aliasMap.getOrElse(attr, attr) -> literals
        }
      case ExtractEquiJoinKeys(_, _, _, _, _, leftChild, rightChild, _) =>
        val leftAttributeConditions = extractAttributeConditions(leftChild)
        val rightAttributeConditions = extractAttributeConditions(rightChild)
        leftAttributeConditions ++ rightAttributeConditions.map {
          case (k, v) =>
            k -> (v ++ leftAttributeConditions.getOrElse(k, Nil))
        }
      case p: Union =>
        p.children.map(child => {
          (child.output, extractAttributeConditions(child))
        }).reduce { (left, right) =>
          val leftAttributeConditions = left._2
          val rightAttributeConditions = right._2
          val v = p.output.zip(left._1).zip(right._1).flatMap {
            case ((o, l), r) =>
              if (leftAttributeConditions.contains(l) && rightAttributeConditions.contains(r)) {
                Some(o -> (leftAttributeConditions(l) ++ rightAttributeConditions(r)))
              } else {
                None
              }
          }.toMap
          (p.output, v)
        }._2

      case p: Generate =>
        val childAttributeConditions = extractAttributeConditions(p.child)
        if (p.generator.deterministic && p.generator.references.size == 1) {
          val singleRef = p.generator.references.head
          val outputs = p.generatorOutput
          childAttributeConditions.flatMap {
            case (k, v) if k == singleRef =>
              outputs.map(o => o -> v)
            case o => Some(o)
          }
        } else {
          childAttributeConditions
        }
      // TODO: support more UnaryNode like: Expand, Window
      case p: UnaryNode => extractAttributeConditions(p.child)
      case _ => Map.empty
    }
  }

  private def getAliasMap(named: Seq[NamedExpression]): Map[Attribute, Attribute] = {
    @tailrec
    def throughUnary(e: Expression): Option[Attribute] = e match {
      case a: Attribute => Some(a)
      case u: Expression if u.deterministic && u.references.size == 1 =>
        throughUnary(u.references.head)
      case _ => None
    }

    named.flatMap {
      case alias @ Alias(child, _) =>
        throughUnary(child).map(a => (a, alias.toAttribute))
      case _ => None
    }.toMap
  }

  private def getExprAttributeConditions(condition: Expression): Map[Attribute, Seq[Literal]] = {
    object ExtractAttribute {
      @scala.annotation.tailrec
      def unapply(expr: Expression): Option[Attribute] = {
        expr match {
          case attr: Attribute => Some(attr)
          case Cast(child, _, _, _) => unapply(child)
          case _ => None
        }
      }
    }

    object ConstantConditions {
      def unapply(expr: Expression): Option[(Attribute, Seq[Literal])] = expr match {
        case EqualTo(ExtractAttribute(attr), value: Literal) =>
          Some((attr, Seq(value)))
        case EqualTo(value: Literal, ExtractAttribute(attr)) =>
          Some((attr, Seq(value)))
        case In(ExtractAttribute(attr), values: Seq[Expression])
            if values.forall(_.isInstanceOf[Literal]) =>
          Some((attr, values.asInstanceOf[Seq[Literal]]))
        case _ => None
      }
    }

    def getConstantConditions(expr: Expression): Map[Attribute, Seq[Literal]] = {
      expr match {
        case And(left, right) =>
          val leftConstantConditions = getConstantConditions(left)
          val rightConstantConditions = getConstantConditions(right)
          leftConstantConditions ++ rightConstantConditions.map {
            case (k, v) =>
              k -> v.intersect(leftConstantConditions.getOrElse(k, Nil))
          }
        case Or(left, right) =>
          val leftConstantConditions = getConstantConditions(left)
          val rightConstantConditions = getConstantConditions(right)
          leftConstantConditions ++ rightConstantConditions.map {
            case (k, v) =>
              k -> (v ++ leftConstantConditions.getOrElse(k, Nil))
          }
        case ConstantConditions(attr, values) => Map(attr -> values)
        case _ => Map.empty
      }
    }

    getConstantConditions(condition)
  }
}
