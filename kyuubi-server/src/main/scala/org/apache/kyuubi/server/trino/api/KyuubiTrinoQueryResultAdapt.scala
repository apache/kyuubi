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

package org.apache.kyuubi.server.trino.api

import java.net.URI
import java.util

import scala.collection.JavaConverters._

import io.trino.client.{ClientTypeSignature, Column, QueryError, QueryResults, StatementStats, Warning}
import org.apache.hive.service.rpc.thrift.{TGetResultSetMetadataResp, TRowSet}

import org.apache.kyuubi.operation.OperationStatus

object KyuubiTrinoQueryResultAdapt {
  private val defaultWarning: util.List[Warning] = new util.ArrayList[Warning]()
  private val GENERIC_INTERNAL_ERROR_CODE = 65536
  private val GENERIC_INTERNAL_ERROR_NAME = "GENERIC_INTERNAL_ERROR_NAME"
  private val GENERIC_INTERNAL_ERROR_TYPE = "INTERNAL_ERROR"

  def createQueryResults(
      queryId: String,
      nextUri: URI,
      queryHtmlUri: URI,
      queryStatus: OperationStatus,
      columns: Option[TGetResultSetMetadataResp] = None,
      data: Option[TRowSet] = None): QueryResults = {

    //    val queryHtmlUri = uriInfo.getRequestUriBuilder
    //      .replacePath("ui/query.html").replaceQuery(queryId).build()

    new QueryResults(
      queryId,
      queryHtmlUri,
      nextUri,
      nextUri,
      convertTColumn(columns),
      convertTRowSet(data),
      StatementStats.builder.setState(queryStatus.state.name()).build(),
      toQueryError(queryStatus),
      defaultWarning,
      null,
      0L)
  }

  def convertTColumn(columns: Option[TGetResultSetMetadataResp]): util.List[Column] = {
    if (columns.isEmpty) {
      return null
    }

    columns.get.getSchema.getColumns.asScala.map(c => {
      val tp = c.getTypeDesc.getTypes.get(0).getPrimitiveEntry.getType.name()
      new Column(c.getColumnName, tp, new ClientTypeSignature(tp))
    }).toList.asJava
  }

  def convertTRowSet(data: Option[TRowSet]): util.List[util.List[Object]] = {
    if (data.isEmpty) {
      return null
    }
    val rowSet = data.get
    var dataSet: Array[scala.List[Object]] = Array()

    if (rowSet.getColumns == null) {
      return rowSet.getRows.asScala
        .map(t => t.getColVals.asScala.map(v => v.getFieldValue.asInstanceOf[Object]).asJava)
        .asJava
    }

    rowSet.getColumns.asScala.foreach {
      case tColumn if tColumn.isSetBoolVal =>
        val nulls = util.BitSet.valueOf(tColumn.getBoolVal.getNulls)
        if (dataSet.isEmpty) {
          dataSet = tColumn.getBoolVal.getValues.asScala.zipWithIndex
            .foldLeft(Array[scala.List[Object]]()) {
              case (acc, x) if nulls.get(x._2) =>
                acc ++ List(List(None))
              case (acc, x) if !nulls.get(x._2) =>
                acc ++ List(List(x._1))
            }
        } else {
          tColumn.getBoolVal.getValues.asScala.zipWithIndex.foreach {
            case (_, rowIdx) if nulls.get(rowIdx) =>
              dataSet(rowIdx) = dataSet(rowIdx) ++ List(None)
            case (v, rowIdx) =>
              dataSet(rowIdx) = dataSet(rowIdx) ++ List(v)
          }
        }
      case tColumn if tColumn.isSetByteVal =>
        val nulls = util.BitSet.valueOf(tColumn.getByteVal.getNulls)
        if (dataSet.isEmpty) {
          dataSet = tColumn.getByteVal.getValues.asScala.zipWithIndex
            .foldLeft(Array[scala.List[Object]]()) {
              case (acc, x) if nulls.get(x._2) =>
                acc ++ List(scala.List(None))
              case (acc, x) if !nulls.get(x._2) =>
                acc ++ List(scala.List(x._1))
            }
        } else {
          tColumn.getByteVal.getValues.asScala.zipWithIndex.foreach {
            case (_, rowIdx) if nulls.get(rowIdx) =>
              dataSet(rowIdx) = dataSet(rowIdx) ++ List(None)
            case (v, rowIdx) =>
              dataSet(rowIdx) = dataSet(rowIdx) ++ List(v)
          }
        }
      case tColumn if tColumn.isSetI16Val =>
        val nulls = util.BitSet.valueOf(tColumn.getI16Val.getNulls)
        if (dataSet.isEmpty) {
          dataSet = tColumn.getI16Val.getValues.asScala.zipWithIndex
            .foldLeft(Array[scala.List[Object]]()) {
              case (acc, x) if nulls.get(x._2) =>
                acc ++ List(List(None))
              case (acc, x) if !nulls.get(x._2) =>
                acc ++ List(List(x._1))
            }
        } else {
          tColumn.getI16Val.getValues.asScala.zipWithIndex.foreach {
            case (_, rowIdx) if nulls.get(rowIdx) =>
              dataSet(rowIdx) = dataSet(rowIdx) ++ List(None)
            case (v, rowIdx) =>
              dataSet(rowIdx) = dataSet(rowIdx) ++ List(v)
          }
        }
      case tColumn if tColumn.isSetI32Val =>
        val nulls = util.BitSet.valueOf(tColumn.getI32Val.getNulls)
        if (dataSet.isEmpty) {
          dataSet = tColumn.getI32Val.getValues.asScala.zipWithIndex
            .foldLeft(Array[scala.List[Object]]()) {
              case (acc, x) if nulls.get(x._2) =>
                acc ++ List(List(None))
              case (acc, x) if !nulls.get(x._2) =>
                acc ++ List(List(x._1))
            }
        } else {
          tColumn.getI32Val.getValues.asScala.zipWithIndex.foreach {
            case (_, rowIdx) if nulls.get(rowIdx) =>
              dataSet(rowIdx) = dataSet(rowIdx) ++ List(None)
            case (v, rowIdx) =>
              dataSet(rowIdx) = dataSet(rowIdx) ++ List(v)
          }
        }
      case tColumn if tColumn.isSetI64Val =>
        val nulls = util.BitSet.valueOf(tColumn.getI64Val.getNulls)
        if (dataSet.isEmpty) {
          dataSet = tColumn.getI64Val.getValues.asScala.zipWithIndex
            .foldLeft(Array[scala.List[Object]]()) {
              case (acc, x) if nulls.get(x._2) =>
                acc ++ List(List(None))
              case (acc, x) if !nulls.get(x._2) =>
                acc ++ List(List(x._1))
            }
        } else {
          tColumn.getI64Val.getValues.asScala.zipWithIndex.foreach {
            case (_, rowIdx) if nulls.get(rowIdx) =>
              dataSet(rowIdx) = dataSet(rowIdx) ++ List(None)
            case (v, rowIdx) =>
              dataSet(rowIdx) = dataSet(rowIdx) ++ List(v)
          }
        }
      case tColumn if tColumn.isSetDoubleVal =>
        val nulls = util.BitSet.valueOf(tColumn.getDoubleVal.getNulls)
        if (dataSet.isEmpty) {
          dataSet = tColumn.getDoubleVal.getValues.asScala.zipWithIndex
            .foldLeft(Array[scala.List[Object]]()) {
              case (acc, x) if nulls.get(x._2) =>
                acc ++ List(List(None))
              case (acc, x) if !nulls.get(x._2) =>
                acc ++ List(List(x._1))
            }
        } else {
          tColumn.getDoubleVal.getValues.asScala.zipWithIndex.foreach {
            case (_, rowIdx) if nulls.get(rowIdx) =>
              dataSet(rowIdx) = dataSet(rowIdx) ++ List(None)
            case (v, rowIdx) =>
              dataSet(rowIdx) = dataSet(rowIdx) ++ List(v)
          }
        }
      case tColumn if tColumn.isSetBinaryVal =>
        val nulls = util.BitSet.valueOf(tColumn.getBinaryVal.getNulls)
        if (dataSet.isEmpty) {
          dataSet = tColumn.getBinaryVal.getValues.asScala.zipWithIndex
            .foldLeft(Array[scala.List[Object]]()) {
              case (acc, x) if nulls.get(x._2) =>
                acc ++ List(List(None))
              case (acc, x) if !nulls.get(x._2) =>
                acc ++ List(List(x._1))
            }
        } else {
          tColumn.getBinaryVal.getValues.asScala.zipWithIndex.foreach {
            case (_, rowIdx) if nulls.get(rowIdx) =>
              dataSet(rowIdx) = dataSet(rowIdx) ++ List(None)
            case (v, rowIdx) =>
              dataSet(rowIdx) = dataSet(rowIdx) ++ List(v)
          }
        }
      case tColumn =>
        val nulls = util.BitSet.valueOf(tColumn.getStringVal.getNulls)
        if (dataSet.isEmpty) {
          dataSet = tColumn.getStringVal.getValues.asScala.zipWithIndex
            .foldLeft(Array[scala.List[Object]]()) {
              case (acc, x) if nulls.get(x._2) =>
                acc ++ List(List(None))
              case (acc, x) if !nulls.get(x._2) =>
                acc ++ List(List(x._1))
            }
        } else {
          tColumn.getStringVal.getValues.asScala.zipWithIndex.foreach {
            case (_, rowIdx) if nulls.get(rowIdx) =>
              dataSet(rowIdx) = dataSet(rowIdx) ++ List(None)
            case (v, rowIdx) =>
              dataSet(rowIdx) = dataSet(rowIdx) ++ List(v)
          }
        }
    }
    dataSet.toList.map(_.asJava).asJava
  }

  def toQueryError(queryStatus: OperationStatus): QueryError = {
    val exception = queryStatus.exception
    if (exception.isEmpty) {
      null
    } else {
      new QueryError(
        exception.get.getMessage,
        queryStatus.state.name(),
        GENERIC_INTERNAL_ERROR_CODE,
        GENERIC_INTERNAL_ERROR_NAME,
        GENERIC_INTERNAL_ERROR_TYPE,
        null,
        null)
    }
  }
}
