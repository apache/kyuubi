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

package org.apache.spark.sql.kyuubi

import java.io.{ByteArrayOutputStream, OutputStream}
import java.time.ZoneId
import java.util.zip.GZIPOutputStream

import com.github.luben.zstd.ZstdOutputStreamNoFinalizer
import net.jpountz.lz4.LZ4FrameOutputStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}
import org.xerial.snappy.SnappyOutputStream

import org.apache.kyuubi.engine.spark.schema.RowSet

object SparkDatasetHelper {
  def toArrowBatchRdd[T](ds: Dataset[T], compressionCodec: Option[String]): RDD[Array[Byte]] = {
    compressionCodec.map { codec =>
      ds.toArrowBatchRdd.map(CompressionCodecFactory.createCodec(codec).compress)
    }.getOrElse(ds.toArrowBatchRdd)
  }

  def convertTopLevelComplexTypeToHiveString(df: DataFrame): DataFrame = {
    val timeZone = ZoneId.of(df.sparkSession.sessionState.conf.sessionLocalTimeZone)

    val quotedCol = (name: String) => col(quoteIfNeeded(name))

    // an udf to call `RowSet.toHiveString` on complex types(struct/array/map).
    val toHiveStringUDF = udf[String, Row, String]((row, schemaDDL) => {
      val dt = DataType.fromDDL(schemaDDL)
      dt match {
        case StructType(Array(StructField(_, st: StructType, _, _))) =>
          RowSet.toHiveString((row, st), timeZone)
        case StructType(Array(StructField(_, at: ArrayType, _, _))) =>
          RowSet.toHiveString((row.toSeq.head, at), timeZone)
        case StructType(Array(StructField(_, mt: MapType, _, _))) =>
          RowSet.toHiveString((row.toSeq.head, mt), timeZone)
        case _ =>
          throw new UnsupportedOperationException
      }
    })

    val cols = df.schema.map {
      case sf @ StructField(name, _: StructType, _, _) =>
        toHiveStringUDF(quotedCol(name), lit(sf.toDDL)).as(name)
      case sf @ StructField(name, (_: MapType | _: ArrayType), _, _) =>
        toHiveStringUDF(struct(quotedCol(name)), lit(sf.toDDL)).as(name)
      case StructField(name, _, _, _) => quotedCol(name)
    }
    df.select(cols: _*)
  }

  /**
   * Fork from Apache Spark-3.3.1 org.apache.spark.sql.catalyst.util.quoteIfNeeded to adapt to
   * Spark-3.1.x
   */
  def quoteIfNeeded(part: String): String = {
    if (part.matches("[a-zA-Z0-9_]+") && !part.matches("\\d+")) {
      part
    } else {
      s"`${part.replace("`", "``")}`"
    }
  }
}

trait CompressionCodec extends Serializable {
  def outputStream(baos: ByteArrayOutputStream): OutputStream
  def compress(byteArray: Array[Byte]): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    var os: OutputStream = null
    try {
      os = outputStream(baos)
      os.write(byteArray)
    } finally {
      if (os != null) {
        os.close()
      }
    }
    baos.toByteArray
  }
}

object CompressionCodecFactory {
  def createCodec(tpe: String): CompressionCodec = {
    tpe match {
      case "lz4" => Lz4CompressionCodec
      case "zstd" => ZstdCompressionCodec
      case "snappy" => SnappyCompressionCodec
      case "gzip" => GZIPCompressionCodec
      case _ => NoCompressionCodec
    }
  }
}

object NoCompressionCodec extends CompressionCodec {
  override def outputStream(baos: ByteArrayOutputStream): OutputStream = {
    throw new UnsupportedOperationException()
  }
  override def compress(byteArray: Array[Byte]): Array[Byte] = byteArray
}

object Lz4CompressionCodec extends CompressionCodec {
  override def outputStream(baos: ByteArrayOutputStream): OutputStream =
    new LZ4FrameOutputStream(baos)
}

object ZstdCompressionCodec extends CompressionCodec {
  override def outputStream(baos: ByteArrayOutputStream): OutputStream =
    new ZstdOutputStreamNoFinalizer(baos)
}

object SnappyCompressionCodec extends CompressionCodec {
  override def outputStream(baos: ByteArrayOutputStream): OutputStream =
    new SnappyOutputStream(baos)
}

object GZIPCompressionCodec extends CompressionCodec {
  override def outputStream(baos: ByteArrayOutputStream): OutputStream =
    new GZIPOutputStream(baos)
}
