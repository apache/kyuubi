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

import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.apache.spark.sql.types.StructField

trait ColumnExtractor extends (AnyRef => Seq[String]) with Extractor

object ColumnExtractor {
  val columnExtractors: Map[String, ColumnExtractor] = {
    ServiceLoader.load(classOf[ColumnExtractor])
      .iterator()
      .asScala
      .map(e => (e.key, e))
      .toMap
  }
}
class StringColumnExtractor extends ColumnExtractor {
  override def apply(v1: AnyRef): Seq[String] = {
    Seq(v1.asInstanceOf[String])
  }
}

class StringSeqColumnExtractor extends ColumnExtractor {
  override def apply(v1: AnyRef): Seq[String] = {
    v1.asInstanceOf[Seq[String]]
  }
}

class StringSeqLastColumnExtractor extends ColumnExtractor {
  override def apply(v1: AnyRef): Seq[String] = {
    v1.asInstanceOf[Seq[String]].takeRight(1)
  }
}

class StringSeqOptionColumnExtractor extends ColumnExtractor {
  override def apply(v1: AnyRef): Seq[String] = {
    v1.asInstanceOf[Option[Seq[String]]].getOrElse(Nil)
  }
}

class StructFieldSeqColumnExtractor extends ColumnExtractor {
  override def apply(v1: AnyRef): Seq[String] = {
    v1.asInstanceOf[Seq[StructField]].map(_.name)
  }
}

class PartitionColumnExtractor extends ColumnExtractor {
  override def apply(v1: AnyRef): Seq[String] = {
    v1.asInstanceOf[Map[String, _]].keySet.toSeq
  }
}

class PartitionOptionColumnExtractor extends ColumnExtractor {
  override def apply(v1: AnyRef): Seq[String] = {
    v1.asInstanceOf[Option[Map[String, _]]].toSeq.flatMap(_.keySet)
  }
}

class PartitionSeqColumnExtractor extends ColumnExtractor {
  override def apply(v1: AnyRef): Seq[String] = {
    v1.asInstanceOf[Seq[Map[String, _]]].flatMap(_.keySet).distinct
  }
}

class PartitionLocsSeqColumnExtractor extends ColumnExtractor {
  override def apply(v1: AnyRef): Seq[String] = {
    v1.asInstanceOf[Seq[(Map[String, _], _)]].flatMap(_._1.keySet).distinct
  }
}
