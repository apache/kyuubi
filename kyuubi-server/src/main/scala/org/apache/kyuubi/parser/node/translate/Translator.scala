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

package org.apache.kyuubi.parser.node.translate

import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.engine.EngineType._
import org.apache.kyuubi.parser.node.{LogicalNode, TranslateNode}

object TranslateUtils {
  def translate(node: TranslateNode, engineType: EngineType): String = {
    val translator = engineType match {
      case SPARK_SQL => new SparkTranslator()
      case FLINK_SQL => new FlinkTranslator()
      case TRINO => new TrinoTranslator()
      case HIVE_SQL => new HiveTranslator()
      case JDBC => new JdbcTranslator()
    }

    node.accept(translator)
  }
}

sealed trait Translator {

  def engineType: String

  def visitNode(node: LogicalNode): String = {
    throw new KyuubiException(s"${node.name()} cannot be translated in engine $engineType. ")
  }

  def visitRenameTableNode(node: RenameTableNode): String = visitNode(node)
}

class SparkTranslator extends Translator {
  override def engineType: String = "Spark"

  override def visitRenameTableNode(node: RenameTableNode): String = {
    s"ALTER TABLE ${node.from} RENAME TO ${node.to}"
  }
}

class FlinkTranslator extends Translator {
  override def engineType: String = "Flink"

  override def visitRenameTableNode(node: RenameTableNode): String = {
    s"ALTER TABLE ${node.from} RENAME TO ${node.to}"
  }
}

class TrinoTranslator extends Translator {
  override def engineType: String = "Trino"

  override def visitRenameTableNode(node: RenameTableNode): String = {
    s"ALTER TABLE ${node.from} RENAME TO ${node.to}"
  }
}

class HiveTranslator extends Translator {
  override def engineType: String = "Hive"

  override def visitRenameTableNode(node: RenameTableNode): String = {
    s"ALTER TABLE ${node.from} RENAME TO ${node.to}"
  }
}

class JdbcTranslator extends Translator {
  override def engineType: String = "Jdbc"
}
