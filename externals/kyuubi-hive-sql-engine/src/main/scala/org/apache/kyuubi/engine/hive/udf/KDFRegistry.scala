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

package org.apache.kyuubi.engine.hive.udf

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.hive.ql.exec.{FunctionRegistry, UDFArgumentLengthException}
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{PrimitiveObjectInspectorFactory, StringObjectInspector}

import org.apache.kyuubi.{KYUUBI_VERSION, Utils}
import org.apache.kyuubi.config.KyuubiReservedKeys.{KYUUBI_ENGINE_ID, KYUUBI_SESSION_USER_KEY}

object KDFRegistry {

  @transient
  val registeredFunctions = new ArrayBuffer[KyuubiDefinedFunction]()

  val kyuubi_version: KyuubiDefinedFunction = create(
    "kyuubi_version",
    new KyuubiVersionFunction,
    "Return the version of Kyuubi Server",
    "string",
    "1.8.0")

  val engine_name: KyuubiDefinedFunction = create(
    "engine_name",
    new EngineNameFunction,
    "Return the name of engine",
    "string",
    "1.8.0")

  val engine_id: KyuubiDefinedFunction = create(
    "engine_id",
    new EngineIdFunction,
    "Return the id of engine",
    "string",
    "1.8.0")

  val system_user: KyuubiDefinedFunction = create(
    "system_user",
    new SystemUserFunction,
    "Return the system user",
    "string",
    "1.8.0")

  val session_user: KyuubiDefinedFunction = create(
    "session_user",
    new SessionUserFunction,
    "Return the session user",
    "string",
    "1.8.0")

  def create(
      name: String,
      udf: GenericUDF,
      description: String,
      returnType: String,
      since: String): KyuubiDefinedFunction = {
    val kdf = KyuubiDefinedFunction(name, udf, description, returnType, since)
    registeredFunctions += kdf
    kdf
  }

  def registerAll(): Unit = {
    for (func <- registeredFunctions) {
      FunctionRegistry.registerTemporaryUDF(func.name, func.udf.getClass)
    }
  }
}

class KyuubiVersionFunction() extends GenericUDF {
  private val returnOI: StringObjectInspector =
    PrimitiveObjectInspectorFactory.javaStringObjectInspector
  override def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {
    if (arguments.length != 0) {
      throw new UDFArgumentLengthException("The function kyuubi_version() takes no arguments, got "
        + arguments.length)
    }
    returnOI
  }

  override def evaluate(arguments: Array[GenericUDF.DeferredObject]): AnyRef = KYUUBI_VERSION

  override def getDisplayString(children: Array[String]): String = "kyuubi_version()"
}

class EngineNameFunction() extends GenericUDF {
  private val returnOI: StringObjectInspector =
    PrimitiveObjectInspectorFactory.javaStringObjectInspector
  override def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {
    if (arguments.length != 0) {
      throw new UDFArgumentLengthException("The function engine_name() takes no arguments, got "
        + arguments.length)
    }
    returnOI
  }
  override def evaluate(arguments: Array[GenericUDF.DeferredObject]): AnyRef =
    SessionState.get.getConf.get("hive.engine.name", "")
  override def getDisplayString(children: Array[String]): String = "engine_name()"
}

class EngineIdFunction() extends GenericUDF {
  private val returnOI: StringObjectInspector =
    PrimitiveObjectInspectorFactory.javaStringObjectInspector
  override def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {
    if (arguments.length != 0) {
      throw new UDFArgumentLengthException("The function engine_id() takes no arguments, got "
        + arguments.length)
    }
    returnOI
  }

  override def evaluate(arguments: Array[GenericUDF.DeferredObject]): AnyRef =
    SessionState.get.getConf.get(KYUUBI_ENGINE_ID, "")

  override def getDisplayString(children: Array[String]): String = "engine_id()"
}

class SystemUserFunction() extends GenericUDF {
  private val returnOI: StringObjectInspector =
    PrimitiveObjectInspectorFactory.javaStringObjectInspector
  override def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {
    if (arguments.length != 0) {
      throw new UDFArgumentLengthException("The function system_user() takes no arguments, got "
        + arguments.length)
    }
    returnOI
  }

  override def evaluate(arguments: Array[GenericUDF.DeferredObject]): AnyRef = Utils.currentUser

  override def getDisplayString(children: Array[String]): String = "system_user()"
}

class SessionUserFunction() extends GenericUDF {
  private val returnOI: StringObjectInspector =
    PrimitiveObjectInspectorFactory.javaStringObjectInspector
  override def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {
    if (arguments.length != 0) {
      throw new UDFArgumentLengthException("The function session_user() takes no arguments, got "
        + arguments.length)
    }
    returnOI
  }

  override def evaluate(arguments: Array[GenericUDF.DeferredObject]): AnyRef = {
    SessionState.get.getConf.get(KYUUBI_SESSION_USER_KEY, "")
  }

  override def getDisplayString(children: Array[String]): String = "session_user()"
}
