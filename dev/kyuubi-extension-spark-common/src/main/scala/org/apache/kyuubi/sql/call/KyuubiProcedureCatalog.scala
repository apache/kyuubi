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

package org.apache.kyuubi.sql.call

import java.util.Locale

import scala.collection.{mutable, Seq}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.reflections.Reflections

import org.apache.kyuubi.sql.KyuubiSQLExtensionException
import org.apache.kyuubi.sql.call.procedure.{Procedure, ProcedureCatalog, ProcedureParameter}

case class KyuubiProcedureCatalog() extends ProcedureCatalog {
  val procedureCatalog: mutable.Map[Identifier, Procedure] = mutable.Map.empty

  override def loadProcedure(ident: Identifier): Procedure = {
    procedureCatalog.get(ident) match {
      case Some(proc) => proc
      case _ => throw new KyuubiSQLExtensionException(s"Kyuubi procedure ${ident.name()} not found")
    }
  }

  override def initialize(s: String, caseInsensitiveStringMap: CaseInsensitiveStringMap): Unit = {
    val procedures = scanKyuubiProcs
    procedures.foreach { proc =>
      procedureCatalog.put(proc.getIdentifier, proc)
    }
  }

  def scanKyuubiProcs: Seq[Procedure] = {
    val reflections = new Reflections("org.apache.kyuubi")
    val classInfos = reflections.getSubTypesOf(classOf[Procedure])
    val procedures = classInfos.map { classInfo =>
      Class.forName(classInfo.getCanonicalName).newInstance.asInstanceOf[Procedure]
    }
    procedures.toSeq
  }

  override def name(): String = classOf[KyuubiProcedureCatalog].getSimpleName
}

object KyuubiProcedureCatalog {
  val kyuubiCatalog = new KyuubiProcedureCatalog
  kyuubiCatalog.initialize("", CaseInsensitiveStringMap.empty())

  def toIdentifier(names: Seq[String]): Identifier = {
    val n = names.length
    val ns = names.take(n - 1).toArray
    val name = names.last
    Identifier.of(ns, name)
  }

  def validateParams(params: Seq[ProcedureParameter]): Unit = {
    // should not be any duplicate param names
    val duplicateParamNames = params.groupBy(_.name).collect {
      case (name, matchingParams) if matchingParams.length > 1 => name
    }

    if (duplicateParamNames.nonEmpty) {
      throw new KyuubiSQLExtensionException(
        s"Duplicate parameter names: ${duplicateParamNames.mkString("[", ",", "]")}")
    }

    // optional params should be at the end
    params.sliding(2).foreach {
      case Seq(previousParam, currentParam) if !previousParam.required && currentParam.required =>
        throw new KyuubiSQLExtensionException(
          s"Optional params must after required ones but $currentParam is after $previousParam")
      case _ =>
    }
  }

  def normalizeParams(params: Seq[ProcedureParameter]): Seq[ProcedureParameter] = {
    params.map {
      case param if param.required =>
        val normalizedName = param.name.toLowerCase(Locale.ROOT)
        ProcedureParameter.required(normalizedName, param.dataType)
      case param =>
        val normalizedName = param.name.toLowerCase(Locale.ROOT)
        ProcedureParameter.optional(normalizedName, param.dataType)
    }
  }

  def normalizeArgs(args: Seq[CallArgument]): Seq[CallArgument] = {
    args.map {
      case a @ NamedArgument(name, _) => a.copy(name = name.toLowerCase(Locale.ROOT))
      case other => other
    }
  }

  def buildArgExprs(params: Seq[ProcedureParameter], args: Seq[CallArgument]): Seq[Expression] = {
    // build a map of declared parameter names to their positions
    val nameToPositionMap = params.map(_.name).zipWithIndex.toMap

    // build a map of parameter names to args
    val nameToArgMap = buildNameToArgMap(params, args, nameToPositionMap)

    // verify all required parameters are provided
    val missingParamNames = params.filter(_.required).collect {
      case param if !nameToArgMap.contains(param.name) => param.name
    }

    if (missingParamNames.nonEmpty) {
      throw new KyuubiSQLExtensionException(
        s"Missing required parameters: ${missingParamNames.mkString("[", ",", "]")}")
    }

    val argExprs = new Array[Expression](params.size)

    nameToArgMap.foreach { case (name, arg) =>
      val position = nameToPositionMap(name)
      argExprs(position) = arg.expr
    }

    // assign nulls to optional params that were not set
    params.foreach {
      case p if !p.required && !nameToArgMap.contains(p.name) =>
        val position = nameToPositionMap(p.name)
        argExprs(position) = Literal.create(null, p.dataType)
      case _ =>
    }

    argExprs
  }

  private def buildNameToArgMap(
      params: Seq[ProcedureParameter],
      args: Seq[CallArgument],
      nameToPositionMap: Map[String, Int]): Map[String, CallArgument] = {

    val containsNamedArg = args.exists(_.isInstanceOf[NamedArgument])
    val containsPositionalArg = args.exists(_.isInstanceOf[PositionalArgument])

    if (containsNamedArg && containsPositionalArg) {
      throw new KyuubiSQLExtensionException("Named and positional arguments cannot be mixed")
    }

    if (containsNamedArg) {
      buildNameToArgMapUsingNames(args, nameToPositionMap)
    } else {
      buildNameToArgMapUsingPositions(args, params)
    }
  }

  private def buildNameToArgMapUsingNames(
      args: Seq[CallArgument],
      nameToPositionMap: Map[String, Int]): Map[String, CallArgument] = {
    val namedArgs = args.asInstanceOf[Seq[NamedArgument]]

    val validationErrors = namedArgs.groupBy(_.name).collect {
      case (name, matchingArgs) if matchingArgs.size > 1 => s"Duplicate procedure argument: $name"
      case (name, _) if !nameToPositionMap.contains(name) => s"Unknown argument: $name"
    }

    if (validationErrors.nonEmpty) {
      throw new KyuubiSQLExtensionException(
        s"Could not build name to arg map: ${validationErrors.mkString(", ")}")
    }

    namedArgs.map(arg => arg.name -> arg).toMap
  }

  private def buildNameToArgMapUsingPositions(
      args: Seq[CallArgument],
      params: Seq[ProcedureParameter]): Map[String, CallArgument] = {

    if (args.size > params.size) {
      throw new KyuubiSQLExtensionException("Too many arguments for procedure")
    }

    args.zipWithIndex.map { case (arg, position) =>
      val param = params(position)
      param.name -> arg
    }.toMap
  }
}
