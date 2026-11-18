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

package org.apache.kyuubi.plugin.spark.authz.ranger

import java.io.File

import scala.collection.JavaConverters._

import org.apache.commons.lang3.StringUtils
import org.apache.ranger.authz.model.RangerResourceInfo

import org.apache.kyuubi.plugin.spark.authz.{AccessControlException, ObjectType, PrivilegeObject}
import org.apache.kyuubi.plugin.spark.authz.ObjectType._
import org.apache.kyuubi.plugin.spark.authz.OperationType.OperationType

/**
 * A privilege object to authorize, which is converted to a Ranger resource
 * (e.g. "table:default/src" or "column:default/src/id") in requests to
 * the Ranger authorizer.
 *
 * @param objectType the type of the object
 * @param database   the database name, or null if not applicable
 * @param table      the table name, or null if not applicable
 * @param columns    the column names, or empty if not applicable
 * @param udf        the function name, or null if not applicable
 * @param uri        the uri path, or null if not applicable
 * @param owner      the owner of the object, if any
 * @param catalog    the catalog name, if any
 */
case class AccessResource private[ranger] (
    objectType: ObjectType,
    database: String,
    table: String,
    columns: Seq[String],
    udf: String,
    uri: String,
    owner: Option[String],
    catalog: Option[String]) {

  def getDatabase: String = database
  def getUdf: String = udf
  def getTable: String = table
  def getColumn: String = columns.headOption.orNull

  def getColumns: Seq[String] = columns

  def getOwnerUser: String = owner.orNull

  /**
   * The path-like representation of this resource used in error messages,
   * e.g. "default/src" for a table, "default/src/id" for a column.
   */
  def getAsString: String = objectType match {
    case COLUMN =>
      (Seq(database, table).filter(_ != null) ++ columns).mkString("/")
    case FUNCTION =>
      Seq(database, udf).filter(_ != null).mkString("/")
    case URI =>
      // the uri is matched against both the exact path and the path with a
      // trailing slash, as the legacy plugin did; the root path has no second variant
      val path = Option(uri).map(normalizeUriPath).getOrElse("")
      if (path == File.separator) s"[$path]" else s"[$path, $path/]"
    case _ =>
      Seq(database, table).filter(_ != null).mkString("/")
  }

  private[ranger] def toResourceInfos: Seq[RangerResourceInfo] = {
    val attributes = owner.map(o => java.util.Collections.singletonMap("OWNER", o: AnyRef)).orNull
    objectType match {
      case DATABASE =>
        Seq(new RangerResourceInfo(
          s"database:${requireRrnComponent(database, "database")}",
          null,
          null,
          attributes))
      case FUNCTION =>
        // An unqualified function reference (e.g. a built-in or temporary function) has no
        // database. The legacy plugin left the database blank, and blank values in the legacy
        // resource matched the wildcard values in policies, so keep the wildcard marker for
        // the blank database. Blank names in other resource levels indicate a broken command
        // extraction, which matched no policy in the legacy resource, so deny them.
        val db = if (StringUtils.isBlank(database)) "*" else escapeRrnMetaChars(database)
        Seq(new RangerResourceInfo(
          s"udf:$db/${requireRrnComponent(udf, "udf")}",
          null,
          null,
          attributes))
      case COLUMN =>
        val db = requireRrnComponent(database, "database")
        val tableName = requireRrnComponent(table, "table")
        columns match {
          case Seq() =>
            // A COLUMN request must name a column, otherwise the RRN is incomplete
            // and would hide a data mask from the top-level result.
            requireRrnComponent(null, "column")
            Seq.empty[RangerResourceInfo]
          case Seq(column) =>
            // A column name may contain a comma. Treat it as a single resource value
            // instead of splitting it on the comma.
            Seq(new RangerResourceInfo(
              s"column:$db/$tableName/${requireRrnComponent(column, "column")}",
              null,
              null,
              attributes))
          case _ =>
            // Multiple columns are represented as a table parent with column
            // sub-resources, which is how Ranger models multi-column requests.
            val subResources = columns
              .map(column => s"column:${requireRrnComponent(column, "column")}")
              .toSet.asJava
            Seq(new RangerResourceInfo(
              s"table:$db/$tableName",
              subResources,
              null,
              attributes))
        }
      case URI =>
        // Url policies may be written with or without a trailing slash, and the legacy
        // plugin matched a uri against both the exact path and the path with a trailing
        // slash. The RRN request carries a single resource value set, so the two variants
        // are returned and authorized as alternatives by separate requests.
        val normalized = Option(uri).map(normalizeUriPath).orNull
        val path = requireRrnComponent(normalized, "uri")
        if (normalized == File.separator) {
          Seq(new RangerResourceInfo(s"url:$path", null, null, attributes))
        } else {
          Seq(
            new RangerResourceInfo(s"url:$path", null, null, attributes),
            new RangerResourceInfo(s"url:$path/", null, null, attributes))
        }
      case VIEW =>
        // A local temporary view has no database in the SHOW TABLES output. The legacy
        // plugin left the database blank, and blank values in the legacy resource matched
        // the wildcard values in policies, so keep the wildcard marker for the blank
        // database, as the unqualified function reference case does.
        val db = if (StringUtils.isBlank(database)) "*" else escapeRrnMetaChars(database)
        Seq(new RangerResourceInfo(
          s"table:$db/${requireRrnComponent(table, "table")}",
          null,
          null,
          attributes))
      case _ =>
        Seq(new RangerResourceInfo(
          s"table:${requireRrnComponent(database, "database")}" +
            s"/${requireRrnComponent(table, "table")}",
          null,
          null,
          attributes))
    }
  }

  /**
   * Escapes the RRN metacharacters in a resource name, so that a name containing
   * them is parsed as a single resource level instead of breaking the resource
   * hierarchy, e.g. a table named "a/b" becomes "a\/b" in the resource name
   * "table:default/a\/b". The escape format follows RangerResourceNameParser:
   * a backslash escapes the next character, so "\\" stands for a literal
   * backslash and "\/" stands for a literal separator.
   */
  private def escapeRrnMetaChars(value: String): String =
    value.replace("\\", "\\\\").replace("/", "\\/")

  private def normalizeUriPath(path: String): String =
    if (path == File.separator) path else path.stripSuffix(File.separator)

  /**
   * Returns the RRN component for the given resource name, throwing an access
   * control exception for a blank name. The RRN parser rejects blank resource
   * values, and the legacy plugin had no value for a blank name, which matched
   * no policy, so the access is denied rather than being evaluated against the
   * wildcard marker.
   */
  private def requireRrnComponent(value: String, name: String): String =
    if (StringUtils.isBlank(value)) {
      throw new AccessControlException(
        s"Access denied: invalid [$objectType] resource, blank $name")
    } else {
      escapeRrnMetaChars(value)
    }
}

object AccessResource {

  def apply(
      objectType: ObjectType,
      firstLevelResource: String,
      secondLevelResource: String,
      thirdLevelResource: String,
      owner: Option[String] = None,
      catalog: Option[String] = None): AccessResource = objectType match {
    case DATABASE =>
      new AccessResource(DATABASE, firstLevelResource, null, Nil, null, null, owner, catalog)
    case FUNCTION =>
      new AccessResource(
        FUNCTION,
        Option(firstLevelResource).getOrElse(""),
        null,
        Nil,
        secondLevelResource,
        null,
        owner,
        catalog)
    case COLUMN =>
      new AccessResource(
        COLUMN,
        firstLevelResource,
        secondLevelResource,
        Option(thirdLevelResource).map(Seq(_)).getOrElse(Nil),
        null,
        null,
        owner,
        catalog)
    case TABLE | VIEW | INDEX =>
      new AccessResource(
        objectType,
        firstLevelResource,
        secondLevelResource,
        Nil,
        null,
        null,
        owner,
        catalog)
    case URI =>
      new AccessResource(URI, null, null, Nil, null, firstLevelResource, owner, catalog)
  }

  /**
   * Creates a resource for multiple columns.
   */
  private[ranger] def forColumns(
      firstLevelResource: String,
      secondLevelResource: String,
      columns: Seq[String],
      owner: Option[String] = None,
      catalog: Option[String] = None): AccessResource = {
    new AccessResource(
      COLUMN,
      firstLevelResource,
      secondLevelResource,
      columns,
      null,
      null,
      owner,
      catalog)
  }

  def apply(
      objectType: ObjectType,
      firstLevelResource: String,
      catalog: Option[String]): AccessResource = {
    apply(objectType, firstLevelResource, null, null, catalog = catalog)
  }

  def apply(
      obj: PrivilegeObject,
      opType: OperationType): AccessResource = {
    val objectType = ObjectType(obj, opType)
    objectType match {
      case COLUMN =>
        forColumns(obj.dbname, obj.objectName, obj.columns, obj.owner, obj.catalog)
      case _ =>
        apply(
          objectType,
          obj.dbname,
          obj.objectName,
          obj.columns.mkString(","),
          obj.owner,
          obj.catalog)
    }
  }
}
