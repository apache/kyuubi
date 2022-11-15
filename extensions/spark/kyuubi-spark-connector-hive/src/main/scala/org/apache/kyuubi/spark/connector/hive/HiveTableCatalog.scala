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

package org.apache.kyuubi.spark.connector.hive

import java.net.URI
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{SQLConfHelper, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchDatabaseException, NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.quoteIfNeeded
import org.apache.spark.sql.connector.catalog.{Identifier, NamespaceChange, SupportsNamespaces, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.catalog.NamespaceChange.RemoveProperty
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.hive.HiveUDFExpressionBuilder
import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper.{catalogV2Util, postExternalCatalogEvent, HiveMetastoreCatalog, HiveSessionCatalog}
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog.{toCatalogDatabase, CatalogDatabaseHelper, IdentifierHelper, NamespaceHelper}

/**
 * A [[TableCatalog]] that wrap HiveExternalCatalog to as V2 CatalogPlugin instance to access Hive.
 */
class HiveTableCatalog(sparkSession: SparkSession)
  extends TableCatalog with SQLConfHelper with SupportsNamespaces with Logging {

  def this() = this(SparkSession.active)

  private val externalCatalogManager = ExternalCatalogManager.getOrCreate(sparkSession)

  private val sc = sparkSession.sparkContext

  private val sessionState = sparkSession.sessionState

  private var catalogName: String = _

  private var catalogOptions: CaseInsensitiveStringMap = _

  var catalog: HiveSessionCatalog = _

  val NAMESPACE_RESERVED_PROPERTIES =
    Seq(
      SupportsNamespaces.PROP_COMMENT,
      SupportsNamespaces.PROP_LOCATION,
      SupportsNamespaces.PROP_OWNER)

  private lazy val hadoopConf: Configuration = {
    val conf = sparkSession.sessionState.newHadoopConf()
    catalogOptions.asScala.foreach {
      case (k, v) => conf.set(k, v)
      case _ =>
    }
    conf
  }

  private lazy val sparkConf: SparkConf = {
    val conf = sparkSession.sparkContext.getConf
    catalogOptions.asScala.foreach {
      case (k, v) => conf.set(k, v)
    }
    conf
  }

  def hadoopConfiguration(): Configuration = hadoopConf

  override def name(): String = {
    require(catalogName != null, "The Hive table catalog is not initialed")
    catalogName
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    assert(catalogName == null, "The Hive table catalog is already initialed.")
    assert(
      conf.getConf(CATALOG_IMPLEMENTATION) == "hive",
      s"Require setting ${CATALOG_IMPLEMENTATION.key} to `hive` to enable hive support.")
    catalogName = name
    catalogOptions = options
    catalog = new HiveSessionCatalog(
      externalCatalogBuilder = () => externalCatalog,
      globalTempViewManagerBuilder = () => sparkSession.sharedState.globalTempViewManager,
      metastoreCatalog = new HiveMetastoreCatalog(sparkSession),
      functionRegistry = sessionState.functionRegistry,
      tableFunctionRegistry = sessionState.tableFunctionRegistry,
      hadoopConf = hadoopConf,
      parser = sessionState.sqlParser,
      functionResourceLoader = sessionState.resourceLoader,
      HiveUDFExpressionBuilder)
  }

  /**
   * A catalog that interacts with external systems.
   */
  lazy val externalCatalog: ExternalCatalogWithListener = {
    val externalCatalog = externalCatalogManager.take(Ticket(catalogName, sparkConf, hadoopConf))

    // Wrap to provide catalog events
    val wrapped = new ExternalCatalogWithListener(externalCatalog)

    // Make sure we propagate external catalog events to the spark listener bus
    wrapped.addListener((event: ExternalCatalogEvent) => postExternalCatalogEvent(sc, event))

    wrapped
  }

  override val defaultNamespace: Array[String] = Array("default")

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    namespace match {
      case Array(db) =>
        catalog
          .listTables(db)
          .map(ident => Identifier.of(ident.database.map(Array(_)).getOrElse(Array()), ident.table))
          .toArray
      case _ =>
        throw new NoSuchNamespaceException(namespace)
    }
  }

  override def loadTable(ident: Identifier): Table = {
    HiveTable(sparkSession, catalog.getTableMetadata(ident.asTableIdentifier), this)
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper.TransformHelper
    val (partitionColumns, maybeBucketSpec) = partitions.toSeq.convertTransforms
    val provider = properties.getOrDefault(TableCatalog.PROP_PROVIDER, conf.defaultDataSourceName)
    val tableProperties = properties.asScala
    val location = Option(properties.get(TableCatalog.PROP_LOCATION))
    val storage = DataSource.buildStorageFormatFromOptions(toOptions(tableProperties.toMap))
      .copy(locationUri = location.map(CatalogUtils.stringToURI))
    val isExternal = properties.containsKey(TableCatalog.PROP_EXTERNAL)
    val tableType =
      if (isExternal || location.isDefined) {
        CatalogTableType.EXTERNAL
      } else {
        CatalogTableType.MANAGED
      }

    val tableDesc = CatalogTable(
      identifier = ident.asTableIdentifier,
      tableType = tableType,
      storage = storage,
      schema = schema,
      provider = Some(provider),
      partitionColumnNames = partitionColumns,
      bucketSpec = maybeBucketSpec,
      properties = tableProperties.toMap,
      tracksPartitionsInCatalog = conf.manageFilesourcePartitions,
      comment = Option(properties.get(TableCatalog.PROP_COMMENT)))

    try {
      catalog.createTable(tableDesc, ignoreIfExists = false)
    } catch {
      case _: TableAlreadyExistsException =>
        throw new TableAlreadyExistsException(ident)
    }

    loadTable(ident)
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    val catalogTable =
      try {
        catalog.getTableMetadata(ident.asTableIdentifier)
      } catch {
        case _: NoSuchTableException =>
          throw new NoSuchTableException(ident)
      }

    val properties = catalogV2Util.applyPropertiesChanges(catalogTable.properties, changes)
    val schema = catalogV2Util.applySchemaChanges(
      catalogTable.schema,
      changes)
    val comment = properties.get(TableCatalog.PROP_COMMENT)
    val owner = properties.getOrElse(TableCatalog.PROP_OWNER, catalogTable.owner)
    val location = properties.get(TableCatalog.PROP_LOCATION).map(CatalogUtils.stringToURI)
    val storage =
      if (location.isDefined) {
        catalogTable.storage.copy(locationUri = location)
      } else {
        catalogTable.storage
      }

    try {
      catalog.alterTable(
        catalogTable.copy(
          properties = properties,
          schema = schema,
          owner = owner,
          comment = comment,
          storage = storage))
    } catch {
      case _: NoSuchTableException =>
        throw new NoSuchTableException(ident)
    }

    loadTable(ident)
  }

  override def dropTable(ident: Identifier): Boolean = {
    try {
      if (loadTable(ident) != null) {
        catalog.dropTable(
          ident.asTableIdentifier,
          ignoreIfNotExists = true,
          purge = true /* skip HDFS trash */ )
        true
      } else {
        false
      }
    } catch {
      case _: NoSuchTableException =>
        false
    }
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    if (tableExists(newIdent)) {
      throw new TableAlreadyExistsException(newIdent)
    }

    // Load table to make sure the table exists
    loadTable(oldIdent)
    catalog.renameTable(oldIdent.asTableIdentifier, newIdent.asTableIdentifier)
  }

  private def toOptions(properties: Map[String, String]): Map[String, String] = {
    properties.filterKeys(_.startsWith(TableCatalog.OPTION_PREFIX)).map {
      case (key, value) => key.drop(TableCatalog.OPTION_PREFIX.length) -> value
    }.toMap
  }

  override def listNamespaces(): Array[Array[String]] = {
    catalog.listDatabases().map(Array(_)).toArray
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    namespace match {
      case Array() =>
        listNamespaces()
      case Array(db) if catalog.databaseExists(db) =>
        Array()
      case _ =>
        throw new NoSuchNamespaceException(namespace)
    }
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    namespace match {
      case Array(db) =>
        try {
          catalog.getDatabaseMetadata(db).toMetadata
        } catch {
          case _: NoSuchDatabaseException =>
            throw new NoSuchNamespaceException(namespace)
        }

      case _ =>
        throw new NoSuchNamespaceException(namespace)
    }
  }

  override def createNamespace(
      namespace: Array[String],
      metadata: util.Map[String, String]): Unit = namespace match {
    case Array(db) if !catalog.databaseExists(db) =>
      catalog.createDatabase(
        toCatalogDatabase(db, metadata, defaultLocation = Some(catalog.getDefaultDBPath(db))),
        ignoreIfExists = false)

    case Array(_) =>
      throw new NamespaceAlreadyExistsException(namespace)

    case _ =>
      throw new IllegalArgumentException(s"Invalid namespace name: ${namespace.quoted}")
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    namespace match {
      case Array(db) =>
        // validate that this catalog's reserved properties are not removed
        changes.foreach {
          case remove: RemoveProperty if NAMESPACE_RESERVED_PROPERTIES.contains(remove.property) =>
            throw new UnsupportedOperationException(
              s"Cannot remove reserved property: ${remove.property}")
          case _ =>
        }

        val metadata = catalog.getDatabaseMetadata(db).toMetadata
        catalog.alterDatabase(
          toCatalogDatabase(db, catalogV2Util.applyNamespaceChanges(metadata, changes)))

      case _ =>
        throw new NoSuchNamespaceException(namespace)
    }
  }

  /**
   * List the metadata of partitions that belong to the specified table, assuming it exists, that
   * satisfy the given partition-pruning predicate expressions.
   */
  def listPartitionsByFilter(
      tableName: TableIdentifier,
      predicates: Seq[Expression]): Seq[CatalogTablePartition] = {
    catalog.listPartitionsByFilter(tableName, predicates)
  }

  def listPartitions(
      tableName: TableIdentifier,
      partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = {
    catalog.listPartitions(tableName, partialSpec)
  }

  override def dropNamespace(
      namespace: Array[String],
      cascade: Boolean): Boolean = namespace match {
    case Array(db) if catalog.databaseExists(db) =>
      if (catalog.listTables(db).nonEmpty && !cascade) {
        throw new IllegalStateException(s"Namespace ${namespace.quoted} is not empty")
      }
      catalog.dropDatabase(db, ignoreIfNotExists = false, cascade)
      true

    case Array(_) =>
      // exists returned false
      false

    case _ =>
      throw new NoSuchNamespaceException(namespace)
  }
}

private object HiveTableCatalog {
  private def toCatalogDatabase(
      db: String,
      metadata: util.Map[String, String],
      defaultLocation: Option[URI] = None): CatalogDatabase = {
    CatalogDatabase(
      name = db,
      description = metadata.getOrDefault(SupportsNamespaces.PROP_COMMENT, ""),
      locationUri = Option(metadata.get(SupportsNamespaces.PROP_LOCATION))
        .map(CatalogUtils.stringToURI)
        .orElse(defaultLocation)
        .getOrElse(throw new IllegalArgumentException("Missing database location")),
      properties = metadata.asScala.toMap --
        Seq(SupportsNamespaces.PROP_COMMENT, SupportsNamespaces.PROP_LOCATION))
  }

  implicit class NamespaceHelper(namespace: Array[String]) {
    def quoted: String = namespace.map(quoteIfNeeded).mkString(".")
  }

  implicit class IdentifierHelper(ident: Identifier) {
    def quoted: String = {
      if (ident.namespace.nonEmpty) {
        ident.namespace.map(quoteIfNeeded).mkString(".") + "." + quoteIfNeeded(ident.name)
      } else {
        quoteIfNeeded(ident.name)
      }
    }

    def asMultipartIdentifier: Seq[String] = ident.namespace :+ ident.name

    def asTableIdentifier: TableIdentifier = ident.namespace match {
      case ns if ns.isEmpty => TableIdentifier(ident.name)
      case Array(dbName) => TableIdentifier(ident.name, Some(dbName))
      case _ =>
        throw KyuubiHiveConnectorException(
          s"$quoted is not a valid TableIdentifier as it has more than 2 name parts.")
    }
  }

  implicit class CatalogDatabaseHelper(catalogDatabase: CatalogDatabase) {
    def toMetadata: util.Map[String, String] = {
      val metadata = mutable.HashMap[String, String]()

      catalogDatabase.properties.foreach {
        case (key, value) => metadata.put(key, value)
      }
      metadata.put(SupportsNamespaces.PROP_LOCATION, catalogDatabase.locationUri.toString)
      metadata.put(SupportsNamespaces.PROP_COMMENT, catalogDatabase.description)

      metadata.asJava
    }
  }
}
