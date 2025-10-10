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

import java.lang.{Boolean => JBoolean, Long => JLong}
import java.net.URI
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{CurrentUserContext, SQLConfHelper, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchDatabaseException, NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.quoteIfNeeded
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.NamespaceChange.RemoveProperty
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.hive.HiveUDFExpressionBuilder
import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper._
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf}
import org.apache.spark.sql.internal.StaticSQLConf.{CATALOG_IMPLEMENTATION, GLOBAL_TEMP_DATABASE}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.kyuubi.spark.connector.hive.HiveConnectorUtils.withSparkSQLConf
import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog.{getStorageFormatAndProvider, toCatalogDatabase, CatalogDatabaseHelper, IdentifierHelper, NamespaceHelper}
import org.apache.kyuubi.spark.connector.hive.KyuubiHiveConnectorDelegationTokenProvider.metastoreTokenSignature
import org.apache.kyuubi.spark.connector.hive.read.HiveFileStatusCache
import org.apache.kyuubi.util.reflect.{DynClasses, DynConstructors}

/**
 * A [[TableCatalog]] that wrap HiveExternalCatalog to as V2 CatalogPlugin instance to access Hive.
 */
class HiveTableCatalog(sparkSession: SparkSession)
  extends TableCatalog with SQLConfHelper with SupportsNamespaces with Logging {

  def this() = this(SparkSession.active)

  private val externalCatalogManager = ExternalCatalogManager.getOrCreate(sparkSession)

  private val LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME = "spark.sql.legacy.v1IdentifierNoCatalog"

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
    catalogOptions.asScala.foreach { case (k, v) => conf.set(k, v) }
    if (catalogOptions.containsKey("hive.metastore.uris")) {
      conf.set("hive.metastore.token.signature", metastoreTokenSignature(catalogOptions))
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

  private def newHiveMetastoreCatalog(sparkSession: SparkSession): HiveMetastoreCatalog = {
    val sparkSessionClz = DynClasses.builder()
      .impl("org.apache.spark.sql.classic.SparkSession") // SPARK-49700 (4.0.0)
      .impl("org.apache.spark.sql.SparkSession")
      .buildChecked()

    val hiveMetastoreCatalogCtor =
      DynConstructors.builder()
        .impl("org.apache.spark.sql.hive.HiveMetastoreCatalog", sparkSessionClz)
        .buildChecked[HiveMetastoreCatalog]()

    hiveMetastoreCatalogCtor.newInstanceChecked(sparkSession)
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
      globalTempViewManagerBuilder = () => globalTempViewManager,
      metastoreCatalog = newHiveMetastoreCatalog(sparkSession),
      functionRegistry = sessionState.functionRegistry,
      tableFunctionRegistry = sessionState.tableFunctionRegistry,
      hadoopConf = hadoopConf,
      parser = sessionState.sqlParser,
      functionResourceLoader = sessionState.resourceLoader,
      HiveUDFExpressionBuilder)
  }

  private lazy val globalTempViewManager: GlobalTempViewManager = {
    val globalTempDB = conf.getConf(GLOBAL_TEMP_DATABASE)
    if (externalCatalog.databaseExists(globalTempDB)) {
      throw KyuubiHiveConnectorException(
        s"$globalTempDB is a system preserved database, please rename your existing database to " +
          s"resolve the name conflict, or set a different value for ${GLOBAL_TEMP_DATABASE.key}, " +
          "and launch your Spark application again.")
    }
    new GlobalTempViewManager(globalTempDB)
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

  override def listTables(namespace: Array[String]): Array[Identifier] =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      namespace match {
        case Array(db) =>
          catalog
            .listTables(db)
            .map(ident =>
              Identifier.of(ident.database.map(Array(_)).getOrElse(Array()), ident.table))
            .toArray
        case _ =>
          throw new NoSuchNamespaceException(namespace)
      }
    }

  override def loadTable(ident: Identifier): Table =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      HiveTable(sparkSession, catalog.getTableMetadata(ident.asTableIdentifier), this)
    }

  // scalastyle:off
  private def newCatalogTable(
      identifier: TableIdentifier,
      tableType: CatalogTableType,
      storage: CatalogStorageFormat,
      schema: StructType,
      provider: Option[String] = None,
      partitionColumnNames: Seq[String] = Seq.empty,
      bucketSpec: Option[BucketSpec] = None,
      owner: String = Option(CurrentUserContext.CURRENT_USER.get()).getOrElse(""),
      createTime: JLong = System.currentTimeMillis,
      lastAccessTime: JLong = -1,
      createVersion: String = "",
      properties: Map[String, String] = Map.empty,
      stats: Option[CatalogStatistics] = None,
      viewText: Option[String] = None,
      comment: Option[String] = None,
      collation: Option[String] = None,
      unsupportedFeatures: Seq[String] = Seq.empty,
      tracksPartitionsInCatalog: JBoolean = false,
      schemaPreservesCase: JBoolean = true,
      ignoredProperties: Map[String, String] = Map.empty,
      viewOriginalText: Option[String] = None): CatalogTable = {
    // scalastyle:on
    Try { // SPARK-50675 (4.0.0)
      DynConstructors.builder()
        .impl(
          classOf[CatalogTable],
          classOf[TableIdentifier],
          classOf[CatalogTableType],
          classOf[CatalogStorageFormat],
          classOf[StructType],
          classOf[Option[String]],
          classOf[Seq[String]],
          classOf[Option[BucketSpec]],
          classOf[String],
          classOf[Long],
          classOf[Long],
          classOf[String],
          classOf[Map[String, String]],
          classOf[Option[CatalogStatistics]],
          classOf[Option[String]],
          classOf[Option[String]],
          classOf[Option[String]],
          classOf[Seq[String]],
          classOf[Boolean],
          classOf[Boolean],
          classOf[Map[String, String]],
          classOf[Option[String]])
        .buildChecked()
        .invokeChecked[CatalogTable](
          null,
          identifier,
          tableType,
          storage,
          schema,
          provider,
          partitionColumnNames,
          bucketSpec,
          owner,
          createTime,
          lastAccessTime,
          createVersion,
          properties,
          stats,
          viewText,
          comment,
          collation,
          unsupportedFeatures,
          tracksPartitionsInCatalog,
          schemaPreservesCase,
          ignoredProperties,
          viewOriginalText)
    }.recover { case _: Exception => // Spark 3.5 and previous
      DynConstructors.builder()
        .impl(
          classOf[CatalogTable],
          classOf[TableIdentifier],
          classOf[CatalogTableType],
          classOf[CatalogStorageFormat],
          classOf[StructType],
          classOf[Option[String]],
          classOf[Seq[String]],
          classOf[Option[BucketSpec]],
          classOf[String],
          classOf[Long],
          classOf[Long],
          classOf[String],
          classOf[Map[String, String]],
          classOf[Option[CatalogStatistics]],
          classOf[Option[String]],
          classOf[Option[String]],
          classOf[Seq[String]],
          classOf[Boolean],
          classOf[Boolean],
          classOf[Map[String, String]],
          classOf[Option[String]])
        .buildChecked()
        .invokeChecked[CatalogTable](
          null,
          identifier,
          tableType,
          storage,
          schema,
          provider,
          partitionColumnNames,
          bucketSpec,
          owner,
          createTime,
          lastAccessTime,
          createVersion,
          properties,
          stats,
          viewText,
          comment,
          unsupportedFeatures,
          tracksPartitionsInCatalog,
          schemaPreservesCase,
          ignoredProperties,
          viewOriginalText)
    }.get
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper.TransformHelper
      val (partitionColumns, maybeBucketSpec) = partitions.toSeq.convertTransforms
      val location = Option(properties.get(TableCatalog.PROP_LOCATION))
      val maybeProvider = Option(properties.get(TableCatalog.PROP_PROVIDER))
      val (storage, provider) =
        getStorageFormatAndProvider(
          maybeProvider,
          location,
          properties.asScala.toMap)
      val tableProperties = properties.asScala
      val isExternal = properties.containsKey(TableCatalog.PROP_EXTERNAL)
      val tableType =
        if (isExternal || location.isDefined) {
          CatalogTableType.EXTERNAL
        } else {
          CatalogTableType.MANAGED
        }

      val tableDesc = newCatalogTable(
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

  override def alterTable(ident: Identifier, changes: TableChange*): Table =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      val catalogTable =
        try {
          catalog.getTableMetadata(ident.asTableIdentifier)
        } catch {
          case _: NoSuchTableException =>
            throw new NoSuchTableException(ident)
        }

      val properties = CatalogV2Util.applyPropertiesChanges(catalogTable.properties, changes)
      val schema = HiveConnectorUtils.applySchemaChanges(
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
      invalidateTable(ident)
      loadTable(ident)
    }

  override def dropTable(ident: Identifier): Boolean =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      try {
        val table = loadTable(ident)
        if (table != null) {
          catalog.dropTable(
            ident.asTableIdentifier,
            ignoreIfNotExists = true,
            purge = true /* skip HDFS trash */ )
          invalidateTable(ident)
          true
        } else {
          false
        }
      } catch {
        case _: NoSuchTableException =>
          false
      }
    }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      if (tableExists(newIdent)) {
        throw new TableAlreadyExistsException(newIdent)
      }

      // Load table to make sure the table exists
      val table = loadTable(oldIdent)
      catalog.renameTable(oldIdent.asTableIdentifier, newIdent.asTableIdentifier)
      invalidateTable(oldIdent)
    }

  override def invalidateTable(ident: Identifier): Unit = {
    super.invalidateTable(ident)
    val qualifiedName = s"$catalogName.$ident"
    HiveFileStatusCache.getOrCreate(sparkSession, qualifiedName).invalidateAll()
  }

  private def toOptions(properties: Map[String, String]): Map[String, String] = {
    properties.filterKeys(_.startsWith(TableCatalog.OPTION_PREFIX)).map {
      case (key, value) => key.drop(TableCatalog.OPTION_PREFIX.length) -> value
    }.toMap
  }

  override def listNamespaces(): Array[Array[String]] =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      catalog.listDatabases().map(Array(_)).toArray
    }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      namespace match {
        case Array() =>
          listNamespaces()
        case Array(db) if catalog.databaseExists(db) =>
          Array()
        case _ =>
          throw new NoSuchNamespaceException(namespace)
      }
    }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
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
      metadata: util.Map[String, String]): Unit =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      namespace match {
        case Array(db) if !catalog.databaseExists(db) =>
          catalog.createDatabase(
            toCatalogDatabase(db, metadata, defaultLocation = Some(catalog.getDefaultDBPath(db))),
            ignoreIfExists = false)

        case Array(_) =>
          throw new NamespaceAlreadyExistsException(namespace)

        case _ =>
          throw new IllegalArgumentException(s"Invalid namespace name: ${namespace.quoted}")
      }
    }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      namespace match {
        case Array(db) =>
          // validate that this catalog's reserved properties are not removed
          changes.foreach {
            case remove: RemoveProperty
                if NAMESPACE_RESERVED_PROPERTIES.contains(remove.property) =>
              throw new UnsupportedOperationException(
                s"Cannot remove reserved property: ${remove.property}")
            case _ =>
          }

          val metadata = catalog.getDatabaseMetadata(db).toMetadata
          catalog.alterDatabase(
            toCatalogDatabase(db, CatalogV2Util.applyNamespaceChanges(metadata, changes)))

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
      cascade: Boolean): Boolean =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      namespace match {
        case Array(db) if catalog.databaseExists(db) =>
          catalog.dropDatabase(db, ignoreIfNotExists = false, cascade)
          true

        case Array(_) =>
          // exists returned false
          false

        case _ =>
          throw new NoSuchNamespaceException(namespace)
      }
    }
}

private object HiveTableCatalog extends Logging {
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

  private def getStorageFormatAndProvider(
      provider: Option[String],
      location: Option[String],
      options: Map[String, String]): (CatalogStorageFormat, String) = {
    val nonHiveStorageFormat = CatalogStorageFormat.empty.copy(
      locationUri = location.map(CatalogUtils.stringToURI),
      properties = options)

    val conf = SQLConf.get
    val defaultHiveStorage = HiveSerDe.getDefaultStorage(conf).copy(
      locationUri = location.map(CatalogUtils.stringToURI),
      properties = options)

    if (provider.isDefined) {
      (nonHiveStorageFormat, provider.get)
    } else if (serdeIsDefined(options)) {
      val maybeSerde = options.get("hive.serde")
      val maybeStoredAs = options.get("hive.stored-as")
      val maybeInputFormat = options.get("hive.input-format")
      val maybeOutputFormat = options.get("hive.output-format")
      val storageFormat = if (maybeStoredAs.isDefined) {
        // If `STORED AS fileFormat` is used, infer inputFormat, outputFormat and serde from it.
        HiveSerDe.sourceToSerDe(maybeStoredAs.get) match {
          case Some(hiveSerde) =>
            defaultHiveStorage.copy(
              inputFormat = hiveSerde.inputFormat.orElse(defaultHiveStorage.inputFormat),
              outputFormat = hiveSerde.outputFormat.orElse(defaultHiveStorage.outputFormat),
              // User specified serde takes precedence over the one inferred from file format.
              serde = maybeSerde.orElse(hiveSerde.serde).orElse(defaultHiveStorage.serde),
              properties = options ++ defaultHiveStorage.properties)
          case _ => throw KyuubiHiveConnectorException(s"Unsupported serde ${maybeSerde.get}.")
        }
      } else {
        defaultHiveStorage.copy(
          inputFormat =
            maybeInputFormat.orElse(defaultHiveStorage.inputFormat),
          outputFormat =
            maybeOutputFormat.orElse(defaultHiveStorage.outputFormat),
          serde = maybeSerde.orElse(defaultHiveStorage.serde),
          properties = options ++ defaultHiveStorage.properties)
      }
      (storageFormat, DDLUtils.HIVE_PROVIDER)
    } else {
      val createHiveTableByDefault = conf.getConf(SQLConf.LEGACY_CREATE_HIVE_TABLE_BY_DEFAULT)
      if (!createHiveTableByDefault) {
        (nonHiveStorageFormat, conf.defaultDataSourceName)
      } else {
        logWarning("A Hive serde table will be created as there is no table provider " +
          s"specified. You can set ${SQLConf.LEGACY_CREATE_HIVE_TABLE_BY_DEFAULT.key} to false " +
          "so that native data source table will be created instead.")
        (defaultHiveStorage, DDLUtils.HIVE_PROVIDER)
      }
    }
  }

  private def serdeIsDefined(options: Map[String, String]): Boolean = {
    val maybeStoredAs = options.get("hive.stored-as")
    val maybeInputFormat = options.get("hive.input-format")
    val maybeOutputFormat = options.get("hive.output-format")
    val maybeSerde = options.get("hive.serde")
    maybeStoredAs.isDefined || maybeInputFormat.isDefined ||
    maybeOutputFormat.isDefined || maybeSerde.isDefined
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
