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

package org.apache.kyuubi.server.flight

import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.google.protobuf.{Any => ProtoAny, ByteString}
import org.apache.arrow.flight.{CallStatus, FlightDescriptor, FlightEndpoint, FlightInfo, Location, SchemaResult, Ticket}
import org.apache.arrow.flight.FlightProducer.{CallContext, ServerStreamListener, StreamListener}
import org.apache.arrow.flight.sql.{CancelResult, NoOpFlightSqlProducer, SqlInfoBuilder}
import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas
import org.apache.arrow.flight.sql.impl.FlightSql._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema

import org.apache.kyuubi.{KYUUBI_VERSION, Logging}
import org.apache.kyuubi.config.{KyuubiConf, KyuubiReservedKeys}
import org.apache.kyuubi.config.KyuubiConf.{FRONTEND_FLIGHT_SQL_FETCH_MAX_ROWS, OPERATION_RESULT_FORMAT}
import org.apache.kyuubi.metrics.{MetricsConstants, MetricsSystem}
import org.apache.kyuubi.operation.{OperationHandle, OperationState, OperationStatus}
import org.apache.kyuubi.service.BackendService
import org.apache.kyuubi.session.SessionHandle
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion

class KyuubiFlightSqlProducer(
    backend: BackendService,
    allocator: BufferAllocator,
    location: () => Location,
    conf: KyuubiConf)
  extends NoOpFlightSqlProducer with Logging {

  private case class QueryState(
      owner: String,
      session: SessionHandle,
      operation: OperationHandle,
      schema: Schema,
      ownerEndpoint: String)

  private val queryStates = new ConcurrentHashMap[String, QueryState]()

  private val fetchPageSize = conf.get(FRONTEND_FLIGHT_SQL_FETCH_MAX_ROWS)

  private val sqlInfoBuilder = new SqlInfoBuilder()
    .withFlightSqlServerName("Kyuubi")
    .withFlightSqlServerVersion(KYUUBI_VERSION)
    .withFlightSqlServerArrowVersion("16.0.0")
    .withFlightSqlServerSql(true)
    .withFlightSqlServerSubstrait(false)
    .withFlightSqlServerCancel(true)
    .withFlightSqlServerTransaction(SqlSupportedTransaction.SQL_SUPPORTED_TRANSACTION_NONE)
    .withSqlIdentifierQuoteChar("`")
    .withSqlIdentifierCase(SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UNKNOWN)
    .withSqlQuotedIdentifierCase(SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UNKNOWN)
    .withSqlNullOrdering(SqlNullOrdering.SQL_NULLS_SORTED_AT_END)

  override def getFlightInfoStatement(
      command: CommandStatementQuery,
      context: CallContext,
      descriptor: FlightDescriptor): FlightInfo = {
    MetricsSystem.tracing { ms =>
      ms.incCount(MetricsConstants.FLIGHT_SQL_OPERATION_TOTAL)
      ms.incCount(MetricsConstants.FLIGHT_SQL_OPERATION_OPEN)
    }
    try {
      val owner = ownerOf(context)
      val session = openSession(owner)
      try {
        val operation = backend.executeStatement(
          session,
          command.getQuery,
          Map.empty,
          runAsync = true,
          queryTimeout = 0)
        val status = waitForCompletion(operation, () => context.isCancelled)
        val schema = KyuubiFlightArrowUtils.schemaFromMetadata(
          backend.getResultSetMetadata(operation))
        if (status.state != OperationState.FINISHED) {
          closeResources(session, operation)
          MetricsSystem.tracing(_.decCount(MetricsConstants.FLIGHT_SQL_OPERATION_OPEN))
          MetricsSystem.tracing(_.incCount(MetricsConstants.FLIGHT_SQL_OPERATION_FAIL))
          throw operationError(status)
        }
        val id = UUID.randomUUID().toString
        val endpointLocation = location()
        queryStates.put(
          id,
          QueryState(owner, session, operation, schema, endpointLocation.getUri.toString))
        val ticket = statementTicket(id)
        new FlightInfo(
          schema,
          descriptor,
          java.util.Arrays.asList(new FlightEndpoint(ticket, endpointLocation)),
          -1L,
          -1L)
      } catch {
        case NonFatal(e) =>
          try backend.closeSession(session)
          catch {
            case NonFatal(closeError) => warn("Failed to close Flight SQL session", closeError)
          }
          throw e
      }
    } catch {
      case e: RuntimeException => throw e
      case NonFatal(e) => throw flightError(CallStatus.INTERNAL, e)
    }
  }

  override def getSchemaStatement(
      command: CommandStatementQuery,
      context: CallContext,
      descriptor: FlightDescriptor): SchemaResult = {
    val owner = ownerOf(context)
    val session = openSession(owner)
    var operation: OperationHandle = null
    try {
      operation = backend.executeStatement(
        session,
        command.getQuery,
        Map.empty,
        runAsync = true,
        queryTimeout = 0)
      val status = waitForCompletion(operation, () => context.isCancelled)
      if (status.state != OperationState.FINISHED) {
        throw operationError(status)
      }
      new SchemaResult(KyuubiFlightArrowUtils.schemaFromMetadata(
        backend.getResultSetMetadata(operation)))
    } catch {
      case e: RuntimeException => throw e
      case NonFatal(e) => throw flightError(CallStatus.INTERNAL, e)
    } finally {
      closeResources(session, operation)
    }
  }

  override def getStreamStatement(
      ticket: TicketStatementQuery,
      context: CallContext,
      listener: ServerStreamListener): Unit = {
    val id = ticket.getStatementHandle.toStringUtf8
    val state = Option(queryStates.get(id)).getOrElse(
      throw flightError(
        CallStatus.NOT_FOUND,
        new IllegalArgumentException("Unknown Flight SQL ticket")))
    if (state.owner != ownerOf(context)) {
      throw flightError(
        CallStatus.UNAUTHORIZED,
        new SecurityException("Flight SQL ticket owner mismatch"))
    }
    val current = location().getUri.toString
    if (state.ownerEndpoint != current) {
      throw flightError(
        CallStatus.UNAVAILABLE,
        new IllegalStateException(
          s"Flight SQL ticket is owned by ${state.ownerEndpoint}, not $current. " +
            "Retry against the owning endpoint; transparent failover is not supported."))
    }

    val iterator = new FlightResultIterator(
      backend,
      state.operation,
      state.schema,
      allocator,
      fetchPageSize,
      () => context.isCancelled || listener.isCancelled)
    listener.setOnCancelHandler(() => {
      MetricsSystem.tracing(_.incCount(MetricsConstants.FLIGHT_SQL_OPERATION_CANCELLED))
      iterator.cancel()
    })
    try {
      iterator.start(listener)
      var ok = true
      while (ok && !context.isCancelled && !listener.isCancelled) {
        ok = iterator.nextBatch()
        if (ok && iterator.currentRoot.getRowCount > 0) {
          listener.putNext()
        }
      }
      if (context.isCancelled || listener.isCancelled) {
        iterator.cancel()
      } else {
        listener.completed()
      }
    } catch {
      case NonFatal(e) =>
        MetricsSystem.tracing(_.incCount(MetricsConstants.FLIGHT_SQL_OPERATION_FAIL))
        if (!context.isCancelled && !listener.isCancelled) listener.error(e)
    } finally {
      iterator.close()
      MetricsSystem.tracing(_.decCount(MetricsConstants.FLIGHT_SQL_OPERATION_OPEN))
      closeState(id, state)
    }
  }

  override def getFlightInfoCatalogs(
      command: CommandGetCatalogs,
      context: CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    metadataInfo(Schemas.GET_CATALOGS_SCHEMA, command, descriptor)

  override def getStreamCatalogs(
      context: CallContext,
      listener: ServerStreamListener): Unit =
    streamMetadata(
      context,
      listener,
      Schemas.GET_CATALOGS_SCHEMA,
      session =>
        backend.getCatalogs(session))

  override def getFlightInfoSchemas(
      command: CommandGetDbSchemas,
      context: CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    metadataInfo(Schemas.GET_SCHEMAS_SCHEMA, command, descriptor)

  override def getStreamSchemas(
      command: CommandGetDbSchemas,
      context: CallContext,
      listener: ServerStreamListener): Unit = {
    val catalog = if (command.hasCatalog) command.getCatalog else null
    val schema = if (command.hasDbSchemaFilterPattern) command.getDbSchemaFilterPattern else null
    streamMetadata(
      context,
      listener,
      Schemas.GET_SCHEMAS_SCHEMA,
      s =>
        backend.getSchemas(s, Option(catalog).getOrElse(""), Option(schema).getOrElse("")))
  }

  override def getFlightInfoTables(
      command: CommandGetTables,
      context: CallContext,
      descriptor: FlightDescriptor): FlightInfo = {
    val schema = if (command.getIncludeSchema) Schemas.GET_TABLES_SCHEMA
    else Schemas.GET_TABLES_SCHEMA_NO_SCHEMA
    metadataInfo(schema, command, descriptor)
  }

  override def getStreamTables(
      command: CommandGetTables,
      context: CallContext,
      listener: ServerStreamListener): Unit = {
    val catalog = if (command.hasCatalog) command.getCatalog else null
    val schema = if (command.hasDbSchemaFilterPattern) command.getDbSchemaFilterPattern else null
    val table = if (command.hasTableNameFilterPattern) command.getTableNameFilterPattern else null
    streamMetadata(
      context,
      listener,
      if (command.getIncludeSchema) Schemas.GET_TABLES_SCHEMA
      else Schemas.GET_TABLES_SCHEMA_NO_SCHEMA,
      s =>
        backend.getTables(
          s,
          Option(catalog).getOrElse(""),
          Option(schema).getOrElse(""),
          Option(table).getOrElse(""),
          command.getTableTypesList))
  }

  override def getFlightInfoTableTypes(
      command: CommandGetTableTypes,
      context: CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    metadataInfo(Schemas.GET_TABLE_TYPES_SCHEMA, command, descriptor)

  override def getStreamTableTypes(
      context: CallContext,
      listener: ServerStreamListener): Unit =
    streamMetadata(
      context,
      listener,
      Schemas.GET_TABLE_TYPES_SCHEMA,
      session =>
        backend.getTableTypes(session))

  override def getFlightInfoTypeInfo(
      command: CommandGetXdbcTypeInfo,
      context: CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    metadataInfo(Schemas.GET_TYPE_INFO_SCHEMA, command, descriptor)

  override def getFlightInfoSqlInfo(
      command: CommandGetSqlInfo,
      context: CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    metadataInfo(Schemas.GET_SQL_INFO_SCHEMA, command, descriptor)

  override def getStreamSqlInfo(
      command: CommandGetSqlInfo,
      context: CallContext,
      listener: ServerStreamListener): Unit =
    sqlInfoBuilder.send(command.getInfoList, listener)

  override def getStreamTypeInfo(
      command: CommandGetXdbcTypeInfo,
      context: CallContext,
      listener: ServerStreamListener): Unit =
    streamMetadata(
      context,
      listener,
      Schemas.GET_TYPE_INFO_SCHEMA,
      session =>
        backend.getTypeInfo(session))

  override def cancelQuery(
      info: FlightInfo,
      context: CallContext,
      listener: StreamListener[CancelResult]): Unit = {
    try {
      val endpoint = info.getEndpoints.get(0)
      val any = ProtoAny.parseFrom(endpoint.getTicket.getBytes)
      val statementTicket = TicketStatementQuery.parseFrom(any.getValue)
      val id = statementTicket.getStatementHandle.toStringUtf8
      Option(queryStates.get(id)) match {
        case Some(state) if state.owner == ownerOf(context) =>
          cancelState(state)
          MetricsSystem.tracing(_.incCount(MetricsConstants.FLIGHT_SQL_OPERATION_CANCELLED))
          listener.onNext(CancelResult.CANCELLED)
        case Some(_) =>
          listener.onNext(CancelResult.NOT_CANCELLABLE)
        case None =>
          listener.onNext(CancelResult.NOT_CANCELLABLE)
      }
      listener.onCompleted()
    } catch {
      case NonFatal(e) => listener.onError(flightError(CallStatus.INVALID_ARGUMENT, e))
    }
  }

  override def close(): Unit = {
    queryStates.entrySet().asScala.foreach(entry => closeState(entry.getKey, entry.getValue))
    queryStates.clear()
  }

  private def metadataInfo(
      schema: Schema,
      command: com.google.protobuf.Message,
      descriptor: FlightDescriptor): FlightInfo = {
    val ticket = new Ticket(ProtoAny.pack(command).toByteArray)
    new FlightInfo(
      schema,
      descriptor,
      java.util.Arrays.asList(new FlightEndpoint(ticket, location())),
      -1L,
      -1L)
  }

  private def streamMetadata(
      context: CallContext,
      listener: ServerStreamListener,
      schema: Schema,
      operationFactory: SessionHandle => OperationHandle): Unit = {
    val owner = ownerOf(context)
    val session = openSession(owner)
    var operation: OperationHandle = null
    var iterator: FlightResultIterator = null
    MetricsSystem.tracing { ms =>
      ms.incCount(MetricsConstants.FLIGHT_SQL_OPERATION_TOTAL)
      ms.incCount(MetricsConstants.FLIGHT_SQL_OPERATION_OPEN)
    }
    try {
      operation = operationFactory(session)
      val status = waitForCompletion(operation, () => context.isCancelled || listener.isCancelled)
      if (status.state != OperationState.FINISHED) throw operationError(status)
      iterator = new FlightResultIterator(
        backend,
        operation,
        schema,
        allocator,
        fetchPageSize,
        () => context.isCancelled || listener.isCancelled)
      listener.setOnCancelHandler(() => {
        MetricsSystem.tracing(_.incCount(MetricsConstants.FLIGHT_SQL_OPERATION_CANCELLED))
        iterator.cancel()
      })
      iterator.start(listener)
      var ok = true
      while (ok && !context.isCancelled && !listener.isCancelled) {
        ok = iterator.nextBatch()
        if (ok && iterator.currentRoot.getRowCount > 0) {
          listener.putNext()
        }
      }
      if (context.isCancelled || listener.isCancelled) {
        iterator.cancel()
      } else {
        listener.completed()
      }
    } catch {
      case NonFatal(e) =>
        MetricsSystem.tracing(_.incCount(MetricsConstants.FLIGHT_SQL_OPERATION_FAIL))
        if (!context.isCancelled && !listener.isCancelled) listener.error(e)
    } finally {
      if (iterator != null) iterator.close()
      MetricsSystem.tracing(_.decCount(MetricsConstants.FLIGHT_SQL_OPERATION_OPEN))
      closeResources(session, operation)
    }
  }

  private def openSession(owner: String): SessionHandle = {
    backend.openSession(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10,
      owner,
      "",
      "",
      Map(
        KyuubiReservedKeys.KYUUBI_CLIENT_IP_KEY -> "",
        KyuubiReservedKeys.KYUUBI_SESSION_REAL_USER_KEY -> owner,
        KyuubiReservedKeys.KYUUBI_SESSION_CONNECTION_URL_KEY -> location().toString,
        OPERATION_RESULT_FORMAT.key -> "arrow",
        KyuubiConf.FRONTEND_PROTOCOLS.key ->
          KyuubiConf.FrontendProtocols.FLIGHT_SQL.toString))
  }

  private def waitForCompletion(
      operation: OperationHandle,
      isCancelled: () => Boolean): OperationStatus = {
    var status = backend.getOperationStatus(operation, Some(1000L))
    while (!OperationState.isTerminal(status.state) && !isCancelled()) {
      status = backend.getOperationStatus(operation, Some(1000L))
    }
    status
  }

  private def statementTicket(id: String): Ticket = {
    val statementTicket = TicketStatementQuery.newBuilder
      .setStatementHandle(ByteString.copyFrom(id.getBytes(StandardCharsets.UTF_8)))
      .build()
    new Ticket(ProtoAny.pack(statementTicket).toByteArray)
  }

  private def ownerOf(context: CallContext): String =
    Option(context.peerIdentity()).filter(_.nonEmpty).getOrElse("anonymous")

  private def closeState(id: String, state: QueryState): Unit = {
    if (queryStates.remove(id, state)) {
      closeResources(state.session, state.operation)
    }
  }

  private def closeResources(session: SessionHandle, operation: OperationHandle): Unit = {
    if (operation != null) {
      try backend.closeOperation(operation)
      catch {
        case NonFatal(e) => warn(s"Failed to close Flight SQL operation $operation", e)
      }
    }
    if (session != null) {
      try backend.closeSession(session)
      catch {
        case NonFatal(e) => warn(s"Failed to close Flight SQL session $session", e)
      }
    }
  }

  private def cancelState(state: QueryState): Unit = {
    try backend.cancelOperation(state.operation)
    catch {
      case NonFatal(e) => warn(s"Failed to cancel Flight SQL operation ${state.operation}", e)
    }
  }

  private def operationError(status: OperationStatus): RuntimeException =
    flightError(
      CallStatus.INTERNAL,
      status.exception.getOrElse(
        new IllegalStateException(s"Flight SQL operation ended in ${status.state}")))

  private def flightError(status: CallStatus, cause: Throwable): RuntimeException =
    status.withDescription(Option(cause.getMessage).getOrElse(cause.getClass.getSimpleName))
      .withCause(cause)
      .toRuntimeException
}
