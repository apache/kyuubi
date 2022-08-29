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

package org.apache.kyuubi.server.filght

import java.util
import java.util.Collections.singletonList

import com.google.protobuf.Any.pack
import com.google.protobuf.Message
import org.apache.arrow.flight._
import org.apache.arrow.flight.sql.{FlightSqlProducer, SqlInfoBuilder}
import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas
import org.apache.arrow.flight.sql.impl.FlightSql
import org.apache.arrow.vector.types.pojo.Schema

import org.apache.kyuubi.Logging

class KyuubiFlightSQLProducer(location: Location) extends FlightSqlProducer with Logging {

  override def createPreparedStatement(
      request: FlightSql.ActionCreatePreparedStatementRequest,
      context: FlightProducer.CallContext,
      listener: FlightProducer.StreamListener[Result]): Unit =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("CreatePreparedStatement is unimplemented").toRuntimeException

  override def closePreparedStatement(
      request: FlightSql.ActionClosePreparedStatementRequest,
      context: FlightProducer.CallContext,
      listener: FlightProducer.StreamListener[Result]): Unit =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("ClosePreparedStatement is unimplemented").toRuntimeException

  override def getFlightInfoStatement(
      command: FlightSql.CommandStatementQuery,
      context: FlightProducer.CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("GetFlightInfoStatement is unimplemented").toRuntimeException

  override def getFlightInfoPreparedStatement(
      command: FlightSql.CommandPreparedStatementQuery,
      context: FlightProducer.CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("GetFlightInfoPreparedStatement is unimplemented").toRuntimeException

  override def getSchemaStatement(
      command: FlightSql.CommandStatementQuery,
      context: FlightProducer.CallContext,
      descriptor: FlightDescriptor): SchemaResult =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("GetSchemaStatement is unimplemented").toRuntimeException

  override def getStreamStatement(
      ticket: FlightSql.TicketStatementQuery,
      context: FlightProducer.CallContext,
      listener: FlightProducer.ServerStreamListener): Unit =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("GetStreamStatement is unimplemented").toRuntimeException

  override def getStreamPreparedStatement(
      command: FlightSql.CommandPreparedStatementQuery,
      context: FlightProducer.CallContext,
      listener: FlightProducer.ServerStreamListener): Unit =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("GetStreamPreparedStatement is unimplemented").toRuntimeException

  override def acceptPutStatement(
      command: FlightSql.CommandStatementUpdate,
      context: FlightProducer.CallContext,
      flightStream: FlightStream,
      ackStream: FlightProducer.StreamListener[PutResult]): Runnable =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("AcceptPutStatement is unimplemented").toRuntimeException

  override def acceptPutPreparedStatementUpdate(
      command: FlightSql.CommandPreparedStatementUpdate,
      context: FlightProducer.CallContext,
      flightStream: FlightStream,
      ackStream: FlightProducer.StreamListener[PutResult]): Runnable =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("AcceptPutPreparedStatementUpdate is unimplemented").toRuntimeException

  override def acceptPutPreparedStatementQuery(
      command: FlightSql.CommandPreparedStatementQuery,
      context: FlightProducer.CallContext,
      flightStream: FlightStream,
      ackStream: FlightProducer.StreamListener[PutResult]): Runnable =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("AcceptPutPreparedStatementQuery is unimplemented").toRuntimeException

  override def getFlightInfoSqlInfo(
      request: FlightSql.CommandGetSqlInfo,
      context: FlightProducer.CallContext,
      descriptor: FlightDescriptor): FlightInfo = {
    getFlightInfoForSchema(request, descriptor, Schemas.GET_SQL_INFO_SCHEMA)
  }

  override def getStreamSqlInfo(
      command: FlightSql.CommandGetSqlInfo,
      context: FlightProducer.CallContext,
      listener: FlightProducer.ServerStreamListener): Unit = {
    val sqlInfoBuilder = new SqlInfoBuilder()
      .withFlightSqlServerName("Apache Kyuubi (Incubating)")
    sqlInfoBuilder.send(command.getInfoList, listener)
  }

  override def getFlightInfoTypeInfo(
      request: FlightSql.CommandGetXdbcTypeInfo,
      context: FlightProducer.CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("GetFlightInfoTypeInfo is unimplemented").toRuntimeException

  override def getStreamTypeInfo(
      request: FlightSql.CommandGetXdbcTypeInfo,
      context: FlightProducer.CallContext,
      listener: FlightProducer.ServerStreamListener): Unit =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("GetStreamTypeInfo is unimplemented").toRuntimeException

  override def getFlightInfoCatalogs(
      request: FlightSql.CommandGetCatalogs,
      context: FlightProducer.CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("GetFlightInfoCatalogs is unimplemented").toRuntimeException

  override def getStreamCatalogs(
      context: FlightProducer.CallContext,
      listener: FlightProducer.ServerStreamListener): Unit =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("GetStreamCatalogs is unimplemented").toRuntimeException

  override def getFlightInfoSchemas(
      request: FlightSql.CommandGetDbSchemas,
      context: FlightProducer.CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("GetFlightInfoSchemas is unimplemented").toRuntimeException

  override def getStreamSchemas(
      command: FlightSql.CommandGetDbSchemas,
      context: FlightProducer.CallContext,
      listener: FlightProducer.ServerStreamListener): Unit =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("GetStreamSchemas is unimplemented").toRuntimeException

  override def getFlightInfoTables(
      request: FlightSql.CommandGetTables,
      context: FlightProducer.CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("GetFlightInfoTables is unimplemented").toRuntimeException

  override def getStreamTables(
      command: FlightSql.CommandGetTables,
      context: FlightProducer.CallContext,
      listener: FlightProducer.ServerStreamListener): Unit =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("GetStreamTables is unimplemented").toRuntimeException

  override def getFlightInfoTableTypes(
      request: FlightSql.CommandGetTableTypes,
      context: FlightProducer.CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("GetFlightInfoTableTypes is unimplemented").toRuntimeException

  override def getStreamTableTypes(
      context: FlightProducer.CallContext,
      listener: FlightProducer.ServerStreamListener): Unit =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("GetStreamTableTypes is unimplemented").toRuntimeException

  override def getFlightInfoPrimaryKeys(
      request: FlightSql.CommandGetPrimaryKeys,
      context: FlightProducer.CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("GetFlightInfoPrimaryKeys is unimplemented").toRuntimeException

  override def getStreamPrimaryKeys(
      command: FlightSql.CommandGetPrimaryKeys,
      context: FlightProducer.CallContext,
      listener: FlightProducer.ServerStreamListener): Unit =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("GetStreamPrimaryKeys is unimplemented").toRuntimeException

  override def getFlightInfoExportedKeys(
      request: FlightSql.CommandGetExportedKeys,
      context: FlightProducer.CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("GetFlightInfoExportedKeys is unimplemented").toRuntimeException

  override def getFlightInfoImportedKeys(
      request: FlightSql.CommandGetImportedKeys,
      context: FlightProducer.CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("GetFlightInfoImportedKeys is unimplemented").toRuntimeException

  override def getFlightInfoCrossReference(
      request: FlightSql.CommandGetCrossReference,
      context: FlightProducer.CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("GetFlightInfoCrossReference is unimplemented").toRuntimeException

  override def getStreamExportedKeys(
      command: FlightSql.CommandGetExportedKeys,
      context: FlightProducer.CallContext,
      listener: FlightProducer.ServerStreamListener): Unit =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("GetStreamExportedKeys is unimplemented").toRuntimeException

  override def getStreamImportedKeys(
      command: FlightSql.CommandGetImportedKeys,
      context: FlightProducer.CallContext,
      listener: FlightProducer.ServerStreamListener): Unit =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("GetStreamImportedKeys is unimplemented").toRuntimeException

  override def getStreamCrossReference(
      command: FlightSql.CommandGetCrossReference,
      context: FlightProducer.CallContext,
      listener: FlightProducer.ServerStreamListener): Unit =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("GetStreamCrossReference is unimplemented").toRuntimeException

  override def listFlights(
      context: FlightProducer.CallContext,
      criteria: Criteria,
      listener: FlightProducer.StreamListener[FlightInfo]): Unit =
    throw CallStatus.UNIMPLEMENTED
      .withDescription("ListFlights is unimplemented").toRuntimeException

  override def close(): Unit = {}

  private def getFlightInfoForSchema[T <: Message](
      request: T,
      descriptor: FlightDescriptor,
      schema: Schema): FlightInfo = {
    val ticket: Ticket = new Ticket(pack(request).toByteArray)
    val endpoints: util.List[FlightEndpoint] = singletonList(new FlightEndpoint(ticket, location))
    new FlightInfo(schema, descriptor, endpoints, -1, -1)
  }
}
