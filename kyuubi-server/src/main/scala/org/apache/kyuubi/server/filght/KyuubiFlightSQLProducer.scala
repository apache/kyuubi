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

import org.apache.arrow.flight._
import org.apache.arrow.flight.sql.FlightSqlProducer
import org.apache.arrow.flight.sql.impl.FlightSql

import org.apache.kyuubi.Logging

class KyuubiFlightSQLProducer extends FlightSqlProducer with Logging {

  override def createPreparedStatement(
      request: FlightSql.ActionCreatePreparedStatementRequest,
      context: FlightProducer.CallContext,
      listener: FlightProducer.StreamListener[Result]): Unit =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def closePreparedStatement(
      request: FlightSql.ActionClosePreparedStatementRequest,
      context: FlightProducer.CallContext,
      listener: FlightProducer.StreamListener[Result]): Unit =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def getFlightInfoStatement(
      command: FlightSql.CommandStatementQuery,
      context: FlightProducer.CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def getFlightInfoPreparedStatement(
      command: FlightSql.CommandPreparedStatementQuery,
      context: FlightProducer.CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def getSchemaStatement(
      command: FlightSql.CommandStatementQuery,
      context: FlightProducer.CallContext,
      descriptor: FlightDescriptor): SchemaResult =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def getStreamStatement(
      ticket: FlightSql.TicketStatementQuery,
      context: FlightProducer.CallContext,
      listener: FlightProducer.ServerStreamListener): Unit =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def getStreamPreparedStatement(
      command: FlightSql.CommandPreparedStatementQuery,
      context: FlightProducer.CallContext,
      listener: FlightProducer.ServerStreamListener): Unit =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def acceptPutStatement(
      command: FlightSql.CommandStatementUpdate,
      context: FlightProducer.CallContext,
      flightStream: FlightStream,
      ackStream: FlightProducer.StreamListener[PutResult]): Runnable =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def acceptPutPreparedStatementUpdate(
      command: FlightSql.CommandPreparedStatementUpdate,
      context: FlightProducer.CallContext,
      flightStream: FlightStream,
      ackStream: FlightProducer.StreamListener[PutResult]): Runnable =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def acceptPutPreparedStatementQuery(
      command: FlightSql.CommandPreparedStatementQuery,
      context: FlightProducer.CallContext,
      flightStream: FlightStream,
      ackStream: FlightProducer.StreamListener[PutResult]): Runnable =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def getFlightInfoSqlInfo(
      request: FlightSql.CommandGetSqlInfo,
      context: FlightProducer.CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def getStreamSqlInfo(
      command: FlightSql.CommandGetSqlInfo,
      context: FlightProducer.CallContext,
      listener: FlightProducer.ServerStreamListener): Unit =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def getFlightInfoTypeInfo(
      request: FlightSql.CommandGetXdbcTypeInfo,
      context: FlightProducer.CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def getStreamTypeInfo(
      request: FlightSql.CommandGetXdbcTypeInfo,
      context: FlightProducer.CallContext,
      listener: FlightProducer.ServerStreamListener): Unit =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def getFlightInfoCatalogs(
      request: FlightSql.CommandGetCatalogs,
      context: FlightProducer.CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def getStreamCatalogs(
      context: FlightProducer.CallContext,
      listener: FlightProducer.ServerStreamListener): Unit =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def getFlightInfoSchemas(
      request: FlightSql.CommandGetDbSchemas,
      context: FlightProducer.CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def getStreamSchemas(
      command: FlightSql.CommandGetDbSchemas,
      context: FlightProducer.CallContext,
      listener: FlightProducer.ServerStreamListener): Unit =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def getFlightInfoTables(
      request: FlightSql.CommandGetTables,
      context: FlightProducer.CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def getStreamTables(
      command: FlightSql.CommandGetTables,
      context: FlightProducer.CallContext,
      listener: FlightProducer.ServerStreamListener): Unit =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def getFlightInfoTableTypes(
      request: FlightSql.CommandGetTableTypes,
      context: FlightProducer.CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def getStreamTableTypes(
      context: FlightProducer.CallContext,
      listener: FlightProducer.ServerStreamListener): Unit =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def getFlightInfoPrimaryKeys(
      request: FlightSql.CommandGetPrimaryKeys,
      context: FlightProducer.CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def getStreamPrimaryKeys(
      command: FlightSql.CommandGetPrimaryKeys,
      context: FlightProducer.CallContext,
      listener: FlightProducer.ServerStreamListener): Unit =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def getFlightInfoExportedKeys(
      request: FlightSql.CommandGetExportedKeys,
      context: FlightProducer.CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def getFlightInfoImportedKeys(
      request: FlightSql.CommandGetImportedKeys,
      context: FlightProducer.CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def getFlightInfoCrossReference(
      request: FlightSql.CommandGetCrossReference,
      context: FlightProducer.CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def getStreamExportedKeys(
      command: FlightSql.CommandGetExportedKeys,
      context: FlightProducer.CallContext,
      listener: FlightProducer.ServerStreamListener): Unit =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def getStreamImportedKeys(
      command: FlightSql.CommandGetImportedKeys,
      context: FlightProducer.CallContext,
      listener: FlightProducer.ServerStreamListener): Unit =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def getStreamCrossReference(
      command: FlightSql.CommandGetCrossReference,
      context: FlightProducer.CallContext,
      listener: FlightProducer.ServerStreamListener): Unit =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def listFlights(
      context: FlightProducer.CallContext,
      criteria: Criteria,
      listener: FlightProducer.StreamListener[FlightInfo]): Unit =
    throw CallStatus.UNIMPLEMENTED.withDescription("DoExchange is unimplemented").toRuntimeException

  override def close(): Unit = {}
}
