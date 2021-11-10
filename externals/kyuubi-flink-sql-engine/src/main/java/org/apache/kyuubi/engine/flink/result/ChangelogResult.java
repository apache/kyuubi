/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.engine.flink.result;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.experimental.SocketStreamIterator;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.apache.kyuubi.engine.flink.sink.CollectStreamTableSink;

/**
 * A result that works similarly to {@link DataStreamUtils#collect(DataStream)}.
 *
 * @param <C> cluster id to which this result belongs to
 */
public class ChangelogResult<C> extends AbstractResult<C, Tuple2<Boolean, Row>> {

  private final SocketStreamIterator<Tuple2<Boolean, Row>> iterator;
  private final CollectStreamTableSink collectTableSink;
  private final ResultRetrievalThread retrievalThread;
  private CompletableFuture<JobExecutionResult> jobExecutionResultFuture;

  private final Object resultLock;
  private AtomicReference<RuntimeException> executionException = new AtomicReference<>();
  private final List<Tuple2<Boolean, Row>> changeRecordBuffer;
  private final int maxBufferSize;

  public ChangelogResult(
      RowTypeInfo outputType,
      TableSchema tableSchema,
      ExecutionConfig config,
      InetAddress gatewayAddress,
      int gatewayPort,
      int maxBufferSize) {
    resultLock = new Object();

    // create socket stream iterator
    final TypeInformation<Tuple2<Boolean, Row>> socketType = Types.TUPLE(Types.BOOLEAN, outputType);
    final TypeSerializer<Tuple2<Boolean, Row>> serializer = socketType.createSerializer(config);
    try {
      // pass gateway port and address such that iterator knows where to bind to
      iterator = new SocketStreamIterator<>(gatewayPort, gatewayAddress, serializer);
    } catch (IOException e) {
      throw new RuntimeException("Could not start socket for result retrieval.", e);
    }

    // create table sink
    // pass binding address and port such that sink knows where to send to
    collectTableSink =
        new CollectStreamTableSink(
            iterator.getBindAddress(), iterator.getPort(), serializer, tableSchema);
    retrievalThread = new ResultRetrievalThread();

    // prepare for changelog
    changeRecordBuffer = new ArrayList<>();
    this.maxBufferSize = maxBufferSize;
  }

  @Override
  public void startRetrieval(JobClient jobClient) {
    // start listener thread
    retrievalThread.start();

    jobExecutionResultFuture =
        CompletableFuture.completedFuture(jobClient)
            .thenCompose(JobClient::getJobExecutionResult)
            .whenComplete(
                (unused, throwable) -> {
                  if (throwable != null) {
                    executionException.compareAndSet(
                        null, new RuntimeException("Error while submitting job.", throwable));
                  }
                });
  }

  @Override
  public TypedResult<List<Tuple2<Boolean, Row>>> retrieveChanges() {
    synchronized (resultLock) {
      // retrieval thread is alive return a record if available
      // but the program must not have failed
      if (isRetrieving() && executionException.get() == null) {
        if (changeRecordBuffer.isEmpty()) {
          return TypedResult.empty();
        } else {
          final List<Tuple2<Boolean, Row>> change = new ArrayList<>(changeRecordBuffer);
          changeRecordBuffer.clear();
          resultLock.notify();
          return TypedResult.payload(change);
        }
      }
      // retrieval thread is dead but there is still a record to be delivered
      else if (!isRetrieving() && !changeRecordBuffer.isEmpty()) {
        final List<Tuple2<Boolean, Row>> change = new ArrayList<>(changeRecordBuffer);
        changeRecordBuffer.clear();
        return TypedResult.payload(change);
      }
      // no results can be returned anymore
      else {
        return handleMissingResult();
      }
    }
  }

  @Override
  public TableSink<?> getTableSink() {
    return collectTableSink;
  }

  @Override
  public void close() {
    retrievalThread.isRunning = false;
    retrievalThread.interrupt();
    iterator.close();
  }

  // --------------------------------------------------------------------------------------------

  private <T> TypedResult<T> handleMissingResult() {

    // check if the monitoring thread is still there
    // we need to wait until we know what is going on
    if (!jobExecutionResultFuture.isDone()) {
      return TypedResult.empty();
    }

    if (executionException.get() != null) {
      throw executionException.get();
    }

    // we assume that a bounded job finished
    return TypedResult.endOfStream();
  }

  private boolean isRetrieving() {
    return retrievalThread.isRunning;
  }

  private void processRecord(Tuple2<Boolean, Row> change) {
    synchronized (resultLock) {
      // wait if the buffer is full
      if (changeRecordBuffer.size() >= maxBufferSize) {
        try {
          resultLock.wait();
        } catch (InterruptedException e) {
          // ignore
        }
      } else {
        changeRecordBuffer.add(change);
      }
    }
  }

  // --------------------------------------------------------------------------------------------

  private class ResultRetrievalThread extends Thread {

    public volatile boolean isRunning = true;

    @Override
    public void run() {
      try {
        while (isRunning && iterator.hasNext()) {
          final Tuple2<Boolean, Row> change = iterator.next();
          processRecord(change);
        }
      } catch (RuntimeException e) {
        // ignore socket exceptions
      }

      // no result anymore
      // either the job is done or an error occurred
      isRunning = false;
    }
  }
}
