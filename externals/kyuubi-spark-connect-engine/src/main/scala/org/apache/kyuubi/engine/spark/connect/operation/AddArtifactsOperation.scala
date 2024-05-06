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
package org.apache.kyuubi.engine.spark.connect.operation

import io.grpc.stub.StreamObserver
import org.apache.kyuubi.{KyuubiException, Utils}
import org.apache.kyuubi.engine.spark.connect.grpc.proto.AddArtifactsResponse.ArtifactSummary
import org.apache.kyuubi.engine.spark.connect.grpc.proto.{AddArtifactsRequest, AddArtifactsResponse}
import org.apache.kyuubi.engine.spark.connect.utils.SparkConnectUtils
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.shaded.google.common.io.CountingOutputStream

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.util.zip.{CRC32, CheckedOutputStream}
import scala.collection.mutable
import scala.util.control.NonFatal

class AddArtifactsOperation(
    session: Session,
    responseObserver: StreamObserver[AddArtifactsResponse])
  extends SparkConnectOperation(session)
  with StreamObserver[AddArtifactsRequest] {

  // Temporary directory where artifacts are rebuilt from the bytes sent over the wire
  protected val stagingDir: Path = Utils.createTempDir()
  protected val stagedArtifacts: mutable.Buffer[StagedArtifact] =
    mutable.Buffer.empty[StagedArtifact]
  private var chunkedArtifact: StagedChunkedArtifact = _

  override def onNext(request: AddArtifactsRequest): Unit = {
    if (request.hasBeginChunk) {
      require(chunkedArtifact == null)
      chunkedArtifact = writeArtifactToFile(request.getBeginChunk)
    } else if (request.hasChunk) {
      require(chunkedArtifact != null && !chunkedArtifact.isFinished)
      chunkedArtifact.write(request.getChunk)

      if (chunkedArtifact.isFinished) {
        chunkedArtifact.close()
        chunkedArtifact = null
      }
    } else if (request.hasBatch) {
      request.getBatch.getArtifactsList.forEach(artifact => writeArtifactToFile(artifact).close())
    } else {
      throw new UnsupportedOperationException(s"Unsupported data transfer request: $request")
    }
  }

  override def onError(throwable: Throwable): Unit = {
    cleanUpStagedArtifacts()
    responseObserver.onError(throwable)
  }

  protected def addStagedArtifactToArtifactManager




  class StagedArtifact(val name: String) {
    val (canonicalFileName: String, fragment: Option[String]) =
      if (name.startsWith(s"archives${File.separator}")) {
        val splits = name.split("#")
        assert(splits.length <= 2, "'#' in the path is not supported for adding an archive.")
        if (splits.length == 2) {
          (splits(0), Some(splits(1)))
        }
      } else {
        (name, None)
      }

    val path: Path = Paths.get(canonicalFileName)
    val stagedPath: Path =
      try {
        SparkConnectUtils.concatenatePaths(stagingDir, path)
      } catch {
        case _: IllegalArgumentException =>
          throw new IllegalArgumentException(
            s"Artifact with name: $name is invalid. The `name` " +
              s"must be a relative path and cannot reference parent/sibling/nephew directories."
          )
        case NonFatal(e) => throw e
      }

    Files.createDirectories(stagedPath.getParent)

    private val fileOut = Files.newOutputStream(stagedPath)
    private val countingOut = new CountingOutputStream(fileOut)
    private val checksumOut = new CheckedOutputStream(countingOut, new CRC32)

    private val builder = ArtifactSummary.newBuilder().setName(name)
    private var artifactSummary: ArtifactSummary = _
    protected var isCrcSuccess: Boolean = _

    protected def updateCrc(isSuccess: Boolean): Unit = {
      isCrcSuccess = isSuccess
    }

    def getCrcStatus: Option[Boolean] = Option(isCrcSuccess)

    def write(dataChunk: AddArtifactsRequest.ArtifactChunk): Unit = {
      try dataChunk.getData.writeTo(checksumOut)
      catch {
        case NonFatal(e) =>
          close()
          throw e
      }
      updateCrc(checksumOut.getChecksum.getValue == dataChunk.getCrc)
      checksumOut.getChecksum.reset()
    }

    def close(): Unit = {
      if (artifactSummary == null) {
        checksumOut.close()
        artifactSummary = builder
          .setName(name)
          .setIsCrcSuccessful(getCrcStatus.getOrElse(false))
          .build()
      }
    }

    def summary(): ArtifactSummary = {
      require(artifactSummary != null)
      artifactSummary
    }
  }

  class StagedChunkedArtifact(name: String, numChunks: Long, totalBytes: Long)
    extends StagedArtifact(name) {
    private var remainingChunks = numChunks
    private var totalBytesProcessed = 0L
    private var isFirstCrcUpdate = true

    def isFinished: Boolean = remainingChunks == 0

    override protected def updateCrc(isSuccess: Boolean): Unit = {
      isCrcSuccess = isSuccess && (isCrcSuccess || isFirstCrcUpdate)
      isFirstCrcUpdate = false
    }

    override def write(dataChunk: AddArtifactsRequest.ArtifactChunk): Unit = {
      if (remainingChunks == 0) {
        throw new KyuubiException(
          s"Excessive data chunks for artifact: $name, " +
            s"expected $numChunks chunks in total. Processed $totalBytesProcessed bytes out of" +
            s" $totalBytes bytes.")
      }
      super.write(dataChunk)
      totalBytesProcessed += dataChunk.getData.size()
      remainingChunks -= 1
    }

    override def close(): Unit = {
      if (remainingChunks !=0 || totalBytesProcessed != totalBytes) {
        throw new KyuubiException(
          s"Missing data chunks for artifact: $name. Expected " +
            s"$numChunks chunks and received ${numChunks - remainingChunks} chunks. Processed" +
            s" $totalBytesProcessed bytes out of $totalBytes bytes.")
      }
      super.close()
    }
  }

}
