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

package org.apache.kyuubi.spark.connector.hive.write

import java.io.IOException
import java.net.URI
import java.text.SimpleDateFormat
import java.util.{Date, Locale, Random}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.common.FileUtils
import org.apache.hadoop.hive.ql.exec.TaskRunner
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogWithListener
import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper.HiveExternalCatalog

import org.apache.kyuubi.util.SemanticVersion

// scalastyle:off line.size.limit
/**
 * A helper for v2 hive writer.
 *
 * Mostly copied from https://github.com/apache/spark/blob/master/sql/hive/src/main/scala/org/apache/spark/sql/hive/execution/SaveAsHiveFile.scala
 */
// scalastyle:on line.size.limit
object HiveWriteHelper extends Logging {

  private val hiveStagingDir = "hive.exec.stagingdir"

  private val hiveScratchDir = "hive.exec.scratchdir"

  def getExternalTmpPath(
      externalCatalog: ExternalCatalogWithListener,
      hadoopConf: Configuration,
      path: Path): Path = {

    // Before Hive 1.1, when inserting into a table, Hive will create the staging directory under
    // a common scratch directory. After the writing is finished, Hive will simply empty the table
    // directory and move the staging directory to it.
    // After Hive 1.1, Hive will create the staging directory under the table directory, and when
    // moving staging directory to table directory, Hive will still empty the table directory, but
    // will exclude the staging directory there.
    // We have to follow the Hive behavior here, to avoid troubles. For example, if we create
    // staging directory under the table director for Hive prior to 1.1, the staging directory will
    // be removed by Hive when Hive is trying to empty the table directory.
    val hiveVersion = SemanticVersion(
      externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog].client.version.fullVersion)
    val stagingDir = hadoopConf.get(hiveStagingDir, ".hive-staging")
    val scratchDir = hadoopConf.get(hiveScratchDir, "/tmp/hive")

    if (hiveVersion < "1.1") {
      oldVersionExternalTempPath(path, hadoopConf, scratchDir)
    } else {
      newVersionExternalTempPath(path, hadoopConf, stagingDir)
    }
  }

  private def executionId: String = {
    val rand: Random = new Random
    val format = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss_SSS", Locale.US)
    "hive_" + format.format(new Date) + "_" + Math.abs(rand.nextLong)
  }

  // Mostly copied from Context.java#getExternalTmpPath of Hive 0.13
  private def oldVersionExternalTempPath(
      path: Path,
      hadoopConf: Configuration,
      scratchDir: String): Path = {
    val extURI: URI = path.toUri
    val scratchPath = new Path(scratchDir, executionId)
    var dirPath = new Path(
      extURI.getScheme,
      extURI.getAuthority,
      scratchPath.toUri.getPath + "-" + TaskRunner.getTaskRunnerID)

    try {
      val fs: FileSystem = dirPath.getFileSystem(hadoopConf)
      dirPath = new Path(fs.makeQualified(dirPath).toString)

      if (!FileUtils.mkdir(fs, dirPath, true, hadoopConf)) {
        throw cannotCreateStagingDirError(dirPath.toString)
      }
      fs.deleteOnExit(dirPath)
    } catch {
      case e: IOException =>
        throw cannotCreateStagingDirError(dirPath.toString, e)
    }
    dirPath
  }

  // Mostly copied from Context.java#getExternalTmpPath of Hive 1.2
  private def newVersionExternalTempPath(
      path: Path,
      hadoopConf: Configuration,
      stagingDir: String): Path = {
    val extURI: URI = path.toUri
    if (extURI.getScheme == "viewfs") {
      new Path(getStagingDir(path, hadoopConf, stagingDir), "-ext-10000") // Hive uses 10000
    } else {
      new Path(getExternalScratchDir(extURI, hadoopConf, stagingDir), "-ext-10000")
    }
  }

  private def getExternalScratchDir(
      extURI: URI,
      hadoopConf: Configuration,
      stagingDir: String): Path = {
    getStagingDir(
      new Path(extURI.getScheme, extURI.getAuthority, extURI.getPath),
      hadoopConf,
      stagingDir)
  }

  private[hive] def getStagingDir(
      inputPath: Path,
      hadoopConf: Configuration,
      stagingDir: String): Path = {
    val inputPathName: String = inputPath.toString
    val fs: FileSystem = inputPath.getFileSystem(hadoopConf)
    var stagingPathName: String =
      if (inputPathName.indexOf(stagingDir) == -1) {
        new Path(inputPathName, stagingDir).toString
      } else {
        inputPathName.substring(0, inputPathName.indexOf(stagingDir) + stagingDir.length)
      }

    // SPARK-20594: This is a walk-around fix to resolve a Hive bug. Hive requires that the
    // staging directory needs to avoid being deleted when users set hive.exec.stagingdir
    // under the table directory.
    if (isSubDir(new Path(stagingPathName), inputPath, fs) &&
      !stagingPathName.stripPrefix(inputPathName).stripPrefix("/").startsWith(".")) {
      logDebug(s"The staging dir '$stagingPathName' should be a child directory starts " +
        "with '.' to avoid being deleted if we set hive.exec.stagingdir under the table " +
        "directory.")
      stagingPathName = new Path(inputPathName, ".hive-staging").toString
    }

    val dir: Path =
      fs.makeQualified(
        new Path(stagingPathName + "_" + executionId + "-" + TaskRunner.getTaskRunnerID))
    logDebug("Created staging dir = " + dir + " for path = " + inputPath)
    try {
      if (!FileUtils.mkdir(fs, dir, true, hadoopConf)) {
        throw cannotCreateStagingDirError(dir.toString)
      }
      fs.deleteOnExit(dir)
    } catch {
      case e: IOException =>
        throw cannotCreateStagingDirError(
          s"'${dir.toString}': ${e.getMessage}",
          e)
    }
    dir
  }

  // HIVE-14259 removed FileUtils.isSubDir(). Adapted it from Hive 1.2's FileUtils.isSubDir().
  private def isSubDir(p1: Path, p2: Path, fs: FileSystem): Boolean = {
    val path1 = fs.makeQualified(p1).toString + Path.SEPARATOR
    val path2 = fs.makeQualified(p2).toString + Path.SEPARATOR
    path1.startsWith(path2)
  }

  def cannotCreateStagingDirError(message: String, e: IOException = null): Throwable = {
    new RuntimeException(s"Cannot create staging directory: $message", e)
  }
}
