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

package org.apache.spark.sql.execution.compactor

import java.io.FileNotFoundException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.ipc.RemoteException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.compactor.CompactorFileFormat.CompactorFileFormat

trait Compactor extends Serializable with Logging {
  def compact(
      conf: Configuration,
      compactSrcFiles: Array[Path],
      compactTempFile: Path,
      compactTargetFile: Path): Unit = {
    logWarning(
      s"""
         |Compacting
         |From: ${compactSrcFiles.mkString("[", ",", "]")}
         |To: $compactTargetFile
         |Temp: $compactTempFile
         |""".stripMargin)
    val fileSystem = compactTargetFile.getFileSystem(conf)
    if (fileSystem.exists(compactTargetFile)) {
      logWarning(compactTargetFile + " already exist, won't compact")
      //  won't throw exception so can continue delete source files.
    } else {
      if (fileSystem.exists(compactTempFile)) {
        logWarning(compactTempFile + " exist, delete and re-compact ")
        fileSystem.delete(compactTempFile, false)
      }

      var compactSucceed = false
      try {
        compactInternal(conf, compactSrcFiles, compactTempFile)
        compactSucceed = true
      } catch {
        case re: RemoteException =>
          logError(
            s"""
               |Compacting failed
               |From: ${compactSrcFiles.mkString("[", ",", "]")}
               |To: ${compactTargetFile}
               |Temp: ${compactTempFile}
               |""".stripMargin,
            re)
          fileSystem.delete(compactTempFile, false)
          if (re.getClassName.equals("java.io.FileNotFoundException")
            && !re.getMessage.contains(compactTempFile) && fileSystem.exists(compactTargetFile)) {
            logWarning("Compact failed, should be another attempt has succeed, " +
              "delete temp file, message:" + re.toString)
            //  won't throw exception so can continue delete source files
          } else {
            throw re
          }
        case fnfe: FileNotFoundException =>
          logError(
            s"""
               |Compacting failed
               |From: ${compactSrcFiles.mkString("[", ",", "]")}
               |To: ${compactTargetFile}
               |Temp: ${compactTempFile}
               |""".stripMargin,
            fnfe)
          fileSystem.delete(compactTempFile, false)
          if (!fnfe.getMessage.contains(compactTempFile) && fileSystem.exists(compactTargetFile)) {
            logWarning("Compact failed, should be another attempt has succeed, " +
              "delete temp file, message:" + fnfe.toString)
            //  won't throw exception so can continue delete source files
          }
        case e: Exception =>
          fileSystem.delete(compactTempFile, false)
          throw e
      }

      try {
        if (compactSucceed) {
          fileSystem.rename(compactTempFile, compactTargetFile)
        }
      } catch {
        case e: Exception =>
          fileSystem.delete(compactTempFile, false)
          if (fileSystem.exists(compactTargetFile)) {
            logWarning(s"$compactTargetFile already exist, " +
              s"should be another attempt has succeed, delete temp file")
            //  won't throw exception so can continue delete source files
          } else {
            throw e
          }
      }
    }

    compactSrcFiles.foreach { path =>
      try {
        logInfo("Delete compact source file " + path)
        val deleteSucceed = fileSystem.delete(path, false)
        if (!deleteSucceed && fileSystem.exists(path)) {
          throw new IllegalStateException("Failed to delete file " + path)
        }
        //  delete empty ancestor directory
        var parentPath = path.getParent
        var siblingStatuses = fileSystem.listStatus(parentPath)
        while (siblingStatuses.length == 0) {
          logInfo("Delete empty source directory " + parentPath)
          fileSystem.delete(parentPath, true)
          parentPath = parentPath.getParent
          siblingStatuses = fileSystem.listStatus(parentPath)
        }
      } catch {
        case e: Exception =>
          if (fileSystem.exists(path)) {
            throw e
          }
      }
    }
  }

  protected def compactInternal(conf: Configuration, src: Array[Path], dest: Path)
}

object Compactor extends Serializable {
  def getCompactor(fileFormat: CompactorFileFormat): Compactor = {
    fileFormat match {
      case CompactorFileFormat.ORC => new OrcCompactor
      case CompactorFileFormat.PARQUET => new ParquetCompactor
      case _ => throw new IllegalArgumentException("Not supported file format: " + fileFormat)
    }
  }
}
