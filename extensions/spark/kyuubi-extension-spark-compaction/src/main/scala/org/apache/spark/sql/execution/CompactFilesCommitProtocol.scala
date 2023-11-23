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

package org.apache.spark.sql.execution

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.hadoop.mapreduce.{JobContext, JobID, OutputCommitter, TaskAttemptContext, TaskID, TaskType}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, TextOutputFormat}
import org.apache.orc.mapreduce.OrcOutputFormat
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.spark.TaskContext
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.CompactConf._
import org.apache.spark.sql.execution.compactor.Compactor
import org.apache.spark.sql.execution.compactor.CompactorFileFormat._
import org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol
import org.apache.spark.util.SerializableConfiguration

class CompactFilesCommitProtocol(
    jobId: String,
    path: String,
    dynamicPartitionOverwrite: Boolean = false)
  extends SQLHadoopMapReduceCommitProtocol(jobId, path, dynamicPartitionOverwrite) {

  private val IN_PROGRESS_PREFIX = "_"
  private val IN_PROGRESS_SUFFIX = ".inprogress"

  @transient private var fileOutputCommitter: FileOutputCommitter = _
  private var fileFormat: Option[CompactorFileFormat] = None
  type FileGroup = mutable.Set[(String, Array[(String, Long)])]
  type LeafFileGroup = mutable.HashMap[String, FileGroup]
  type LeafFileGroupMultiMapTrait = mutable.MultiMap[String, (String, Array[(String, Long)])]

  override def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit = {
    compactFiles(jobContext)
    super.commitJob(jobContext, taskCommits)
  }

  // overwrite setupCommitter in order to get committer.
  override protected def setupCommitter(context: TaskAttemptContext): OutputCommitter = {
    // TODO: Current code only support v1 yet, will support v2 later
    context.getConfiguration.setInt("mapreduce.fileoutputcommitter.algorithm.version", 1)
    val outputCommitter: OutputCommitter = super.setupCommitter(context)
    outputCommitter match {
      case committer: FileOutputCommitter =>
        fileOutputCommitter = committer
      case _ =>
        throw new IllegalStateException(
          "Cannot compact small files of " + outputCommitter.getClass)
    }
    fileFormat = context.getOutputFormatClass.getConstructor().newInstance() match {
      case _: ParquetOutputFormat[_] => Some(PARQUET)
      case _: OrcOutputFormat[_] => Some(ORC)
      case _: TextOutputFormat[_, _] =>
        context.getConfiguration.get("mapred.output.format.class", "") match {
          case "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat" => Some(ORC)
          case "org.apache.orc.mapred.OrcOutputFormat" => Some(ORC)
          case "org.apache.parquet.hadoop.ParquetOutputFormat" => Some(PARQUET)
          case "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat" => Some(PARQUET)
          case _ => None
        }
      case _ => None
    }
    outputCommitter
  }


  private def compactFiles(jobContext: JobContext): Unit = {
    if (fileFormat.isEmpty) {
      logWarning("Not able to detect file format, bypass small file compact")
    } else {
      val jobAttemptPath = fileOutputCommitter.getJobAttemptPath(jobContext)
      val fs = jobAttemptPath.getFileSystem(jobContext.getConfiguration)
      val committedTaskPaths =
        fs.listStatus(jobAttemptPath, new CommittedTaskFilter()).map(_.getPath)
      logInfo(s"Detect compact file format: ${fileFormat.get} in path $jobAttemptPath")
      logInfo(s"Committed task paths: " +
        s"${committedTaskPaths.map(_.toString).take(10).mkString(", ")}")
      val sparkSession: SparkSession =
        SparkSession.getActiveSession.getOrElse(SparkSession.getDefaultSession.get)
      val leafDirPathsGroups = getAllGroupedLeafDirPaths(
        sparkSession,
        fs,
        committedTaskPaths,
        sparkSession.conf.get(COMPACT_PREPARE_PARALLELISM))

      val compactTaskSlices =
        sliceCompactTasks(
          leafDirPathsGroups,
          sparkSession.conf.get(COMPACT_TARGET_SIZE),
          sparkSession.conf.get(COMPACT_FILE_THRESHOLD))
      if (compactTaskSlices.isEmpty) {
        logWarning("No compact tasks required.")
      } else {
        val compactTaskSlipRdd =
          sparkSession.sparkContext.parallelize(compactTaskSlices, compactTaskSlices.length)
        val beforeCompactTaskIds = fs.listStatus(jobAttemptPath).map(_.getPath.getName)
        // include _temporary actually.
        sparkSession.sparkContext.runJob(
          compactTaskSlipRdd,
          (taskContext: TaskContext, iter: Iterator[(Array[String], String)]) => {
            val jobTrackerID = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US).format(new Date)
            val jobId = new JobID(jobTrackerID, taskContext.stageId())
            val taskId = new TaskID(jobId, TaskType.MAP, taskContext.partitionId())

            iter.foreach { case (srcRelativePaths, destRelativePath) =>
              val compactTargetFilePath = new Path(destRelativePath)
              val compactTempFileName =
                new Path(
                  compactTargetFilePath.getParent,
                  IN_PROGRESS_PREFIX + compactTargetFilePath.getName
                    + "." + taskContext.attemptNumber() + IN_PROGRESS_SUFFIX)
              val compactTaskPath = new Path(jobAttemptPath, taskId.toString)
              val compactTempFile = new Path(compactTaskPath, compactTempFileName)
              val compactTargetFile = new Path(compactTaskPath, destRelativePath)
              Compactor.getCompactor(fileFormat.get).compact(
                new SerializableConfiguration(jobContext.getConfiguration).value,
                srcRelativePaths.map(new Path(_)),
                compactTempFile,
                compactTargetFile)
            }

            taskId.toString
          })

        // Clean all compact task's remain temp files
        val afterCompactTaskIds = fs.listStatus(jobAttemptPath).map(_.getPath.getName)
        val compactTaskIds = afterCompactTaskIds.filterNot(beforeCompactTaskIds.contains)

        logInfo("Clean temp files")
        compactTaskIds.foreach { taskId =>
          val compactTaskWorkingDir = new Path(jobAttemptPath, taskId)
          if (fs.exists(compactTaskWorkingDir)) {
            cleanTempFiles(fs, compactTaskWorkingDir)
          }
        }

        logInfo("Compacted " + compactTaskSlices.map(_._1.length).sum +
          " files to " + compactTaskSlices.length + " files")
      }
    }
  }

  private def cleanTempFiles(fs: FileSystem, compactTaskWorkingDir: Path): Unit = {
    fs.listStatus(compactTaskWorkingDir).foreach { fileStatus =>
      if (fileStatus.isDirectory) {
        cleanTempFiles(fs, fileStatus.getPath)
      } else if (fileStatus.isFile) {
        val fileName = fileStatus.getPath.getName
        if (fileName.startsWith(IN_PROGRESS_PREFIX) && fileName.endsWith(IN_PROGRESS_SUFFIX)) {
          logInfo("Delete temp file " + fileStatus.getPath)
          fs.delete(fileStatus.getPath, false)
        }
      }
    }
  }

  private def getAllGroupedLeafDirPaths(
      sparkSession: SparkSession,
      fileSystem: FileSystem,
      committedTaskPaths: Array[Path],
      parallelism: Int): LeafFileGroup = {
    val result = new LeafFileGroup with LeafFileGroupMultiMapTrait

    // parallelism in executor, Path object cannot be serialized, here we convert to string
    val committedTaskPathRdd =
      sparkSession.sparkContext.parallelize(committedTaskPaths.map(_.toString), parallelism)
    val serializableHadoopConf = new SerializableConfiguration(fileSystem.getConf)

    val ret = new Array[LeafFileGroup](
      committedTaskPathRdd.partitions.length)
    sparkSession.sparkContext.runJob(
      committedTaskPathRdd,
      (_: TaskContext, iter: Iterator[String]) => {
        getAllGroupedLeafDirPathsOfTaskCommittedPaths(serializableHadoopConf.value, iter)
      },
      committedTaskPathRdd.partitions.indices,
      (index, res: LeafFileGroup) => ret(index) = res)

    ret.foreach { committedLeafDirectoryPathOfTask =>
      committedLeafDirectoryPathOfTask.foreach {
        case (committedLeafDirRelativePathKey, committedLeafDirRelativePathGroup) =>
          committedLeafDirRelativePathGroup.foreach {
            case (parentDir, childPathAndLength) =>
              result.addBinding(committedLeafDirRelativePathKey, (parentDir, childPathAndLength))
          }
      }
    }

    result
  }

  private def getAllGroupedLeafDirPathsOfTaskCommittedPaths(
      configuration: Configuration,
      committedTaskDirs: Iterator[String])
      : LeafFileGroup = {
    //  [String, mutable.Set[(String, Array[(String, Long)])]] =>
    //  [hivePartitionKey, mutable.Set[(hivePartitionPathOfTask, Array[(filePath, fileLength)])]]
    val result = new LeafFileGroup with LeafFileGroupMultiMapTrait
    committedTaskDirs.map { committedTaskDir: String =>
      val committedTaskPath = new Path(committedTaskDir)
      val fileSystem = committedTaskPath.getFileSystem(configuration)
      val committedTaskFileStatus = fileSystem.getFileStatus(committedTaskPath)
      if (committedTaskFileStatus.isFile) {
        throw new IllegalStateException(
          committedTaskDir + "should be a directory rather than a file.")
      } else if (committedTaskFileStatus.isDirectory) {
        getAllGroupedLeafDirPathsOfEachTaskCommittedPath(
          fileSystem,
          fileSystem.listStatus(committedTaskPath),
          committedTaskFileStatus)
      } else {
        //  ignore symlink case
        null
      }
    }.filter(_ != null).foreach { committedLeafDirectoryPathOfTask =>
      committedLeafDirectoryPathOfTask.foreach {
        case (committedLeafDirRelativePathKey, committedLeafDirRelativePathGroup) =>
          committedLeafDirRelativePathGroup.foreach {
            case (parentDir, childPathAndLength) =>
              result.addBinding(committedLeafDirRelativePathKey, (parentDir, childPathAndLength))
          }
      }
    }
    result
  }

  /**
   * For case 1:
   * /path/task_commit_path/
   *           |------ part-000-12345 (length 3)
   *           |------ part-001-12345 (length 4)
   *           |------ part-002-12345 (length 2)
   *
   * Return:
   * {
   *     "/" ->
   *         (
   *             "/path/task_commit_path",
   *             Array(
   *                 ("/path/task_commit_path/part-000-12345", 3),
   *                 ("/path/task_commit_path/part-001-12345", 4)
   *                 ("/path/task_commit_path/part-002-12345", 2)
   *             )
   *         )
   *  }
   *
   * For case 2:
   * /path/task_commit_path/
   *           /-----part=p1
   *           |       |------ part-000-12345 (length 3)
   *           |       |------ part-002-12345 (length 4)
   *           |       |------ part-003-12345 (length 2)
   *           |
   *           |-----part=p2
   *           |       |------ part-004-12345 (length 2)
   *           |       |------ part-005-12345 (length 3)
   * Return:
   * {
   *     "/part=p1" ->
   *         (
   *             "/path/task_commit_path/path=p1",
   *              Array(
   *                  ("/path/task_commit_path/path=p1/part-000-12345", 3),
   *                  ("/path/task_commit_path/path=p1/part-001-12345", 4)
   *                  ("/path/task_commit_path/path=p1/part-002-12345", 2)
   *              )
   *          ),
   *     "/part=p2" ->
   *         (
   *             "/path/task_commit_path/path=p2",
   *                 Array(
   *                     ("/path/task_commit_path/path=p2/part-004-12345", 2),
   *                     ("/path/task_commit_path/path=p2/part-005-12345", 3)
   *                  )
   *          )
   * }
   */
  private def getAllGroupedLeafDirPathsOfEachTaskCommittedPath(
      fileSystem: FileSystem,
      committedTaskPaths: Array[FileStatus],
      committedTaskPath: FileStatus, // First level
      parentResult: LeafFileGroup with LeafFileGroupMultiMapTrait = null)
      : LeafFileGroup with LeafFileGroupMultiMapTrait = {

    val result = if (parentResult == null) {
      new LeafFileGroup with LeafFileGroupMultiMapTrait
    } else {
      parentResult
    }

    breakable {
      committedTaskPaths.foreach { stat =>
        if (stat.isFile) {
          val parentDirectory = stat.getPath.getParent
          val leafPathKey =
            parentDirectory.toString.replaceFirst(committedTaskPath.getPath.toString, "")
          val slashedLeafPathKey = if (leafPathKey.startsWith("/")) {
            leafPathKey
          } else {
            "/" + leafPathKey
          }
          //  ensure all siblings are files
          committedTaskPaths.foreach { stat =>
            if (!stat.isFile) {
              throw new IllegalStateException(stat.getPath + " should be a file")
            }
          }
          result.addBinding(
            slashedLeafPathKey,
            (parentDirectory.toString,
              committedTaskPaths.map(stat => (stat.getPath.toString, stat.getLen))))
          break()
        } else if (stat.isDirectory) {
          getAllGroupedLeafDirPathsOfEachTaskCommittedPath(
            fileSystem,
            fileSystem.listStatus(stat.getPath),
            committedTaskPath,
            result)
        }
        //  ignore symlink case
      }
    }
    result
  }

  private def sliceCompactTasks(
      leafDirPathsGroups: LeafFileGroup,
      compactTargetSize: Long,
      smallFileThreshold: Long): Seq[(Array[String], String)] = {
    if (smallFileThreshold >= compactTargetSize) {
      throw new IllegalArgumentException(
        s"${COMPACT_FILE_THRESHOLD.key} could not larger than ${COMPACT_TARGET_SIZE.key}")
    }
    leafDirPathsGroups.flatMap { case (leafDirPathKey, leafDirPathsGroup) =>
      getCompactTaskSlips(
        leafDirPathKey,
        leafDirPathsGroup,
        compactTargetSize,
        smallFileThreshold)
    }.toSeq
  }

  private def getCompactTaskSlips(
      leafDirPathKey: String,
      leafDirPathsGroup: FileGroup,
      compactSize: Long,
      smallfileSize: Long): Seq[(Array[String], String)] = {
    val result = new ListBuffer[(Array[String], String)]()

    var fileSlices = new ListBuffer[String]()
    var slipTotalSize: Long = 0
    //  files in path of one same group should be compact together
    leafDirPathsGroup.foreach { case (parentPath, files) =>
      files.foreach { case (file, length) =>
        if (length < smallfileSize) {
          fileSlices += file
          slipTotalSize = slipTotalSize + length
          if (slipTotalSize >= compactSize) {
            result += (
              fileSlices.toArray,
              getCompactFileRelativePath(leafDirPathKey, fileSlices))

            fileSlices = new ListBuffer[String]()
            slipTotalSize = 0
          }
        }
      }
    }
    if (slipTotalSize > 0 && fileSlices.length > 1) {
      result += (
        fileSlices.toArray,
        getCompactFileRelativePath(leafDirPathKey, fileSlices))
    }

    result
  }

  private def getCompactFileRelativePath(
      leafDirPathKey: String,
      fileSlices: ListBuffer[String]): String = {
    val headPath = new Path(fileSlices.head)

    val compactFileName = headPath.getName.replaceFirst("part", "compact")
    new Path(leafDirPathKey, compactFileName).toString
  }

  private class CommittedTaskFilter extends PathFilter {
    override def accept(path: Path): Boolean = {
      !(FileOutputCommitter.PENDING_DIR_NAME == path.getName)
    }
  }
}
