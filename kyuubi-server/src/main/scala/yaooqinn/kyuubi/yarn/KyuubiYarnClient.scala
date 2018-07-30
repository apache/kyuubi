/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.yarn

import java.io.{FileSystem => _, _}
import java.net.URI
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.PrivilegedExceptionAction
import java.text.SimpleDateFormat
import java.util.{Locale, Properties, UUID}
import java.util.concurrent.Callable
import java.util.zip.{ZipEntry, ZipOutputStream}

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap, ListBuffer, Map}
import scala.util.control.NonFatal

import com.google.common.io.Files
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.mapred.Master
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.util.StringUtils
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.{YarnClient, YarnClientApplication}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.{ApplicationNotFoundException, YarnException}
import org.apache.hadoop.yarn.util.Records
import org.apache.spark.{KyuubiConf, KyuubiSparkUtil, SparkConf}
import org.apache.spark.deploy.yarn.KyuubiDistributedCacheManager

import yaooqinn.kyuubi.{Logging, _}

/**
 * A Yarn Client for submitting Kyuubi towards Yarn
 * @param conf SparkConf
 */
private[yarn] class KyuubiYarnClient(conf: SparkConf) extends Logging {
  import KyuubiYarnClient._

  private[this] val hadoopConf = new YarnConfiguration(KyuubiSparkUtil.newConfiguration(conf))

  private[this] val yarnClient = YarnClient.createYarnClient()
  yarnClient.init(hadoopConf)
  yarnClient.start()

  private[this] var memory = conf.getSizeAsMb(KyuubiSparkUtil.DRIVER_MEM, "1024m").toInt
  private[this] var memoryOverhead =
    conf.getSizeAsMb(KyuubiSparkUtil.DRIVER_MEM_OVERHEAD, (memory * 0.1).toInt + "m").toInt
  private[this] val cores = conf.getInt(KyuubiSparkUtil.DRIVER_CORES,
    YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES)
  private[this] val principal = conf.get(KyuubiSparkUtil.PRINCIPAL, "")
  private[this] val keytabOrigin = conf.get(KyuubiSparkUtil.KEYTAB, "")
  private[this] val loginFromKeytab = principal.nonEmpty && keytabOrigin.nonEmpty
  private[this] val keytabForAM: String = if (loginFromKeytab) {
    new File(keytabOrigin).getName + "-" + UUID.randomUUID()
  } else {
    null
  }

  private[this] val user: UserGroupInformation = UserGroupInformation.getCurrentUser
  private[this] val creds = user.getCredentials
  private[this] val stagingHome = FileSystem.get(hadoopConf).getHomeDirectory
  private[this] var appId: ApplicationId = _
  private[this] var appStagingDir: Path = _

  @throws[IOException]
  @throws[YarnException]
  def submit(): Unit = {
    try {
      val app = yarnClient.createApplication()
      val appRes = app.getNewApplicationResponse
      val maxMemory = appRes.getMaximumResourceCapability.getMemory
      if (memory + memoryOverhead > maxMemory) {
        warn(s"Requesting AM memory(${memory}m + ${memoryOverhead}m) exceeds cluster's limit")
        memory = (maxMemory * 0.9).toInt
        memoryOverhead = (maxMemory * 0.1).toInt
        warn(s"Adjusting Kyuubi container to ${memory}B heap and ${memoryOverhead}m overhead.")
      } else {
        info(s"Will allocate Kyuubi container with ${memory}m heap and ${memoryOverhead}m overhead")
      }
      appId = appRes.getApplicationId
      appStagingDir = new Path(stagingHome, buildPath(KYUUBI_STAGING, appId.toString))
      val containerContext = createContainerLaunchContext()
      val appContext = createApplicationSubmissionContext(app, containerContext)
      info(s"Submitting application $appId to ResourceManager")
      yarnClient.submitApplication(appContext)
      while(stateChecker.call()) {
        Thread.sleep(1000)
      }
    } catch {
      case e: Throwable =>
        if (appId != null) cleanupStagingDir()
        throw e
    }
  }

  /**
   * Cleanup application staging directory.
   */
  private[this] def cleanupStagingDir(): Unit = {
    def cleanupStagingDirInternal(): Unit = {
      try {
        val fs = appStagingDir.getFileSystem(hadoopConf)
        if (fs.delete(appStagingDir, true)) {
          info(s"Deleted staging directory $appStagingDir")
        }
      } catch {
        case ioe: IOException =>
          warn("Failed to cleanup staging dir " + appStagingDir, ioe)
      } finally {
        killApplication()
      }
    }

    def killApplication(): Unit = {
      try {
        yarnClient.killApplication(appId)
      } catch {
        case NonFatal(e) =>
          error(s"Failed to kill application $appId", e)
      }
    }

    if (loginFromKeytab) {
      val currentUser = UserGroupInformation.getCurrentUser
      currentUser.reloginFromKeytab()
      currentUser.doAs(new PrivilegedExceptionAction[Unit] {
        override def run(): Unit = {
          cleanupStagingDirInternal()
        }
      })
    } else {
      cleanupStagingDirInternal()
    }
  }

  /**
   * Describe container information and create the context for launching Application Master
   *
   * @return ContainerLaunchContext which contains all information for NodeManager to launch
   *         Application Master
   */
  private[this] def createContainerLaunchContext(): ContainerLaunchContext = {
    val launchEnv = setupLaunchEnv()
    val localResources = prepareLocalResources()

    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])
    amContainer.setLocalResources(localResources.asJava)
    amContainer.setEnvironment(launchEnv.asJava)

    val javaOpts = ListBuffer[String]()
    javaOpts += "-Xmx" + memory + "m"
    javaOpts += "-Djava.io.tmpdir=" +
      buildPath(Environment.PWD.$$(), YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR)

    conf.getOption(KyuubiSparkUtil.AM_EXTRA_JAVA_OPTIONS).foreach { opts =>
      javaOpts ++= KyuubiSparkUtil.splitCommandString(opts)
        .map(KyuubiSparkUtil.substituteAppId(_, appId.toString))
        .map(KyuubiSparkUtil.escapeForShell)
    }

    val amClass = classOf[KyuubiAppMaster].getCanonicalName

    val amArgs = Seq(amClass) ++
      Seq("--properties-file", buildPath(Environment.PWD.$$(), SPARK_CONF_DIR, SPARK_CONF_FILE))

    val commands =
      Seq(Environment.JAVA_HOME.$$() + "/bin/java", "-server") ++
      javaOpts ++ amArgs ++
      Seq(
        "1>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout",
        "2>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")

    val printableCommands = commands.map(s => if (s == null) "null" else s).toList
    amContainer.setCommands(printableCommands.asJava)
    info(List.fill(100)("=").mkString)
    info("Kyuubi Application Master launch context:")
    info(s"    Main class: $amClass")
    info("    Environments:")
    launchEnv.foreach { case (k, v) =>
        info(s"        $k -> $v")
    }
    info("    Resources:")
    localResources.foreach { case (k, v) =>
        info(s"        $k -> $v")
    }
    info("    Command:")
    info(s"        ${printableCommands.mkString(" ")}")
    info(List.fill(100)("=").mkString)

    val dob = new DataOutputBuffer()
    creds.writeTokenStorageToStream(dob)
    amContainer.setTokens(ByteBuffer.wrap(dob.getData))

    amContainer.setApplicationACLs(getACLs.toMap.asJava)
    amContainer
  }

  /**
   * Set up the context for submitting our ApplicationMaster.
   */
  private[this] def createApplicationSubmissionContext(
      app: YarnClientApplication,
      containerContext: ContainerLaunchContext): ApplicationSubmissionContext = {
    val appContext = app.getApplicationSubmissionContext
    appContext.setApplicationName(s"$KYUUBI_YARN_APP_NAME[$KYUUBI_VERSION]")
    appContext.setQueue(conf.get(KyuubiSparkUtil.QUEUE, YarnConfiguration.DEFAULT_QUEUE_NAME))
    appContext.setAMContainerSpec(containerContext)
    appContext.setApplicationType(KYUUBI_YARN_APP_TYPE)
    conf.getOption(KyuubiSparkUtil.MAX_APP_ATTEMPTS) match {
      case Some(v) => appContext.setMaxAppAttempts(v.toInt)
      case None => debug(s"${KyuubiSparkUtil.MAX_APP_ATTEMPTS} is not set. " +
        "Cluster's default value will be used.")
    }
    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(memory + memoryOverhead)
    capability.setVirtualCores(cores)
    appContext.setResource(capability)
    appContext
  }

  /**
   * Get acls for Kyuubi Application Master
   */
  private[this] def getACLs: Seq[(ApplicationAccessType, String)] = {
    val yarnAcls = hadoopConf.get(YarnConfiguration.YARN_ADMIN_ACL)
    val adminAcl = if (yarnAcls == null) {
      user.getShortUserName
    } else if (yarnAcls == "*") {
      "*"
    } else {
      (yarnAcls.split(',').map(_.trim).filter(_.nonEmpty).toSet +
        user.getShortUserName).mkString(",")
    }

    Seq(
      ApplicationAccessType.MODIFY_APP -> adminAcl,
      ApplicationAccessType.VIEW_APP -> adminAcl)
  }

  /**
   * Set up the environment for launching the ApplicationMaster which wraps Kyuubi Server.
   */
  private[this] def setupLaunchEnv(): EnvMap = {
    info("Setting up the launch environment for our Kyuubi Server container")
    val env = new EnvMap
    populateClasspath(hadoopConf, env)
    env("KYUUBI_YARN_STAGING_DIR") = appStagingDir.toString
    // Point to Localized Spark jars
    env("KYUUBI_YARN_SPARK_JARS") = buildPath(Environment.PWD.$$(), SPARK_LIB_DIR, "*")
    val amEnvPrefix = "spark.kyuubi.yarn.appMasterEnv."
    conf.getAll
      .filter(_._1.startsWith(amEnvPrefix))
      .map { case (k, v) => (k.stripPrefix(amEnvPrefix), v) }
      .foreach { case (k, v) => addPathToEnvironment(env, k, v) }
    env
  }

  @throws[IOException]
  private[this] def prepareLocalResources(): HashMap[String, LocalResource] = {
    info("Preparing resources for our AM container to start Kyuubi Server")
    val fs = appStagingDir.getFileSystem(hadoopConf)
    val tokenRenewer = Master.getMasterPrincipal(hadoopConf)
    debug("Delegation token renewer is: " + tokenRenewer)
    if (tokenRenewer == null || tokenRenewer.length() == 0) {
      error("Can't get any Master Kerberos principal for use as renewer.")
    } else {
      fs.addDelegationTokens(tokenRenewer, creds)
    }
    val localResources = new HashMap[String, LocalResource]
    FileSystem.mkdirs(fs, appStagingDir, new FsPermission(STAGING_DIR_PERMISSION))
    val symlinkCache = HashMap[URI, Path]()
    val statCache = HashMap[URI, FileStatus]()

    /**
     * Copy the non-local file to HDFS (if not already there) and add it to the Application Master's
     * local resources
     *
     * @param path the canonical path of the file.
     * @param resType the type of resource being distributed.
     * @param destName the  of the file in the distributed cache.
     * @param targetDir Subdirectory where to place the file.
     */
    def upload(
        path: String,
        resType: LocalResourceType = LocalResourceType.FILE,
        destName: Option[String] = None,
        targetDir: Option[String] = None): Unit = {
      val trimmedPath = path.trim()
      val localURI = KyuubiSparkUtil.resolveURI(trimmedPath)
      if (localURI.getScheme != LOCAL_SCHEME) {
        val localPath = getQualifiedLocalPath(localURI, hadoopConf)
        val linkName = targetDir.map(_ + "/").getOrElse("") +
          destName.orElse(Option(localURI.getFragment)).getOrElse(localPath.getName)
        val destPath = copyFileToRemote(appStagingDir, localPath, symlinkCache)
        val destFs = FileSystem.get(destPath.toUri, hadoopConf)
        KyuubiDistributedCacheManager.addResource(
          destFs, hadoopConf, destPath, localResources, resType, linkName, statCache)
      }
    }

    // Upload keytab if exists
    if (loginFromKeytab) {
      upload(keytabOrigin, destName = Option(keytabForAM))
    }

    // Add KYUUBI jar to the cache
    upload(kyuubiJar)

    // Add Spark to the cache
    val jarsDir = new File(KyuubiSparkUtil.SPARK_JARS_DIR)
    val jarsArchive = File.createTempFile(SPARK_LIB_DIR, ".zip",
      new File(KyuubiSparkUtil.getLocalDir(conf)))
    val jarsStream = new ZipOutputStream(new FileOutputStream(jarsArchive))

    try {
      jarsStream.setLevel(0)
      jarsDir.listFiles().foreach { f =>
        if (f.isFile && f.getName.toLowerCase(Locale.ROOT).endsWith(".jar") && f.canRead) {
          jarsStream.putNextEntry(new ZipEntry(f.getName))
          Files.copy(f, jarsStream)
          jarsStream.closeEntry()
        }
      }
    } finally {
      jarsStream.close()
    }

    upload(jarsArchive.toURI.getPath, resType = LocalResourceType.ARCHIVE,
      destName = Some(SPARK_LIB_DIR))
    jarsArchive.delete()

    val remoteConfArchivePath = new Path(appStagingDir, SPARK_CONF_ARCHIVE)
    val remoteFs = FileSystem.get(remoteConfArchivePath.toUri, hadoopConf)
    val localConfArchive = new Path(createConfArchive().toURI)
    copyFileToRemote(appStagingDir, localConfArchive, symlinkCache,
      destName = Some(SPARK_CONF_ARCHIVE))
    KyuubiDistributedCacheManager.addResource(remoteFs, hadoopConf, remoteConfArchivePath,
      localResources, LocalResourceType.ARCHIVE, SPARK_CONF_DIR, statCache)
    localResources
  }

  private[this] def createConfArchive(): File = {
    val hadoopConfFiles = new HashMap[String, File]()

    // SPARK_CONF_DIR shows up in the classpath before HADOOP_CONF_DIR/YARN_CONF_DIR
    sys.env.get("SPARK_CONF_DIR").foreach { localConfDir =>
      val dir = new File(localConfDir)
      if (dir.isDirectory) {
        val files = dir.listFiles(new FileFilter {
          override def accept(pathname: File): Boolean = {
            pathname.isFile && pathname.getName.endsWith(".xml")
          }
        })
        files.foreach { f => hadoopConfFiles(f.getName) = f }
      }
    }

    val confDirs = Seq("HADOOP_CONF_DIR", "YARN_CONF_DIR")

    confDirs.foreach { envKey =>
      sys.env.get(envKey).foreach { path =>
        val dir = new File(path)
        if (dir.isDirectory) {
          val files = dir.listFiles()
          if (files == null) {
            warn("Failed to list files under directory " + dir)
          } else {
            files.foreach { file =>
              if (file.isFile && !hadoopConfFiles.contains(file getName())) {
                hadoopConfFiles(file.getName) = file
              }
            }
          }
        }
      }
    }

    val confArchive = File.createTempFile(SPARK_CONF_DIR, ".zip",
      new File(KyuubiSparkUtil.getLocalDir(conf)))
    val confStream = new ZipOutputStream(new FileOutputStream(confArchive))

    try {
      confStream.setLevel(0)

      // 1. log4j and metrics
      for { prop <- Seq("log4j.properties", "metrics.properties")
            url <- Option(KyuubiSparkUtil.getContextOrSparkClassLoader.getResource(prop))
            if url.getProtocol == "file" } {
        val file = new File(url.getPath)
        confStream.putNextEntry(new ZipEntry(file.getName))
        Files.copy(file, confStream)
        confStream.closeEntry()
      }

      // 2. hadoop/hive and other xml configurations
      confStream.putNextEntry(new ZipEntry(s"$HADOOP_CONF_DIR/"))
      confStream.closeEntry()
      hadoopConfFiles.foreach { case (name, file) =>
        if (file.canRead) {
          confStream.putNextEntry(new ZipEntry(s"$HADOOP_CONF_DIR/$name"))
          Files.copy(file, confStream)
          confStream.closeEntry()
        }
      }

      // 3. spark conf
      val props = new Properties()
      conf.getAll.foreach { case (k, v) =>
        props.setProperty(k, v)
      }
      // Override spark.yarn.keytab to point to the location in distributed cache which will be used
      // by AM.
      Option(keytabForAM).foreach { k => props.setProperty(KyuubiSparkUtil.KEYTAB, k) }
      confStream.putNextEntry(new ZipEntry(SPARK_CONF_FILE))
      val writer = new OutputStreamWriter(confStream, StandardCharsets.UTF_8)
      props.store(writer, "Spark configuration.")
      writer.flush()
      confStream.closeEntry()
    } finally {
      confStream.close()
    }
    confArchive
  }

  /**
   * Copy the given file to a remote file system (e.g. HDFS) if needed.
   * The file is only copied if the source and destination file systems are different or the source
   * scheme is "file". This is used for preparing resources for launching the ApplicationMaster
   * container.
   */
  private[this] def copyFileToRemote(destDir: Path, srcPath: Path,
      cache: Map[URI, Path], destName: Option[String] = None): Path = {
    val destFs = destDir.getFileSystem(hadoopConf)
    val srcFs = srcPath.getFileSystem(hadoopConf)
    val destPath = new Path(destDir, destName.getOrElse(srcPath.getName))
    info(s"Uploading resource $srcPath -> $destPath")
    FileUtil.copy(srcFs, srcPath, destFs, destPath, false, hadoopConf)
    destFs.setPermission(destPath, new FsPermission(APP_FILE_PERMISSION))
    // Resolve any symlinks in the URI path so using a "current" symlink to point to a specific
    // version shows the specific version in the distributed cache configuration
    val qualifiedDestPath = destFs.makeQualified(destPath)
    val qualifiedDestDir = qualifiedDestPath.getParent
    val resolvedDestDir = cache.getOrElseUpdate(qualifiedDestDir.toUri, {
      val fc = FileContext.getFileContext(qualifiedDestDir.toUri, hadoopConf)
      fc.resolvePath(qualifiedDestDir)
    })
    new Path(resolvedDestDir, qualifiedDestPath.getName)
  }

  private[this] val stateChecker: Callable[Boolean] = new Callable[Boolean] {
    private val timeout = conf.getTimeAsMs(KyuubiConf.YARN_CONTAINER_TIMEOUT.key, "60s")

    import YarnApplicationState._

    override def call(): Boolean = {
      try {
        val report = yarnClient.getApplicationReport(appId)
        val state = report.getYarnApplicationState
        if (state != ACCEPTED) info(formatReportDetails(report))
        info(s"Application report for $appId[$state]")
        state match {
          case RUNNING => false
          case ACCEPTED | NEW | NEW_SAVING | SUBMITTED =>
            if (System.currentTimeMillis - report.getStartTime < timeout) {
              true
            } else {
              error(s"Application $appId failed to start after ${timeout}ms")
              cleanupStagingDir()
              false
            }
          case _ => false
        }
      } catch {
        case _: ApplicationNotFoundException =>
          error(s"Application $appId not found.")
          cleanupStagingDir()
          false
        case NonFatal(e) =>
          error(s"Failed to contact YARN for application $appId.", e)
          false
      }
    }

    def formatReportDetails(report: ApplicationReport): String = {
      Seq(
        ("Client token", Option(report.getClientToAMToken).map(_.toString).getOrElse("")),
        ("Diagnostics", report.getDiagnostics),
        ("Kyuubi server host", report.getHost),
        ("Bind port", report.getRpcPort.toString),
        ("Queue", report.getQueue),
        ("Start time", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(report.getStartTime)),
        ("Final status", Option(report.getFinalApplicationStatus).map(_.toString).getOrElse("")),
        ("Tracking URL", report.getTrackingUrl),
        ("User", report.getUser)
      ).map { case (k, v) =>
        val newValue = Option(v).filter(_.nonEmpty).getOrElse("N/A")
        s"\n\t $k: $newValue"
      }.mkString("")
    }
  }

}

object KyuubiYarnClient {
  /**
   * Load kyuubi server jar, choose the specified kyuubi jar first, try the default compiled one
   * if not found
   */
  private val kyuubiJar =
    Option(System.getenv("KYUUBI_JAR"))
      .getOrElse(KyuubiSparkUtil.getContextOrSparkClassLoader
        .getResource(KYUUBI_JAR_NAME).getPath)

  /**
   * Given a local URI, resolve it and return a qualified local path that corresponds to the URI.
   * This is used for preparing local resources to be included in the container launch context.
   */
  def getQualifiedLocalPath(localURI: URI, hadoopConf: Configuration): Path = {
    val qualifiedURI =
      if (localURI.getScheme == null) {
        // If not specified, assume this is in the local filesystem to keep the behavior
        // consistent with that of Hadoop
        new URI(FileSystem.getLocal(hadoopConf).makeQualified(new Path(localURI)).toString)
      } else {
        localURI
      }
    new Path(qualifiedURI)
  }

  /**
   * Joins all the path components using [[Path.SEPARATOR]].
   */
  def buildPath(components: String*): String = {
    components.mkString(Path.SEPARATOR)
  }

  /**
   * Add a path variable to the given environment map.
   * If the map already contains this key, append the value to the existing value instead.
   */
  def addPathToEnvironment(env: EnvMap, key: String, value: String): Unit = {
    val newValue =
      if (env.contains(key)) {
        env(key) + ApplicationConstants.CLASS_PATH_SEPARATOR + value
      } else {
        value
      }
    env.put(key, newValue)
  }

  /**
   * Add the given path to the classpath entry of the given environment map.
   * If the classpath is already set, this appends the new path to the existing classpath.
   */
  def addClasspathEntry(path: String, env: EnvMap): Unit =
    addPathToEnvironment(env, Environment.CLASSPATH.name, path)

  def populateClasspath(conf: Configuration, env: EnvMap): Unit = {
    addClasspathEntry(Environment.PWD.$$(), env)
    addClasspathEntry(buildPath(Environment.PWD.$$(), SPARK_CONF_DIR), env)
    addClasspathEntry(buildPath(Environment.PWD.$$(), new File(kyuubiJar).getName), env)
    addClasspathEntry(buildPath(Environment.PWD.$$(), SPARK_LIB_DIR, "*"), env)
    populateHadoopClasspath(conf, env)
    addClasspathEntry(buildPath(Environment.PWD.$$(), SPARK_CONF_DIR, HADOOP_CONF_DIR), env)
  }

  /**
   * Populate the classpath entry in the given environment map with any application
   * classpath specified through the Hadoop and Yarn configurations.
   */
  private[yarn] def populateHadoopClasspath(conf: Configuration, env: EnvMap): Unit = {
    val classPathElementsToAdd = getYarnAppClasspath(conf) ++ getMRAppClasspath(conf)
    classPathElementsToAdd.foreach { c =>
      addPathToEnvironment(env, Environment.CLASSPATH.name, c.trim)
    }
  }

  private def getYarnAppClasspath(conf: Configuration): Seq[String] =
    Option(conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH)) match {
      case Some(s) => s.toSeq
      case None => getDefaultYarnApplicationClasspath
    }

  private def getMRAppClasspath(conf: Configuration): Seq[String] =
    Option(conf.getStrings("mapreduce.application.classpath")) match {
      case Some(s) => s.toSeq
      case None => getDefaultMRApplicationClasspath
    }

  private[yarn] def getDefaultYarnApplicationClasspath: Seq[String] =
    YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH.toSeq

  private[yarn] def getDefaultMRApplicationClasspath: Seq[String] =
    StringUtils.getStrings(MRJobConfig.DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH).toSeq

  /**
   * Entrance for Kyuubi On Yarn
   */
  def startKyuubiAppMaster(): Unit = {
    val conf = new SparkConf()
    new KyuubiYarnClient(conf).submit()
  }
}
