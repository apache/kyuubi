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
package org.apache.kyuubi.engine.deploy.yarn

import java.io._
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.security.PrivilegedExceptionAction
import java.util
import java.util.{Locale, Properties, UUID}
import java.util.zip.{ZipEntry, ZipOutputStream}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.mapred.Master
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.{YarnClient, YarnClientApplication}
import org.apache.hadoop.yarn.util.Records

import org.apache.kyuubi.{KyuubiException, Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY
import org.apache.kyuubi.engine.deploy.yarn.EngineYarnModeSubmitter._
import org.apache.kyuubi.util.KyuubiHadoopUtils

abstract class EngineYarnModeSubmitter extends Logging {

  val KYUUBI_ENGINE_STAGING: String = ".kyuubiEngineStaging"

  /*
   * The following variables are used to describe the contents of the
   * ApplicationMaster's working directory. The directory structure is as follows:
   *
   * ApplicationMasterWorkDir/
   * |-- __kyuubi_engine_conf__
   * |   |-- __hadoop_conf__
   * |   |   |-- hadoop conf file1
   * |   |   |-- hadoop conf file2
   * |   |   `-- ...
   * |   `-- __kyuubi_conf__.properties
   * `-- __kyuubi_engine_libs__
   *     |-- kyuubi_engine.jar
   *     `-- ...
   */
  val LOCALIZED_LIB_DIR = "__kyuubi_engine_libs__"
  val LOCALIZED_CONF_DIR = "__kyuubi_engine_conf__"
  val HADOOP_CONF_DIR = "__hadoop_conf__"
  val KYUUBI_CONF_FILE = "__kyuubi_conf__.properties"

  val STAGING_DIR_PERMISSION: FsPermission =
    FsPermission.createImmutable(Integer.parseInt("700", 8).toShort)

  private val applicationMaster = ApplicationMaster.getClass.getName.dropRight(1)

  @volatile private var yarnClient: YarnClient = _
  private var appId: ApplicationId = _

  private[engine] var stagingDirPath: Path = _

  val kyuubiConf = new KyuubiConf()

  var yarnConf: Configuration = _
  var hadoopConf: Configuration = _
  var appUser: String = _
  var keytab: String = _
  var amKeytabFileName: Option[String] = _

  var engineType: String

  def engineMainClass(): String

  /**
   * The extra jars that will be added to the classpath of the engine.
   */
  def engineExtraJars(): Seq[File] = Seq.empty

  def run(): Unit = {
    yarnConf = KyuubiHadoopUtils.newYarnConfiguration(kyuubiConf)
    hadoopConf = KyuubiHadoopUtils.newHadoopConf(kyuubiConf)
    appUser = kyuubiConf.getOption(KYUUBI_SESSION_USER_KEY).orNull
    require(appUser != null, s"$KYUUBI_SESSION_USER_KEY is not set")
    keytab = kyuubiConf.get(ENGINE_KEYTAB).orNull
    val principal = kyuubiConf.get(ENGINE_PRINCIPAL).orNull
    amKeytabFileName =
      if (UserGroupInformation.isSecurityEnabled && principal != null && keytab != null) {
        info(s"Kerberos credentials: principal = $principal, keytab = $keytab")
        UserGroupInformation.loginUserFromKeytab(principal, keytab)
        // Generate a file name that can be used for the keytab file, that does not conflict
        // with any user file.
        Some(new File(keytab).getName() + "-" + UUID.randomUUID().toString)
      } else {
        None
      }

    val ugi = if (UserGroupInformation.getCurrentUser.getShortUserName == appUser) {
      UserGroupInformation.getCurrentUser
    } else {
      UserGroupInformation.createProxyUser(appUser, UserGroupInformation.getCurrentUser)
    }
    ugi.doAs(new PrivilegedExceptionAction[Unit] {
      override def run(): Unit = submitApplication()
    })
  }

  protected def submitApplication(): Unit = {
    try {
      yarnClient = YarnClient.createYarnClient()
      yarnClient.init(yarnConf)
      yarnClient.start()

      debug("Requesting a new application from cluster with %d NodeManagers"
        .format(yarnClient.getYarnClusterMetrics.getNumNodeManagers))

      val newApp = yarnClient.createApplication()
      val newAppResponse = newApp.getNewApplicationResponse
      appId = newAppResponse.getApplicationId

      // The app staging dir based on the STAGING_DIR configuration if configured
      // otherwise based on the users home directory.
      val appStagingBaseDir = kyuubiConf.get(ENGINE_DEPLOY_YARN_MODE_STAGING_DIR)
        .map { new Path(_, UserGroupInformation.getCurrentUser.getShortUserName) }
        .getOrElse(FileSystem.get(hadoopConf).getHomeDirectory())
      stagingDirPath = new Path(appStagingBaseDir, buildPath(KYUUBI_ENGINE_STAGING, appId.toString))

      // Set up the appropriate contexts to launch AM
      val containerContext = createContainerLaunchContext()
      val appContext = createApplicationSubmissionContext(newApp, containerContext)

      // Finally, submit and monitor the application
      info(s"Submitting application $appId to ResourceManager")
      yarnClient.submitApplication(appContext)
      monitorApplication(appId)
    } catch {
      case e: Throwable =>
        if (stagingDirPath != null) {
          cleanupStagingDir()
        }
        throw new KyuubiException("Failed to submit application to YARN", e)
    } finally {
      if (yarnClient != null) {
        yarnClient.stop()
      }
    }
  }

  private def setupSecurityToken(amContainer: ContainerLaunchContext): Unit = {
    if (UserGroupInformation.isSecurityEnabled) {
      val credentials = obtainHadoopFsDelegationToken()
      val serializedCreds = KyuubiHadoopUtils.serializeCredentials(credentials)
      amContainer.setTokens(ByteBuffer.wrap(serializedCreds))
    }
  }

  private def obtainHadoopFsDelegationToken(): Credentials = {
    val tokenRenewer = Master.getMasterPrincipal(hadoopConf)
    info(s"Delegation token renewer is: $tokenRenewer")

    if (tokenRenewer == null || tokenRenewer.isEmpty) {
      val errorMessage = "Can't get Master Kerberos principal for use as renewer."
      error(errorMessage)
      throw new KyuubiException(errorMessage)
    }

    val credentials = new Credentials()
    FileSystem.get(hadoopConf).addDelegationTokens(tokenRenewer, credentials)
    credentials
  }

  private def createContainerLaunchContext(): ContainerLaunchContext = {
    info("Setting up container launch context for engine AM")
    val env = setupLaunchEnv(kyuubiConf)
    val localResources = prepareLocalResources(stagingDirPath, env)

    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])
    amContainer.setLocalResources(localResources.asJava)
    amContainer.setEnvironment(env.asJava)

    val javaOpts = ListBuffer[String]()

    val javaOptions =
      kyuubiConf.get(ENGINE_DEPLOY_YARN_MODE_JAVA_OPTIONS).filter(StringUtils.isNotBlank(_))
    if (javaOptions.isDefined) {
      javaOpts += javaOptions.get
    }

    val am = Seq(applicationMaster)

    val engineClass = Seq("--class", engineMainClass())

    val kyuubiConfProperties = Seq(
      "--properties-file",
      buildPath(Environment.PWD.$$(), LOCALIZED_CONF_DIR, KYUUBI_CONF_FILE))

    val commands =
      Seq(Environment.JAVA_HOME.$$() + "/bin/java", "-server") ++
        javaOpts ++ am ++ engineClass ++ kyuubiConfProperties ++
        Seq(
          "1>",
          ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout",
          "2>",
          ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")

    val printableCommands = commands.map(s => if (s == null) "null" else s).toList
    amContainer.setCommands(printableCommands.asJava)
    info(s"Commands: ${printableCommands.mkString(" ")}")

    setupSecurityToken(amContainer)
    amContainer
  }

  private def prepareLocalResources(
      destDir: Path,
      env: mutable.HashMap[String, String]): mutable.HashMap[String, LocalResource] = {
    info("Preparing resources for engine AM container")
    // Upload kyuubi engine and the extra JAR to the remote file system if necessary,
    // and add them as local resources to the application master.
    val fs = destDir.getFileSystem(hadoopConf)

    val localResources = mutable.HashMap[String, LocalResource]()
    FileSystem.mkdirs(fs, destDir, new FsPermission(STAGING_DIR_PERMISSION))

    distributeJars(localResources, env)
    distributeConf(localResources, env)

    // If we passed in a keytab, make sure we copy the keytab to the staging directory on
    // HDFS, and setup the relevant environment vars, so the AM can login again.
    amKeytabFileName.foreach { kt =>
      info("To enable the AM to login from keytab, credentials are being copied over to the AM" +
        " via the YARN Secure Distributed Cache.")
      distribute(
        new Path(new File(keytab).toURI),
        LocalResourceType.FILE,
        kt,
        localResources)
    }

    localResources
  }

  private def distributeJars(
      localResources: mutable.HashMap[String, LocalResource],
      env: mutable.HashMap[String, String]): Unit = {
    val jarsArchive = File.createTempFile(LOCALIZED_LIB_DIR, ".zip", Utils.createTempDir().toFile)
    val jarsStream = new ZipOutputStream(new FileOutputStream(jarsArchive))
    try {
      jarsStream.setLevel(0)
      val jars = kyuubiConf.getOption(KYUUBI_ENGINE_DEPLOY_YARN_MODE_JARS_KEY)
      val putedEntry = new ListBuffer[String]
      jars.get.split(KYUUBI_ENGINE_DEPLOY_YARN_MODE_ARCHIVE_SEPARATOR).foreach { path =>
        val jars = Utils.listFilesRecursively(new File(path)) ++ engineExtraJars()
        jars.foreach { f =>
          if (!putedEntry.contains(f.getName) && f.isFile &&
            f.getName.toLowerCase(Locale.ROOT).endsWith(".jar") && f.canRead) {
            jarsStream.putNextEntry(new ZipEntry(f.getName))
            Files.copy(f.toPath, jarsStream)
            jarsStream.closeEntry()
            putedEntry += f.getName
            addClasspathEntry(buildPath(Environment.PWD.$$(), LOCALIZED_LIB_DIR, f.getName), env)
          }
        }
      }
      putedEntry.clear()
    } finally {
      jarsStream.close()
    }

    distribute(
      new Path(jarsArchive.getAbsolutePath),
      resType = LocalResourceType.ARCHIVE,
      destName = LOCALIZED_LIB_DIR,
      localResources)
  }

  private def distributeConf(
      localResources: mutable.HashMap[String, LocalResource],
      env: mutable.HashMap[String, String]): Unit = {
    val confArchive = File.createTempFile(LOCALIZED_CONF_DIR, ".zip", Utils.createTempDir().toFile)
    val confStream = new ZipOutputStream(new FileOutputStream(confArchive))
    try {
      confStream.setLevel(0)
      val putedEntry = new ListBuffer[String]
      def putEntry(f: File): Unit = {
        if (!putedEntry.contains(f.getName) && f.isFile && f.canRead) {
          confStream.putNextEntry(new ZipEntry(s"$HADOOP_CONF_DIR/${f.getName}"))
          Files.copy(f.toPath, confStream)
          confStream.closeEntry()
          putedEntry += f.getName
          addClasspathEntry(
            buildPath(Environment.PWD.$$(), LOCALIZED_CONF_DIR, HADOOP_CONF_DIR, f.getName),
            env)
        }
      }
      listConfFiles().foreach(putEntry)
      val properties = confToProperties(kyuubiConf)
      amKeytabFileName.foreach(kt => properties.put(ENGINE_KEYTAB.key, kt))
      writePropertiesToArchive(properties, KYUUBI_CONF_FILE, confStream)
    } finally {
      confStream.close()
    }

    distribute(
      new Path(confArchive.getAbsolutePath),
      resType = LocalResourceType.ARCHIVE,
      destName = LOCALIZED_CONF_DIR,
      localResources)
  }

  def listDistinctFiles(archive: String): Seq[File] = {
    val distinctFiles = new mutable.LinkedHashSet[File]
    archive.split(KYUUBI_ENGINE_DEPLOY_YARN_MODE_ARCHIVE_SEPARATOR).foreach { path =>
      val file = new File(path)
      val files = Utils.listFilesRecursively(file)
      files.foreach { f =>
        if (f.isFile && f.canRead) {
          distinctFiles += f
        }
      }
    }
    distinctFiles.groupBy(_.getName).map {
      case (_, items) => items.head
    }.toSeq
  }

  def listConfFiles(): Seq[File] = {
    // respect the following priority loading configuration, and distinct files
    // hadoop configuration -> yarn configuration
    val hadoopConf = kyuubiConf.getOption(KYUUBI_ENGINE_DEPLOY_YARN_MODE_HADOOP_CONF_KEY)
    val yarnConf = kyuubiConf.getOption(KYUUBI_ENGINE_DEPLOY_YARN_MODE_YARN_CONF_KEY)
    listDistinctFiles(hadoopConf.get) ++ listDistinctFiles(yarnConf.get)
  }

  private def distribute(
      srcPath: Path,
      resType: LocalResourceType,
      destName: String,
      localResources: mutable.HashMap[String, LocalResource]): Unit = {
    val fs = stagingDirPath.getFileSystem(hadoopConf)
    val destPath = new Path(stagingDirPath, srcPath.getName)
    info(s"Copying $srcPath to $destPath")
    fs.copyFromLocalFile(srcPath, destPath)
    fs.setPermission(destPath, new FsPermission(STAGING_DIR_PERMISSION))

    val destFs = FileSystem.get(destPath.toUri, hadoopConf)
    val destStatus = destFs.getFileStatus(destPath)

    val destResource = Records.newRecord(classOf[LocalResource])
    destResource.setType(resType)
    destResource.setVisibility(LocalResourceVisibility.APPLICATION)
    destResource.setResource(URL.fromPath(destPath))
    destResource.setTimestamp(destStatus.getModificationTime)
    destResource.setSize(destStatus.getLen)
    localResources(destName) = destResource
  }

  private[kyuubi] def setupLaunchEnv(kyuubiConf: KyuubiConf): mutable.HashMap[String, String] = {
    info("Setting up the launch environment for engine AM container")
    val env = new mutable.HashMap[String, String]()

    kyuubiConf.getAll
      .filter { case (k, _) => k.startsWith(KyuubiConf.KYUUBI_ENGINE_YARN_MODE_ENV_PREFIX) }
      .map { case (k, v) =>
        (k.substring(KyuubiConf.KYUUBI_ENGINE_YARN_MODE_ENV_PREFIX.length + 1), v)
      }
      .foreach { case (k, v) => KyuubiHadoopUtils.addPathToEnvironment(env, k, v) }

    addClasspathEntry(buildPath(Environment.PWD.$$(), LOCALIZED_CONF_DIR), env)
    env.put(
      Environment.HADOOP_CONF_DIR.name(),
      buildPath(Environment.PWD.$$(), LOCALIZED_CONF_DIR, HADOOP_CONF_DIR))
    addClasspathEntry(buildPath(Environment.PWD.$$(), LOCALIZED_CONF_DIR, HADOOP_CONF_DIR), env)
    env.put("KYUUBI_ENGINE_YARN_MODE_STAGING_DIR", stagingDirPath.toString)
    env
  }

  private def createApplicationSubmissionContext(
      newApp: YarnClientApplication,
      containerContext: ContainerLaunchContext): ApplicationSubmissionContext = {

    val appContext = newApp.getApplicationSubmissionContext
    appContext.setApplicationName(kyuubiConf.get(ENGINE_DEPLOY_YARN_MODE_APP_NAME)
      .getOrElse(s"Apache Kyuubi $engineType Engine"))
    appContext.setQueue(kyuubiConf.get(ENGINE_DEPLOY_YARN_MODE_QUEUE))
    appContext.setAMContainerSpec(containerContext)
    kyuubiConf.get(ENGINE_DEPLOY_YARN_MODE_PRIORITY).foreach { appPriority =>
      appContext.setPriority(Priority.newInstance(appPriority))
    }
    appContext.setApplicationType(engineType.toUpperCase(Locale.ROOT))

    val allTags = new util.HashSet[String]
    kyuubiConf.get(ENGINE_DEPLOY_YARN_MODE_TAGS).foreach { tags =>
      allTags.addAll(tags.asJava)
    }
    appContext.setApplicationTags(allTags)
    appContext.setMaxAppAttempts(1)

    val capability = Records.newRecord(classOf[Resource])
    capability.setMemorySize(kyuubiConf.get(ENGINE_DEPLOY_YARN_MODE_MEMORY))
    capability.setVirtualCores(kyuubiConf.get(ENGINE_DEPLOY_YARN_MODE_CORES))
    debug(s"Created resource capability for AM request: $capability")
    appContext.setResource(capability)

    appContext
  }

  private def monitorApplication(appId: ApplicationId): Unit = {
    val report = yarnClient.getApplicationReport(appId)
    val state = report.getYarnApplicationState
    info(s"Application report for $appId (state: $state)")
    if (state == YarnApplicationState.FAILED || state == YarnApplicationState.KILLED) {
      throw new KyuubiException(s"Application $appId finished with status: $state")
    }
  }

  private def cleanupStagingDir(): Unit = {
    try {
      val fs = stagingDirPath.getFileSystem(hadoopConf)
      if (fs.delete(stagingDirPath, true)) {
        info(s"Deleted staging directory $stagingDirPath")
      }
    } catch {
      case ioe: IOException =>
        warn("Failed to cleanup staging dir " + stagingDirPath, ioe)
    }
  }

  /**
   * Joins all the path components using Path.SEPARATOR.
   */
  private def buildPath(components: String*): String = {
    components.mkString(Path.SEPARATOR)
  }

  /**
   * Add the given path to the classpath entry of the given environment map.
   * If the classpath is already set, this appends the new path to the existing classpath.
   */
  private def addClasspathEntry(path: String, env: mutable.HashMap[String, String]): Unit =
    KyuubiHadoopUtils.addPathToEnvironment(env, Environment.CLASSPATH.name, path)

  private def confToProperties(conf: KyuubiConf): Properties = {
    val props = new Properties()
    conf.getAll.foreach { case (k, v) =>
      props.setProperty(k, v)
    }
    props
  }

  def writePropertiesToArchive(props: Properties, name: String, out: ZipOutputStream): Unit = {
    out.putNextEntry(new ZipEntry(name))
    val writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)
    props.store(writer, "Kyuubi configuration.")
    writer.flush()
    out.closeEntry()
  }

  def writeConfigurationToArchive(
      conf: Configuration,
      name: String,
      out: ZipOutputStream): Unit = {
    out.putNextEntry(new ZipEntry(name))
    val writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)
    conf.writeXml(writer)
    writer.flush()
    out.closeEntry()
  }

  def parseClasspath(classpath: String, jars: ListBuffer[File]): Unit = {
    classpath.split(":").filter(_.nonEmpty).foreach { cp =>
      if (cp.endsWith("/*")) {
        val dir = cp.substring(0, cp.length - 2)
        new File(dir) match {
          case f if f.isDirectory =>
            f.listFiles().filter(_.getName.endsWith(".jar")).foreach(jars += _)
          case _ =>
        }
      } else {
        jars += new File(cp)
      }
    }
  }
}

object EngineYarnModeSubmitter {
  final val KYUUBI_ENGINE_DEPLOY_YARN_MODE_JARS_KEY = "kyuubi.engine.deploy.yarn.mode.jars"
  final val KYUUBI_ENGINE_DEPLOY_YARN_MODE_HIVE_CONF_KEY =
    "kyuubi.engine.deploy.yarn.mode.hiveConf"
  final val KYUUBI_ENGINE_DEPLOY_YARN_MODE_HADOOP_CONF_KEY =
    "kyuubi.engine.deploy.yarn.mode.hadoopConf"
  final val KYUUBI_ENGINE_DEPLOY_YARN_MODE_YARN_CONF_KEY =
    "kyuubi.engine.deploy.yarn.mode.yarnConf"

  final val KYUUBI_ENGINE_DEPLOY_YARN_MODE_ARCHIVE_SEPARATOR = ","
}

case class YarnAppReport(
    appState: YarnApplicationState,
    finalState: FinalApplicationStatus,
    diagnostics: Option[String])
