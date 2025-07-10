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

package org.apache.kyuubi.engine.spark

import java.io.{File, FileFilter, IOException}
import java.nio.file.Paths
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Locale

import scala.collection.mutable

import com.google.common.annotations.VisibleForTesting
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.{ApplicationManagerInfo, KyuubiApplicationManager, ProcBuilder}
import org.apache.kyuubi.engine.KubernetesApplicationOperation.{KUBERNETES_SERVICE_HOST, KUBERNETES_SERVICE_PORT}
import org.apache.kyuubi.engine.ProcBuilder.KYUUBI_ENGINE_LOG_PATH_KEY
import org.apache.kyuubi.ha.HighAvailabilityConf
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_ENGINE_AUTH_TYPE
import org.apache.kyuubi.ha.client.AuthTypes
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.server.KyuubiServer
import org.apache.kyuubi.util.{JavaUtils, KubernetesUtils, Validator}
import org.apache.kyuubi.util.command.CommandLineUtils._

class SparkProcessBuilder(
    override val proxyUser: String,
    override val doAsEnabled: Boolean,
    override val conf: KyuubiConf,
    val engineRefId: String,
    val extraEngineLog: Option[OperationLog] = None)
  extends ProcBuilder with Logging {

  @VisibleForTesting
  def this(proxyUser: String, doAsEnabled: Boolean, conf: KyuubiConf) {
    this(proxyUser, doAsEnabled, conf, "")
  }

  import SparkProcessBuilder._

  private[kyuubi] val sparkHome = getEngineHome(shortName)

  override protected val executable: String = {
    Paths.get(sparkHome, "bin", SPARK_SUBMIT_FILE).toFile.getCanonicalPath
  }

  override def mainClass: String = "org.apache.kyuubi.engine.spark.SparkSQLEngine"

  /**
   * Add `spark.master` if KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT
   * are defined. So we can deploy spark on kubernetes without setting `spark.master`
   * explicitly when kyuubi-servers are on kubernetes, which also helps in case that
   * api-server is not exposed to us.
   */
  override protected def completeMasterUrl(conf: KyuubiConf): Unit = {
    try {
      (
        clusterManager(),
        sys.env.get(KUBERNETES_SERVICE_HOST),
        sys.env.get(KUBERNETES_SERVICE_PORT)) match {
        case (None, Some(kubernetesServiceHost), Some(kubernetesServicePort)) =>
          // According to "https://kubernetes.io/docs/concepts/architecture/control-plane-
          // node-communication/#node-to-control-plane", the API server is configured to listen
          // for remote connections on a secure HTTPS port (typically 443), so we set https here.
          val masterURL = s"k8s://https://${kubernetesServiceHost}:${kubernetesServicePort}"
          conf.set(MASTER_KEY, masterURL)
        case _ =>
      }
    } catch {
      case e: Exception =>
        warn("Failed when setting up spark.master with kubernetes environment automatically.", e)
    }
  }

  /**
   * Converts kyuubi config key so that Spark could identify.
   * - If the key is start with `spark.`, keep it AS IS as it is a Spark Conf
   * - If the key is start with `hadoop.`, it will be prefixed with `spark.hadoop.`
   * - Otherwise, the key will be added a `spark.` prefix
   */
  protected def convertConfigKey(key: String): String = {
    if (key.startsWith("spark.")) {
      key
    } else if (key.startsWith("hadoop.")) {
      "spark.hadoop." + key
    } else {
      "spark." + key
    }
  }

  private[kyuubi] def extractSparkCoreScalaVersion(fileNames: Iterable[String]): String = {
    fileNames.collectFirst { case SPARK_CORE_SCALA_VERSION_REGEX(scalaVersion) => scalaVersion }
      .getOrElse(throw new KyuubiException("Failed to extract Scala version from spark-core jar"))
  }

  override protected val engineScalaBinaryVersion: String = {
    env.get("SPARK_SCALA_VERSION").filter(StringUtils.isNotBlank).getOrElse {
      extractSparkCoreScalaVersion(Paths.get(sparkHome, "jars").toFile.list())
    }
  }

  override protected lazy val engineHomeDirFilter: FileFilter = file => {
    val patterns = SCALA_COMPILE_VERSION match {
      case "2.12" => Seq(SPARK3_HOME_REGEX_SCALA_212)
      case "2.13" => Seq(SPARK3_HOME_REGEX_SCALA_213, SPARK4_HOME_REGEX_SCALA_213)
    }
    file.isDirectory && patterns.exists(_.findFirstMatchIn(file.getName).isDefined)
  }

  override protected[kyuubi] lazy val commands: Iterable[String] = {
    // complete `spark.master` if absent on kubernetes
    completeMasterUrl(conf)

    KyuubiApplicationManager.tagApplication(engineRefId, shortName, clusterManager(), conf)
    val buffer = new mutable.ListBuffer[String]()
    buffer += executable
    buffer += CLASS
    buffer += mainClass

    var allConf = conf.getAll

    // if enable sasl kerberos authentication for zookeeper, need to upload the server keytab file
    if (AuthTypes.withName(conf.get(HA_ZK_ENGINE_AUTH_TYPE)) == AuthTypes.KERBEROS) {
      allConf = allConf ++ zkAuthKeytabFileConf(allConf)
    }
    // pass spark engine log path to spark conf
    (allConf ++
      engineLogPathConf ++
      extraYarnConf(allConf) ++
      appendPodNameConf(allConf) ++
      prepareK8sFileUploadPath() ++
      engineWaitCompletionConf).foreach {
      case (k, v) => buffer ++= confKeyValue(convertConfigKey(k), v)
    }

    setupKerberos(buffer)

    mainResource.foreach { r => buffer += r }

    buffer
  }

  override protected def module: String = "kyuubi-spark-sql-engine"

  protected def setupKerberos(buffer: mutable.Buffer[String]): Unit = {
    // if the keytab is specified, PROXY_USER is not supported
    tryKeytab() match {
      case None if doAsEnabled =>
        setSparkUserName(proxyUser, buffer)
        buffer += PROXY_USER
        buffer += proxyUser
      case None => // doAs disabled
        setSparkUserName(Utils.currentUser, buffer)
      case Some(name) =>
        setSparkUserName(name, buffer)
    }
  }

  private def tryKeytab(): Option[String] = {
    val principal = conf.getOption(PRINCIPAL)
    val keytab = conf.getOption(KEYTAB)
    if (principal.isEmpty || keytab.isEmpty) {
      None
    } else {
      try {
        val ugi = UserGroupInformation
          .loginUserFromKeytabAndReturnUGI(principal.get, keytab.get)
        if (doAsEnabled && ugi.getShortUserName != proxyUser) {
          warn(s"The session proxy user: $proxyUser is not same with " +
            s"spark principal: ${ugi.getShortUserName}, skip using keytab. " +
            "Fallback to use proxy user.")
          None
        } else if (!doAsEnabled && ugi.getShortUserName != Utils.currentUser) {
          warn(s"The server's user: ${Utils.currentUser} is not same with " +
            s"spark principal: ${ugi.getShortUserName}, skip using keytab. " +
            "Fallback to use server's user.")
          None
        } else {
          Some(ugi.getShortUserName)
        }
      } catch {
        case e: IOException =>
          error(s"Failed to login for ${principal.get}", e)
          None
      }
    }
  }

  private def zkAuthKeytabFileConf(sparkConf: Map[String, String]): Map[String, String] = {
    val zkAuthKeytab = conf.get(HighAvailabilityConf.HA_ZK_AUTH_KEYTAB)
    if (zkAuthKeytab.isDefined) {
      sparkConf.get(SPARK_FILES) match {
        case Some(files) =>
          Map(SPARK_FILES -> s"$files,${zkAuthKeytab.get}")
        case _ =>
          Map(SPARK_FILES -> zkAuthKeytab.get)
      }
    } else {
      Map()
    }
  }

  override def shortName: String = "spark"

  protected lazy val defaultsConf: Map[String, String] = {
    val confDir = env.getOrElse(SPARK_CONF_DIR, s"$sparkHome${File.separator}conf")
    try {
      val confFile = new File(s"$confDir${File.separator}$SPARK_CONF_FILE_NAME")
      if (confFile.exists()) {
        Utils.getPropertiesFromFile(Some(confFile))
      } else {
        Map.empty[String, String]
      }
    } catch {
      case _: Exception =>
        warn(s"Failed to load spark configurations from $confDir")
        Map.empty[String, String]
    }
  }

  override def appMgrInfo(): ApplicationManagerInfo = {
    ApplicationManagerInfo(
      clusterManager(),
      kubernetesContext(),
      kubernetesNamespace())
  }

  private val forciblyRewriteDriverPodName: Boolean =
    conf.get(KUBERNETES_FORCIBLY_REWRITE_DRIVER_POD_NAME)
  private val forciblyRewriteExecPodNamePrefix: Boolean =
    conf.get(KUBERNETES_FORCIBLY_REWRITE_EXEC_POD_NAME_PREFIX)

  def appendPodNameConf(conf: Map[String, String]): Map[String, String] = {
    val appName = conf.getOrElse(APP_KEY, "spark")
    val map = mutable.Map.newBuilder[String, String]
    if (clusterManager().exists(cm => cm.toLowerCase(Locale.ROOT).startsWith("k8s"))) {
      if (!conf.contains(KUBERNETES_EXECUTOR_POD_NAME_PREFIX)) {
        val prefix = KubernetesUtils.generateExecutorPodNamePrefix(
          appName,
          engineRefId,
          forciblyRewriteExecPodNamePrefix)
        map += (KUBERNETES_EXECUTOR_POD_NAME_PREFIX -> prefix)
      }
      if (deployMode().exists(_.toLowerCase(Locale.ROOT) == "cluster")) {
        if (!conf.contains(KUBERNETES_DRIVER_POD_NAME)) {
          val name = KubernetesUtils.generateDriverPodName(
            appName,
            engineRefId,
            forciblyRewriteDriverPodName)
          map += (KUBERNETES_DRIVER_POD_NAME -> name)
        }
      }
    }
    map.result().toMap
  }

  def prepareK8sFileUploadPath(): Map[String, String] = {
    kubernetesFileUploadPath() match {
      case Some(uploadPathPattern) if isK8sClusterMode =>
        val today = LocalDate.now()
        val uploadPath = uploadPathPattern
          .replace("{{YEAR}}", today.format(YEAR_FMT))
          .replace("{{MONTH}}", today.format(MONTH_FMT))
          .replace("{{DAY}}", today.format(DAY_FMT))

        if (conf.get(KUBERNETES_SPARK_AUTO_CREATE_FILE_UPLOAD_PATH)) {
          // Create the `uploadPath` using permission 777, otherwise, spark just creates the
          // `$uploadPath/spark-upload-$uuid` using default permission 511, which might prevent
          // other users from creating the staging dir under `uploadPath` later.
          val hadoopConf = KyuubiServer.getHadoopConf()
          val path = new Path(uploadPath)
          var fs: FileSystem = null
          try {
            fs = path.getFileSystem(hadoopConf)
            if (!fs.exists(path)) {
              info(s"Try creating $KUBERNETES_FILE_UPLOAD_PATH: $uploadPath")
              fs.mkdirs(path, KUBERNETES_UPLOAD_PATH_PERMISSION)
            }
          } catch {
            case ioe: IOException =>
              warn(s"Failed to create $KUBERNETES_FILE_UPLOAD_PATH: $uploadPath", ioe)
          } finally {
            if (fs != null) {
              Utils.tryLogNonFatalError(fs.close())
            }
          }
        }
        Map(KUBERNETES_FILE_UPLOAD_PATH -> uploadPath)
      case _ =>
        Map.empty
    }
  }

  def extraYarnConf(conf: Map[String, String]): Map[String, String] = {
    val map = mutable.Map.newBuilder[String, String]
    if (clusterManager().exists(_.toLowerCase(Locale.ROOT).startsWith("yarn"))) {
      if (!conf.contains(YARN_MAX_APP_ATTEMPTS_KEY)) {
        // Set `spark.yarn.maxAppAttempts` to 1 to avoid invalid attempts.
        // As mentioned in YARN-5617, it is improved after hadoop `2.8.2/2.9.0/3.0.0`.
        map += (YARN_MAX_APP_ATTEMPTS_KEY -> "1")
      }
    }
    map.result().toMap
  }

  override def clusterManager(): Option[String] = {
    getSparkOption(MASTER_KEY)
  }

  def deployMode(): Option[String] = {
    getSparkOption(DEPLOY_MODE_KEY)
  }

  override def isClusterMode(): Boolean = {
    clusterManager().map(_.toLowerCase(Locale.ROOT)) match {
      case Some(m) if m.startsWith("yarn") || m.startsWith("k8s") =>
        deployMode().exists(_.toLowerCase(Locale.ROOT) == "cluster")
      case _ => false
    }
  }

  def isK8sClusterMode: Boolean = {
    clusterManager().exists(cm => cm.toLowerCase(Locale.ROOT).startsWith("k8s")) &&
    deployMode().exists(_.toLowerCase(Locale.ROOT) == "cluster")
  }

  def kubernetesContext(): Option[String] = {
    getSparkOption(KUBERNETES_CONTEXT_KEY)
  }

  def kubernetesNamespace(): Option[String] = {
    getSparkOption(KUBERNETES_NAMESPACE_KEY)
  }

  def kubernetesFileUploadPath(): Option[String] = {
    getSparkOption(KUBERNETES_FILE_UPLOAD_PATH)
  }

  override def validateConf(): Unit = Validator.validateConf(conf)

  // For spark on kubernetes, spark pod using env SPARK_USER_NAME as current user
  def setSparkUserName(userName: String, buffer: mutable.Buffer[String]): Unit = {
    clusterManager().foreach { cm =>
      if (cm.toUpperCase.startsWith("K8S")) {
        buffer ++= confKeyValue("spark.kubernetes.driverEnv.SPARK_USER_NAME", userName)
        buffer ++= confKeyValue("spark.executorEnv.SPARK_USER_NAME", userName)
      }
    }
  }

  private[spark] def engineLogPathConf(): Map[String, String] = {
    Map(KYUUBI_ENGINE_LOG_PATH_KEY -> engineLog.getAbsolutePath)
  }

  private[spark] def getSparkOption(key: String): Option[String] = {
    conf.getOption(KUBERNETES_NAMESPACE_KEY).orElse(defaultsConf.get(KUBERNETES_NAMESPACE_KEY))
  }

  override def waitEngineCompletion: Boolean = {
    !isClusterMode() || getSparkOption(KyuubiConf.SESSION_ENGINE_STARTUP_WAIT_COMPLETION.key)
      .getOrElse(KyuubiConf.SESSION_ENGINE_STARTUP_WAIT_COMPLETION.defaultValStr)
      .toBoolean
  }

  def engineWaitCompletionConf(): Map[String, String] =
    clusterManager().map(_.toLowerCase(Locale.ROOT)) match {
      case Some(m) if m.startsWith("yarn") =>
        Map(YARN_SUBMIT_WAIT_APP_COMPLETION -> waitEngineCompletion.toString)
      case Some(m) if m.startsWith("k8s") =>
        Map(KUBERNETES_SUBMISSION_WAIT_APP_COMPLETION -> waitEngineCompletion.toString)
      case _ => Map.empty
    }
}

object SparkProcessBuilder {
  final val APP_KEY = "spark.app.name"
  final val TAG_KEY = "spark.yarn.tags"
  final val MASTER_KEY = "spark.master"
  final val DEPLOY_MODE_KEY = "spark.submit.deployMode"
  final val KUBERNETES_CONTEXT_KEY = "spark.kubernetes.context"
  final val KUBERNETES_NAMESPACE_KEY = "spark.kubernetes.namespace"
  final val KUBERNETES_DRIVER_POD_NAME = "spark.kubernetes.driver.pod.name"
  final val KUBERNETES_EXECUTOR_POD_NAME_PREFIX = "spark.kubernetes.executor.podNamePrefix"
  final val KUBERNETES_SUBMISSION_WAIT_APP_COMPLETION =
    "spark.kubernetes.submission.waitAppCompletion"
  final val YARN_MAX_APP_ATTEMPTS_KEY = "spark.yarn.maxAppAttempts"
  final val YARN_SUBMIT_WAIT_APP_COMPLETION = "spark.yarn.submit.waitAppCompletion"
  final val INTERNAL_RESOURCE = "spark-internal"

  final val KUBERNETES_FILE_UPLOAD_PATH = "spark.kubernetes.file.upload.path"
  final val KUBERNETES_UPLOAD_PATH_PERMISSION = new FsPermission(Integer.parseInt("777", 8).toShort)

  final val YEAR_FMT = DateTimeFormatter.ofPattern("yyyy")
  final val MONTH_FMT = DateTimeFormatter.ofPattern("MM")
  final val DAY_FMT = DateTimeFormatter.ofPattern("dd")

  /**
   * The path configs from Spark project that might upload local files:
   * - SparkSubmit
   * - org.apache.spark.deploy.yarn.Client::prepareLocalResources
   * - KerberosConfDriverFeatureStep::configurePod
   * - KubernetesUtils.uploadAndTransformFileUris
   */
  final val PATH_CONFIGS = Seq(
    SPARK_FILES,
    "spark.jars",
    "spark.archives",
    "spark.yarn.jars",
    "spark.yarn.dist.files",
    "spark.yarn.dist.pyFiles",
    "spark.submit.pyFiles",
    "spark.yarn.dist.jars",
    "spark.yarn.dist.archives",
    "spark.kerberos.keytab",
    "spark.yarn.keytab",
    "spark.kubernetes.kerberos.krb5.path",
    "spark.kubernetes.file.upload.path")

  final private[spark] val CLASS = "--class"
  final private[spark] val PROXY_USER = "--proxy-user"
  final private[spark] val SPARK_FILES = "spark.files"
  final private[spark] val PRINCIPAL = "spark.kerberos.principal"
  final private[spark] val KEYTAB = "spark.kerberos.keytab"
  // Get the appropriate spark-submit file
  final private val SPARK_SUBMIT_FILE =
    if (JavaUtils.isWindows) "spark-submit.cmd" else "spark-submit"
  final private val SPARK_CONF_DIR = "SPARK_CONF_DIR"
  final private val SPARK_CONF_FILE_NAME = "spark-defaults.conf"

  final private[kyuubi] val SPARK_CORE_SCALA_VERSION_REGEX =
    """^spark-core_(\d\.\d+)-.*\.jar$""".r

  final private[kyuubi] val SPARK3_HOME_REGEX_SCALA_212 =
    """^spark-3\.\d+\.\d+-bin-hadoop\d+(\.\d+)?$""".r

  final private[kyuubi] val SPARK3_HOME_REGEX_SCALA_213 =
    """^spark-3\.\d+\.\d+-bin-hadoop\d(\.\d+)?+-scala2\.13$""".r

  final private[kyuubi] val SPARK4_HOME_REGEX_SCALA_213 =
    """^spark-4\.\d+\.\d+(-\w*)?-bin-hadoop\d(\.\d+)?+$""".r
}
