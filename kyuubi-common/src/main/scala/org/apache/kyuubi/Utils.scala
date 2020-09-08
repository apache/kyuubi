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

package org.apache.kyuubi

import java.io.{File, InputStreamReader, IOException}
import java.net.{URI, URISyntaxException}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.{Properties, UUID}

import scala.collection.JavaConverters._

import org.apache.hadoop.security.UserGroupInformation

private[kyuubi] object Utils extends Logging {

  import org.apache.kyuubi.config.KyuubiConf._

  def strToSeq(s: String): Seq[String] = {
    require(s != null)
    s.split(",").map(_.trim).filter(_.nonEmpty)
  }

  def getSystemProperties: Map[String, String] = {
    sys.props.toMap
  }

  def getDefaultPropertiesFile(env: Map[String, String] = sys.env): Option[File] = {
    env.get(KYUUBI_CONF_DIR)
      .orElse(env.get(KYUUBI_HOME).map(_ + File.separator + "/conf"))
      .map( d => new File(d + File.separator + KYUUBI_CONF_FILE_NAME))
      .filter(f => f.exists() && f.isFile)
      .orElse {
        Option(getClass.getClassLoader.getResource(KYUUBI_CONF_FILE_NAME)).map { url =>
          new File(url.getFile)
        }
      }
  }

  def getPropertiesFromFile(file: Option[File]): Map[String, String] = {
    file.map { f =>
      info(s"Loading Kyuubi properties from ${f.getAbsolutePath}")
      val reader = new InputStreamReader(f.toURI.toURL.openStream(), StandardCharsets.UTF_8)
      try {
        val properties = new Properties()
        properties.load(reader)
        properties.stringPropertyNames().asScala.map { k =>
          (k, properties.getProperty(k).trim)
        }.toMap
      } catch {
        case e: IOException =>
          throw new KyuubiException(
            s"Failed when loading Kyuubi properties from ${f.getAbsolutePath}", e)
      } finally {
        reader.close()
      }
    }.getOrElse(Map.empty)
  }

  /**
   * Return a well-formed URI for the file described by a user input string.
   *
   * If the supplied path does not contain a scheme, or is a relative path, it will be
   * converted into an absolute path with a file:// scheme.
   */
  def resolveURI(path: String): URI = {
    try {
      val uri = new URI(path)
      if (uri.getScheme != null) {
        return uri
      }
      // make sure to handle if the path has a fragment (applies to yarn
      // distributed cache)
      if (uri.getFragment != null) {
        val absoluteURI = new File(uri.getPath).getAbsoluteFile.toURI
        return new URI(absoluteURI.getScheme, absoluteURI.getHost, absoluteURI.getPath,
          uri.getFragment)
      }
    } catch {
      case _: URISyntaxException =>
    }
    new File(path).getAbsoluteFile.toURI
  }

  private val MAX_DIR_CREATION_ATTEMPTS: Int = 10

  /**
   * Create a directory inside the given parent directory. The directory is guaranteed to be
   * newly created, and is not marked for automatic deletion.
   */
  def createDirectory(root: String, namePrefix: String = "kyuubi"): Path = {
    var error: Exception = null
    (0 until MAX_DIR_CREATION_ATTEMPTS).foreach { _ =>
      val candidate = Paths.get(root, s"$namePrefix-${UUID.randomUUID()}")
      try {
        val path = Files.createDirectories(candidate)
        return path
      } catch {
        case e: IOException => error = e
      }
    }
    throw new IOException("Failed to create a temp directory (under " + root + ") after " +
      MAX_DIR_CREATION_ATTEMPTS + " attempts!", error)
  }

  /**
   * Create a temporary directory inside the given parent directory. The directory will be
   * automatically deleted when the VM shuts down.
   */
  def createTempDir(
      root: String = System.getProperty("java.io.tmpdir"),
      namePrefix: String = "kyuubi"): Path = {
    val dir = createDirectory(root, namePrefix)
    dir.toFile.deleteOnExit()
    dir
  }

  def currentUser: String = UserGroupInformation.getCurrentUser.getShortUserName
}
