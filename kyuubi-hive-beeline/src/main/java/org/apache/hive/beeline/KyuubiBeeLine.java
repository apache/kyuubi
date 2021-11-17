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

package org.apache.hive.beeline;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Driver;

public class KyuubiBeeLine extends BeeLine {
  public static final String KYUUBI_BEELINE_DEFAULT_JDBC_DRIVER = "org.apache.kyuubi.jdbc.KyuubiHiveDriver";
  protected KyuubiCommands commands = new KyuubiCommands(this);
  private Driver defaultDriver = null;

  public KyuubiBeeLine() {
    this(true);
  }

  public KyuubiBeeLine(boolean isBeeLine) {
    super(isBeeLine);
    try {
      Field commandsFiled = BeeLine.class.getDeclaredField("commands");
      commandsFiled.setAccessible(true);
      commandsFiled.set(this, commands);
    } catch (Throwable t) {
      throw new ExceptionInInitializerError("Failed to inject kyuubi commands");
    }
    try {
      defaultDriver = (Driver) Class.forName(KYUUBI_BEELINE_DEFAULT_JDBC_DRIVER, true,
        Thread.currentThread().getContextClassLoader()).newInstance();
    } catch (Throwable t) {
      throw new ExceptionInInitializerError(KYUUBI_BEELINE_DEFAULT_JDBC_DRIVER + "-missing");
    }
  }

  /**
   * Starts the program.
   */
  public static void main(String[] args) throws IOException {
    mainWithInputRedirection(args, null);
  }

  protected Driver getDefaultDriver() {
    return defaultDriver;
  }
}
