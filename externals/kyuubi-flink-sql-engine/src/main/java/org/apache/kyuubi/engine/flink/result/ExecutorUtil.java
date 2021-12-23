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

package org.apache.kyuubi.engine.flink.result;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.context.SessionContext;
import org.apache.flink.table.client.gateway.local.LocalExecutor;

public class ExecutorUtil {

  public static SessionContext getSessionContext(Executor executor, String sessionId)
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    if (executor instanceof LocalExecutor) {
      LocalExecutor localExecutor = (LocalExecutor) executor;

      Class clazz = localExecutor.getClass();
      Class[] parameterTypes = new Class[] {String.class};
      Method getSessionContextMethod = clazz.getDeclaredMethod("getSessionContext", parameterTypes);
      getSessionContextMethod.setAccessible(true);

      Object sessionContext = getSessionContextMethod.invoke(localExecutor, sessionId);
      if (sessionContext instanceof SessionContext) {
        return (SessionContext) sessionContext;
      }
    }

    return null;
  }
}
