<!--
- Licensed to the Apache Software Foundation (ASF) under one or more
- contributor license agreements.  See the NOTICE file distributed with
- this work for additional information regarding copyright ownership.
- The ASF licenses this file to You under the Apache License, Version 2.0
- (the "License"); you may not use this file except in compliance with
- the License.  You may obtain a copy of the License at
-
-   http://www.apache.org/licenses/LICENSE-2.0
-
- Unless required by applicable law or agreed to in writing, software
- distributed under the License is distributed on an "AS IS" BASIS,
- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
- See the License for the specific language governing permissions and
- limitations under the License.
-->

# Inject Session Conf With Custom Config Advisor

```{versionadded} 1.5.0
```

## Session Conf Advisor

Kyuubi supports inject session configs with custom config advisor.
It is usually used to append or overwrite session configs dynamically, so administrators of Kyuubi can have an ability to control the user specified configs.

## The Steps Of Injecting Session Configs

1. Create a custom class which implements the `org.apache.kyuubi.plugin.SessionConfAdvisor`.
2. Compile and put the jar into `$KYUUBI_HOME/jars`
3. Adding configuration at `kyuubi-defaults.conf`:

   ```properties
   kyuubi.session.conf.advisor=${classname}
   ```

The `org.apache.kyuubi.plugin.SessionConfAdvisor` has a zero-arg constructor, holds one method with user and session conf and returns a new conf map.

```java
public interface SessionConfAdvisor {
  default Map<String, String> getConfOverlay(String user, Map<String, String> sessionConf) {
    return Collections.EMPTY_MAP;
  }
}
```

```{note}
The returned conf map will overwrite the original session conf.
```

## Example

We have a custom class `CustomSessionConfAdvisor`:

```java
public class CustomSessionConfAdvisor implements SessionConfAdvisor {
  @Override
  Map<String, String> getConfOverlay(String user, Map<String, String> sessionConf) {
    if ("uly".equals(user)) {
      return Collections.singletonMap("spark.driver.memory", "1G");
    } else {
      return Collections.EMPTY_MAP;
    }
  }
}
```

If a user `uly` creates a connection with:

```text
jdbc:kyuubi://localhost:10009/;hive.server2.proxy.user=uly;#spark.driver.memory=2G
```

The final Spark application will allocate `1G` rather than `2G` for the driver jvm.

