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

# Kyuubi Spark SQL Engine Plugin

The `Kyuubi Spark SQL Engine Plugin` is a plugin that includes SPI definitions for extending some features of the Spark SQL Engine.

## Plugins

| Plugin                                                     | Description                                            |
|:-----------------------------------------------------------|:-------------------------------------------------------|
| `org.apache.kyuubi.engine.spark.plugin.SQLStringifyPlugin` | The plugin to extend custom stringified sql statement. |

## How to implement a custom plugin

Usually to implement a custom plugin we need to do the following steps:

+ Add `kyuubi-spark-sql-engine-plugin` dependency
+ Implement plugin interface
+ Create a service file
+ Package and integration with Kyuubi Spark SQL Engine

### Example of implementing a custom SQLStringifyPlugin

Add `kyuubi-spark-sql-engine-plugin` dependency:

```xml
<dependency>
    <groupId>org.apache.kyuubi</groupId>
    <artifactId>kyuubi-spark-sql-engine-plugin</artifactId>
    <version>${kyuubi.version}</version>
    <scope>provided</scope>
</dependency>
```

Extend `SQLStringifyPlugin` interface and implement methods:

```java
package org.apache.kyuubi.custom;

import java.util.Locale;
import org.apache.spark.sql.SparkSession;
import org.apache.kyuubi.engine.spark.plugin.SQLStringifyPlugin;


public class CustomSQLStringifyPlugin implements SQLStringifyPlugin {
    @Override
    public String mode() {
        return "custom_to_upper";
    }

    @Override
    public String toString(SparkSession spark, String statement) {
        return statement.toUpperCase(Locale.ROOT);
    }
}
```

Create a `org.apache.kyuubi.engine.spark.plugin.SQLStringifyPlugin` file in `src/main/resources/META-INF/services/` with content:

```
org.apache.kyuubi.custom.CustomSQLStringifyPlugin
```

Package the custom plugin jar, and set `spark.jars=/path/to/custom-plugin-jar`.

Enjoy your custom plugin with Kyuubi Spark SQL Engine:

```sql
set kyuubi.operation.plan.only.mode=custom_to_upper;
select * from table;
```

output:

```text
SELECT * FROM TABLE;
```
