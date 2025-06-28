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

# Sharing and Isolation for Kyuubi Engines

## Overview

To solve problems like long startup times for compute engines and to
efficiently manage cluster resources in a multi-tenant environment, Kyuubi
provides a flexible engine sharing and isolation mechanism that allows
different levels of engine sharing among users and sessions. This feature
enables efficient resource utilization while maintaining proper isolation
boundaries. The sharing mechanism is controlled through configuration
parameters that determine how engines are shared and isolated across
different contexts.

## Engine Share Levels

Kyuubi supports five different engine share levels, each providing different
degrees of isolation and resource sharing:

### CONNECTION Level

- **Isolation**: Each client connection gets its own dedicated engine
- **Use Case**: Maximum isolation, suitable for sensitive workloads or when
  complete resource isolation is required
- **Configuration**: `kyuubi.engine.share.level=CONNECTION`

### USER Level

- **Isolation**: All sessions from the same username share the same engine
- **Use Case**: Balance between resource efficiency and user isolation
  (recommended for most scenarios)
- **Configuration**: `kyuubi.engine.share.level=USER`

### GROUP Level

- **Isolation**: All users belonging to the same primary group share the
  same engine
- **Use Case**: Team-based resource sharing
- **Configuration**: `kyuubi.engine.share.level=GROUP`
- **Note**: Falls back to USER level if primary group is not found

### SERVER_LOCAL Level

- **Isolation**: Engines are shared within the same server instance
- **Use Case**: Local server resource optimization
- **Configuration**: `kyuubi.engine.share.level=SERVER_LOCAL`

### SERVER Level

- **Isolation**: Engines are shared across multiple servers
- **Use Case**: Maximum resource sharing and efficiency
- **Configuration**: `kyuubi.engine.share.level=SERVER`

## Subdomain Isolation

Kyuubi supports subdomain-based isolation within share levels, allowing
fine-grained control over engine sharing:

### Configuration

```properties
# Enable subdomain isolation
kyuubi.engine.share.level.subdomain=<subdomain_name>
```

### Features

- **Flexible Sharing**: Users can create multiple engines within the same
  share level using different subdomains
- **Default Behavior**: When disable engine pool, use 'default' if absent.

### How to Use Subdomains in Practice

Users can specify the subdomain in their connection properties. For example,
two different JDBC connections from the same user 'bob' can create two
separate engines:

**Connection 1 (for Analytics):**

```
jdbc:kyuubi://host:port/db;kyuubi.engine.share.level.subdomain=analytics
```

*This will create or reuse an engine in the 'analytics' subdomain for user
'bob'.*

**Connection 2 (for ETL):**

```
jdbc:kyuubi://host:port/db;kyuubi.engine.share.level.subdomain=etl
```

*This will create or reuse a different engine in the 'etl' subdomain for
user 'bob'.*

### Configuration File Example

```properties
# Create engines with different subdomains for the same user
kyuubi.engine.share.level=USER
kyuubi.engine.share.level.subdomain=analytics
```

## Configuration Examples

### Basic User-Level Sharing

```properties
kyuubi.engine.share.level=USER
kyuubi.engine.type=SPARK_SQL
```

### Team-Based Sharing with Custom Subdomain

```properties
kyuubi.engine.share.level=GROUP
kyuubi.engine.share.level.subdomain=analytics_team
```

### Maximum Isolation Setup

```properties
kyuubi.engine.share.level=CONNECTION
# Each connection gets its own engine
```
