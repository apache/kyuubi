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

# Spark Shutdown Watchdog Plugin - Testing Guide

## Overview

This document provides comprehensive testing instructions for the Spark Shutdown Watchdog Plugin. The plugin monitors Spark driver shutdown and forcefully terminates the JVM if graceful shutdown stalls, preventing resource leaks in production environments.

## Prerequisites

- **Java**: JDK 8 or higher (Java 11+ recommended)
- **Maven**: 3.6+ 
- **Spark**: 3.0+ (tested with Spark 4.0.1)
- **SPARK_HOME**: Environment variable pointing to Spark installation

## Building the Plugin

```bash
cd extensions/spark/kyuubi-spark-shutdown-watchdog
mvn clean package
```

The JAR file will be generated at:
```
target/kyuubi-spark-shutdown-watchdog-1.11.0-SNAPSHOT.jar
```

## Unit Tests

Run the unit test suite:

```bash
mvn test
```

### Test Cases

1. **watchdog does not start when disabled**
   - Verifies that watchdog doesn't start when `spark.kyuubi.shutdown.watchdog.enabled=false`
   - Expected: No watchdog thread created

2. **watchdog does not start when timeout is non-positive**
   - Verifies that watchdog doesn't start when timeout <= 0
   - Expected: No watchdog thread created

3. **watchdog triggers emergency exit after timeout**
   - Verifies that watchdog forces exit after timeout period
   - Expected: Exit code matches `EXIT_CODE_FORCED_TERMINATION` after timeout

**Expected Output**:
```
Run completed in X seconds
Total number of tests run: 3
Tests: succeeded 3, failed 0
All tests passed.
```

## Manual Integration Testing

### Test Scenario 1: Normal Shutdown (Watchdog Should Not Trigger)

**Purpose**: Verify that watchdog doesn't interfere with normal shutdown

**Steps**:

1. Create a simple Spark application that completes normally:

```java
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;

public class TestNormalShutdown {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
            .setAppName("TestNormalShutdown")
            .setMaster("local[2]")
            .set("spark.kyuubi.shutdown.watchdog.enabled", "true")
            .set("spark.kyuubi.shutdown.watchdog.timeout", "5s")
            .set("spark.plugins", "org.apache.spark.kyuubi.shutdown.watchdog.SparkShutdownWatchdogPlugin");
        
        JavaSparkContext jsc = new JavaSparkContext(conf);
        
        // Simple job
        long count = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5)).count();
        System.out.println("Count: " + count);
        
        jsc.stop();
        System.out.println("Application completed successfully");
    }
}
```

2. Compile and run:

```bash
# Compile
javac -cp "$SPARK_HOME/jars/*:target/kyuubi-spark-shutdown-watchdog-1.11.0-SNAPSHOT.jar" TestNormalShutdown.java

# Run
$SPARK_HOME/bin/spark-submit \
  --class TestNormalShutdown \
  --conf "spark.kyuubi.shutdown.watchdog.enabled=true" \
  --conf "spark.kyuubi.shutdown.watchdog.timeout=5s" \
  --conf "spark.plugins=org.apache.spark.kyuubi.shutdown.watchdog.SparkShutdownWatchdogPlugin" \
  --jars target/kyuubi-spark-shutdown-watchdog-1.11.0-SNAPSHOT.jar \
  . \
  TestNormalShutdown
```

3. **Expected Results**:
   - Application exits with code 0
   - Logs show: `Shutdown Watchdog activated. Driver will be forcefully terminated if graceful shutdown exceeds 5000 ms.`
   - Logs do NOT show: `EMERGENCY SHUTDOWN TRIGGERED`
   - Application completes in reasonable time (< 30 seconds)

4. **Verification**:
```bash
# Check logs for watchdog activation (should be present)
grep "Shutdown Watchdog activated" spark-logs/*

# Verify no emergency shutdown (should return nothing)
grep "EMERGENCY SHUTDOWN TRIGGERED" spark-logs/*
```

### Test Scenario 2: Hanging Shutdown (Watchdog Should Trigger)

**Purpose**: Verify that watchdog forces termination when shutdown stalls

**Steps**:

1. Create a Spark application with a non-daemon thread that prevents shutdown:

```java
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

public class TestHangingShutdown {
    private static final CountDownLatch latch = new CountDownLatch(1);
    
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
            .setAppName("TestHangingShutdown")
            .setMaster("local[2]")
            .set("spark.kyuubi.shutdown.watchdog.enabled", "true")
            .set("spark.kyuubi.shutdown.watchdog.timeout", "5s")
            .set("spark.plugins", "org.apache.spark.kyuubi.shutdown.watchdog.SparkShutdownWatchdogPlugin");
        
        JavaSparkContext jsc = new JavaSparkContext(conf);
        
        // Simple job
        long count = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5)).count();
        System.out.println("Count: " + count);
        
        // Create non-daemon thread to prevent shutdown
        Thread hangingThread = new Thread(() -> {
            try {
                Thread.sleep(60000); // Sleep for 60 seconds
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "hanging-thread");
        hangingThread.setDaemon(false); // Non-daemon thread prevents JVM exit
        hangingThread.start();
        
        jsc.stop();
        System.out.println("After jsc.stop(), waiting...");
        
        // Wait for watchdog to kill us
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
```

2. Compile and run:

```bash
# Compile
javac -cp "$SPARK_HOME/jars/*:target/kyuubi-spark-shutdown-watchdog-1.11.0-SNAPSHOT.jar" TestHangingShutdown.java

# Run (capture output to log file)
$SPARK_HOME/bin/spark-submit \
  --class TestHangingShutdown \
  --conf "spark.kyuubi.shutdown.watchdog.enabled=true" \
  --conf "spark.kyuubi.shutdown.watchdog.timeout=5s" \
  --conf "spark.plugins=org.apache.spark.kyuubi.shutdown.watchdog.SparkShutdownWatchdogPlugin" \
  --jars target/kyuubi-spark-shutdown-watchdog-1.11.0-SNAPSHOT.jar \
  . \
  TestHangingShutdown 2>&1 | tee test-hanging.log
```

3. **Expected Results**:
   - Application starts and completes Spark job successfully
   - After `jsc.stop()`, process continues running (non-daemon thread prevents shutdown)
   - After ~5 seconds (watchdog timeout), watchdog triggers
   - Logs show emergency shutdown messages
   - Thread dump is generated and logged
   - Process terminates (may exit with code 0 due to spark-submit wrapper, but watchdog messages confirm termination)

4. **Verification**:
```bash
# Check for watchdog emergency messages (should be present)
grep "EMERGENCY SHUTDOWN TRIGGERED" test-hanging.log
grep "THREAD DUMP FOR DIAGNOSTIC" test-hanging.log
grep "Non-daemon threads are preventing JVM exit" test-hanging.log

# Verify thread dump contains hanging thread
grep "hanging-thread" test-hanging.log

# Check timing (should be ~5-15 seconds total, including Spark startup)
# The watchdog timeout is 5s, so after jsc.stop(), it should trigger within 5-7 seconds
```

**Expected Log Output**:
```
25/12/01 20:46:00 INFO SparkShutdownWatchdogPlugin: Shutdown Watchdog activated. Driver will be forcefully terminated if graceful shutdown exceeds 5000 ms.
...
25/12/01 20:46:05 ERROR SparkShutdownWatchdogPlugin: EMERGENCY SHUTDOWN TRIGGERED
25/12/01 20:46:05 ERROR SparkShutdownWatchdogPlugin: Graceful shutdown exceeded 5000 ms timeout
25/12/01 20:46:05 ERROR SparkShutdownWatchdogPlugin: Non-daemon threads are preventing JVM exit
25/12/01 20:46:05 ERROR SparkShutdownWatchdogPlugin: Initiating forced termination...
25/12/01 20:46:05 ERROR SparkShutdownWatchdogPlugin: === THREAD DUMP FOR DIAGNOSTIC ===
================== Thread Dump Start ==================
...
Thread: "hanging-thread" #61 
   State: TIMED_WAITING
...
================== Thread Dump End ==================
25/12/01 20:46:05 ERROR SparkShutdownWatchdogPlugin: === END OF THREAD DUMP ===
25/12/01 20:46:05 ERROR SparkShutdownWatchdogPlugin: Forcefully terminating JVM now...
```

### Test Scenario 3: Watchdog Disabled

**Purpose**: Verify that watchdog doesn't activate when disabled

**Steps**:

1. Use the same hanging application from Test Scenario 2, but disable watchdog:

```bash
$SPARK_HOME/bin/spark-submit \
  --class TestHangingShutdown \
  --conf "spark.kyuubi.shutdown.watchdog.enabled=false" \
  --conf "spark.kyuubi.shutdown.watchdog.timeout=5s" \
  --conf "spark.plugins=org.apache.spark.kyuubi.shutdown.watchdog.SparkShutdownWatchdogPlugin" \
  --jars target/kyuubi-spark-shutdown-watchdog-1.11.0-SNAPSHOT.jar \
  . \
  TestHangingShutdown 2>&1 | tee test-disabled.log &

# Wait 10 seconds, then kill the process
sleep 10
kill %1 2>/dev/null || pkill -f TestHangingShutdown
```

2. **Expected Results**:
   - Application hangs (non-daemon thread prevents shutdown)
   - No watchdog emergency messages in logs
   - Process continues running until manually terminated

3. **Verification**:
```bash
# Should NOT contain watchdog emergency messages
grep "EMERGENCY SHUTDOWN TRIGGERED" test-disabled.log
# Should return no matches (exit code 1)
```

### Test Scenario 4: Thread Dump Utility Verification

**Purpose**: Verify that `ThreadDumpUtils` generates correct thread dumps

**Steps**:

1. Create a simple test class:

```java
import org.apache.spark.kyuubi.shutdown.watchdog.ThreadDumpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestThreadDump {
    private static final Logger logger = LoggerFactory.getLogger(TestThreadDump.class);
    
    public static void main(String[] args) {
        System.out.println("=== Testing dumpToConsole ===");
        ThreadDumpUtils.dumpToConsole();
        
        System.out.println("\n=== Testing dumpToString ===");
        String dump = ThreadDumpUtils.dumpToString();
        System.out.println("Thread dump length: " + dump.length() + " characters");
        System.out.println("First 500 characters:");
        System.out.println(dump.substring(0, Math.min(500, dump.length())));
        
        System.out.println("\n=== Testing dumpToLogger ===");
        ThreadDumpUtils.dumpToLogger(logger);
        
        System.out.println("\nAll tests completed!");
    }
}
```

2. Compile and run:

```bash
# Compile (need slf4j-api and slf4j-simple for logger)
javac -cp "$SPARK_HOME/jars/*:target/kyuubi-spark-shutdown-watchdog-1.11.0-SNAPSHOT.jar" TestThreadDump.java

# Run
java -cp "$SPARK_HOME/jars/*:target/kyuubi-spark-shutdown-watchdog-1.11.0-SNAPSHOT.jar:." TestThreadDump
```

3. **Expected Results**:
   - Thread dump printed to console
   - Thread statistics shown (daemon vs non-daemon, by state)
   - Stack traces for all threads
   - Summary table at the end
   - No errors or exceptions

## Configuration Parameters

### `spark.kyuubi.shutdown.watchdog.enabled`

- **Type**: Boolean
- **Default**: `true`
- **Description**: Enable/disable the shutdown watchdog
- **Example**: `--conf "spark.kyuubi.shutdown.watchdog.enabled=true"`

### `spark.kyuubi.shutdown.watchdog.timeout`

- **Type**: Time duration (e.g., "5s", "30s", "1m")
- **Default**: Must be set explicitly (0ms disables watchdog)
- **Description**: Maximum time to wait for graceful shutdown before forcing termination
- **Example**: `--conf "spark.kyuubi.shutdown.watchdog.timeout=5s"`

### `spark.plugins`

- **Type**: Comma-separated list of plugin class names
- **Required**: Must include `org.apache.spark.kyuubi.shutdown.watchdog.SparkShutdownWatchdogPlugin`
- **Description**: Registers the plugin with Spark
- **Example**: `--conf "spark.plugins=org.apache.spark.kyuubi.shutdown.watchdog.SparkShutdownWatchdogPlugin"`

## Test Results Interpretation

### Success Criteria

1. **Normal Shutdown Test**:
   - ✓ Exit code: 0
   - ✓ Duration: < 30 seconds (includes Spark startup)
   - ✓ Watchdog activation message present
   - ✓ No emergency shutdown messages

2. **Hanging Shutdown Test**:
   - ✓ Watchdog triggers after timeout period (~5 seconds after `jsc.stop()`)
   - ✓ Emergency shutdown messages present in logs
   - ✓ Thread dump generated and logged
   - ✓ Hanging thread identified in thread dump
   - ✓ Process terminates

3. **Disabled Test**:
   - ✓ No watchdog emergency messages
   - ✓ Process hangs (expected behavior)

4. **Thread Dump Test**:
   - ✓ Thread dump generated successfully
   - ✓ Contains all threads with stack traces
   - ✓ Includes thread statistics and summary

### Failure Indicators

- Watchdog doesn't trigger when it should (check configuration and logs)
- Watchdog triggers when it shouldn't (check for unexpected non-daemon threads)
- Missing thread dump in logs (check for exceptions)
- Plugin not loading (check Spark logs for plugin errors)
- Incorrect timeout behavior (verify timeout configuration)

## Troubleshooting

### Plugin Not Loading

**Symptoms**: No watchdog messages in logs

**Solutions**:
1. Verify `spark.plugins` configuration includes the plugin class name
2. Check that JAR is in classpath (`--jars` parameter)
3. Check Spark driver logs for plugin loading errors
4. Verify JAR file exists and is accessible

**Verification**:
```bash
# Should see plugin initialization message
grep "DriverPluginContainer.*Initialized driver component" spark-logs/*
```

### Watchdog Not Triggering

**Symptoms**: Application hangs indefinitely even with watchdog enabled

**Solutions**:
1. Verify `spark.kyuubi.shutdown.watchdog.enabled=true`
2. Verify timeout is set and > 0
3. Check that plugin is loaded (look for "Shutdown Watchdog activated" message)
4. Verify non-daemon threads exist (check thread dump if available)
5. Check Spark version compatibility

**Verification**:
```bash
# Should see activation message
grep "Shutdown Watchdog activated" spark-logs/*

# Check timeout configuration
grep "shutdown.watchdog.timeout" spark-logs/*
```

### Thread Dump Not Generated

**Symptoms**: Watchdog triggers but no thread dump in logs

**Solutions**:
1. Check logger configuration (thread dump uses ERROR level)
2. Verify `ThreadDumpUtils` is in classpath
3. Check for exceptions in logs
4. Verify Java version compatibility (Java 8+)

### Java Version Compatibility

**Symptoms**: Compilation errors or runtime errors

**Solutions**:
- Spark 4.0+ requires Java 11+
- Use `Thread.getId()` instead of `Thread.threadId()` for Java 8 compatibility
- Verify Java version: `java -version`
- Set JAVA_HOME if needed: `export JAVA_HOME=$(/usr/libexec/java_home -v 17)`

## Performance Considerations

- **Normal Operation**: Zero overhead (watchdog only monitors shutdown phase)
- **Emergency Shutdown**: Thread dump generation takes < 100ms
- **Memory**: Minimal (single daemon thread)
- **CPU**: Negligible (watchdog thread sleeps until timeout)

## Security Considerations

- Watchdog only monitors shutdown, doesn't interfere with running jobs
- Thread dump contains sensitive information (stack traces, class names) - ensure logs are secured
- Exit codes align with Spark's standard exit codes
- No network or file system access required

## Example: Complete Test Workflow

Here's a complete example workflow for testing:

```bash
# 1. Build the plugin
cd extensions/spark/kyuubi-spark-shutdown-watchdog
mvn clean package

# 2. Set environment variables
export SPARK_HOME=/path/to/spark
export JAVA_HOME=$(/usr/libexec/java_home -v 17)  # For macOS

# 3. Run unit tests
mvn test

# 4. Create test application (see Test Scenario 2 above)
# Save as TestHangingShutdown.java

# 5. Compile test application
javac -cp "$SPARK_HOME/jars/*:target/kyuubi-spark-shutdown-watchdog-1.11.0-SNAPSHOT.jar" TestHangingShutdown.java

# 6. Run hanging shutdown test
$SPARK_HOME/bin/spark-submit \
  --class TestHangingShutdown \
  --conf "spark.kyuubi.shutdown.watchdog.enabled=true" \
  --conf "spark.kyuubi.shutdown.watchdog.timeout=5s" \
  --conf "spark.plugins=org.apache.spark.kyuubi.shutdown.watchdog.SparkShutdownWatchdogPlugin" \
  --jars target/kyuubi-spark-shutdown-watchdog-1.11.0-SNAPSHOT.jar \
  . \
  TestHangingShutdown 2>&1 | tee test-results.log

# 7. Verify results
grep "EMERGENCY SHUTDOWN TRIGGERED" test-results.log
grep "THREAD DUMP FOR DIAGNOSTIC" test-results.log
grep "hanging-thread" test-results.log
```

## Test Matrix

| Spark Version | Scala Version | Java Version | Status |
|--------------|---------------|--------------|--------|
| 3.0+         | 2.12          | 8+           | ✅ Compatible |
| 3.5+         | 2.12          | 11+          | ✅ Tested |
| 4.0+         | 2.13          | 11+          | ✅ Tested |
