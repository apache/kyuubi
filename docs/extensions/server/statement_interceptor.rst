.. Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

Intercept Statements with Custom Statement Interceptor
======================================================

.. versionadded:: 1.12.0

.. caution:: unstable

Kyuubi supports intercepting interactive statements on the server before they are routed to the engine, through custom statement interceptors. As a unified gateway, Kyuubi can host this governance once at the server layer instead of having every engine reimplement it, so administrators can inspect, reject, or rewrite each statement without forking Kyuubi or writing the same logic for Spark, Flink, Trino, and JDBC separately. Typical use cases include rule-based SQL guards, authorization, risky-statement interception, auditing, and SQL rewriting.

The interceptor is a complement to, not a replacement for, the existing extension points such as custom authentication, ``SessionConfAdvisor``, event handlers, or Spark AuthZ.

The Plugin Interface
--------------------

The public SPI lives in the ``kyuubi-server-plugin`` module under the ``org.apache.kyuubi.plugin`` package, alongside the existing ``SessionConfAdvisor`` and ``GroupProvider``. The module is dependency-free, so the interfaces only use JDK types and do not reference internal Kyuubi types, which keeps plugins decoupled from the server and the API stable.

A single instance of each interceptor is created per Kyuubi server via its zero-arg constructor, and ``beforeExecuteStatement`` is invoked concurrently by many sessions. Implementations MUST be thread-safe and MUST NOT keep per-request mutable state in instance fields.

.. code-block:: java

   public interface StatementInterceptor {

     // Called once when the server starts; conf is an immutable read-only snapshot
     // of the full server configuration. Implementations read their own private keys.
     default void initialize(Map<String, String> conf) {}

     // Invoked for each statement before operation creation and engine routing.
     StatementInterceptResult beforeExecuteStatement(StatementInterceptContext context);

     // Called once when the server stops, to release resources.
     default void close() {}
   }

The context exposes a stable, gateway-level view of the statement using JDK types only. It intentionally does not expose the session configuration (which carries connection parameters and engine credentials) nor any mutable internal session object.

.. code-block:: java

   public interface StatementInterceptContext {
     String sessionId();
     String statementId();            // unique id, equal to the operation handle the client receives
     String user();                   // effective user the statement runs as (proxy user if impersonating)
     String realUser();               // authenticated user before impersonation; equals user() if none
     String ipAddress();              // empty string when unknown, never null
     String statement();              // the current statement (after prior rewrites)
     Map<String, String> confOverlay();   // statement-level overlay, read-only
     boolean runAsync();
     long queryTimeout();             // client-requested timeout in seconds, 0 means none
     String engineType();             // spark / flink / trino / ...
   }

An interceptor returns one of three decisions:

.. code-block:: java

   public final class StatementInterceptResult {
     public enum Action { PROCEED, REWRITE, REJECT }

     public static StatementInterceptResult proceed();          // keep the current statement
     public static StatementInterceptResult rewrite(String s);  // replace it for the next interceptor and the engine
     public static StatementInterceptResult reject(String msg); // stop the chain and return an error to the client
   }

Enable Statement Interceptors
-----------------------------

1. Create one or more classes implementing ``org.apache.kyuubi.plugin.StatementInterceptor``.
2. Compile and put the jar into ``$KYUUBI_HOME/jars``.
3. Add the configuration in ``kyuubi-defaults.conf``:

   .. code-block:: properties

      kyuubi.operation.statement.interceptors=com.example.SqlGuard,com.example.LlmSqlRewriter

Interceptors run in the configured order. The execution semantics are:

- the chain starts from the original statement;
- ``PROCEED`` keeps the current statement and passes it to the next interceptor;
- ``REWRITE`` replaces the current statement and passes the new one to the next interceptor and ultimately to the engine;
- ``REJECT`` stops the chain immediately, no operation is created, and the client receives an error carrying SQLState ``42000`` (access rule violation), so clients can tell a policy rejection from a syntax error;
- if an interceptor throws or returns ``null``, the statement fails (fail-closed).

The interceptors are eagerly loaded and initialized at server startup, so a misconfigured or failing interceptor fails the server fast rather than at the first query. They are closed in reverse order when the server stops.

Example
-------

A simple guard that rejects statements containing configured keywords:

.. code-block:: java

   package com.example;

   import java.util.*;
   import java.util.stream.Collectors;
   import org.apache.kyuubi.plugin.*;

   public class SqlGuard implements StatementInterceptor {

     private Set<String> blockedKeywords;

     @Override
     public void initialize(Map<String, String> conf) {
       String raw = conf.getOrDefault("example.sql.guard.blocked.keywords", "drop,truncate");
       blockedKeywords = Arrays.stream(raw.split(","))
           .map(s -> s.trim().toLowerCase(Locale.ROOT))
           .filter(s -> !s.isEmpty())
           .collect(Collectors.toSet());
     }

     @Override
     public StatementInterceptResult beforeExecuteStatement(StatementInterceptContext ctx) {
       String sql = ctx.statement().trim().toLowerCase(Locale.ROOT);
       if (blockedKeywords.stream().anyMatch(sql::contains)) {
         return StatementInterceptResult.reject("SQL is rejected by policy for user " + ctx.user());
       }
       return StatementInterceptResult.proceed();
     }
   }

.. note:: The substring match above is for illustration only. A production SQL guard should use a parser, otherwise a statement like ``SELECT * FROM dropdown_events`` would be wrongly rejected by ``contains("drop")``.

Then deploy the jar and enable it:

.. code-block:: properties

   kyuubi.operation.statement.interceptors=com.example.SqlGuard
   example.sql.guard.blocked.keywords=drop,truncate,delete

Notes
-----

.. note:: The configuration is ``serverOnly``, so it cannot be overridden in a session and users cannot bypass the interceptors.

.. note:: Interceptors only apply to the interactive path (``executeStatement``), covering both engine-routed statements and server-side commands. Batch jobs are submitted as applications and never pass through this hook, and metadata operations such as ``getTables`` are not intercepted.

.. caution:: ``beforeExecuteStatement`` runs synchronously on the statement-submission thread and adds directly to the submission latency. Interceptors that make external calls (for example to an authorization service or an LLM) must enforce their own timeout and retry limits, and choose their own degradation policy: governance interceptors should fail closed (``REJECT`` or throw), while enrichment interceptors may fail open (``PROCEED``).
