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

package org.apache.kyuubi.engine.dataagent.runtime.middleware;

/**
 * Outcome of an {@link AgentMiddleware} interceptor hook. Three arms, uniform across all hooks:
 *
 * <ul>
 *   <li>{@link #proceed()} — pass through with the original value
 *   <li>{@link #replace(Object)} — substitute the value, then continue
 *   <li>{@link #abort(String)} — stop processing this item; per-hook semantics:
 *       <ul>
 *         <li>{@code beforeLlmCall}: skip the LLM call and end the loop
 *         <li>{@code beforeToolCall}: deny the call; reason is fed back to the LLM as the result
 *         <li>{@code afterToolCall}: short-circuit the chain; outer middlewares are not invoked,
 *             reason replaces the result for the LLM, and the emitted {@code ToolResult} is marked
 *             {@code isError=true}
 *         <li>{@code onEvent}: drop the event
 *       </ul>
 * </ul>
 */
public final class Decision<T> {

  public enum Kind {
    PROCEED,
    REPLACE,
    ABORT
  }

  private static final Decision<?> PROCEED = new Decision<>(Kind.PROCEED, null, null);

  private final Kind kind;
  private final T replacement;
  private final String reason;

  private Decision(Kind kind, T replacement, String reason) {
    this.kind = kind;
    this.replacement = replacement;
    this.reason = reason;
  }

  @SuppressWarnings("unchecked")
  public static <T> Decision<T> proceed() {
    return (Decision<T>) PROCEED;
  }

  public static <T> Decision<T> replace(T value) {
    if (value == null) {
      throw new IllegalArgumentException("replace value must not be null");
    }
    return new Decision<>(Kind.REPLACE, value, null);
  }

  public static <T> Decision<T> abort(String reason) {
    if (reason == null) {
      throw new IllegalArgumentException("abort reason must not be null");
    }
    return new Decision<>(Kind.ABORT, null, reason);
  }

  /**
   * Fold helper for middleware dispatchers: PROCEED if {@code current} is still the original
   * reference (no middleware replaced anything), REPLACE otherwise.
   */
  public static <T> Decision<T> of(T original, T current) {
    return current == original ? proceed() : replace(current);
  }

  public Kind kind() {
    return kind;
  }

  /** Non-null only when {@link #kind()} is {@link Kind#REPLACE}. */
  public T replacement() {
    return replacement;
  }

  /** Non-null only when {@link #kind()} is {@link Kind#ABORT}. */
  public String reason() {
    return reason;
  }
}
