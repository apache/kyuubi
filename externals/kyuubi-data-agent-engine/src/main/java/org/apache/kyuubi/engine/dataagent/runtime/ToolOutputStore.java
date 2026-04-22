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

package org.apache.kyuubi.engine.dataagent.runtime;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-session temp-file store for large tool outputs that the input gate has offloaded from
 * conversation history.
 *
 * <p><b>Layout:</b> {@code <root>/<sessionId>/tool_<toolCallId>.txt}, where {@code <root>} is a
 * per-engine randomly-named temp directory (see {@link #create()}). UTF-8 text.
 *
 * <p><b>Isolation:</b> every {@code read}/{@code grep} call requires the current session id and
 * validates that the caller-supplied path resolves under {@code <root>/<sessionId>}, not merely
 * under {@code <root>}. A session cannot read another session's offloaded output even if it somehow
 * obtained the absolute path. The per-engine random root additionally isolates different engine
 * processes sharing the same host.
 *
 * <p>Failures (traversal, missing file, session-id mismatch, IO) are reported as error strings
 * rather than thrown — the tool must keep the agent loop alive.
 */
public class ToolOutputStore implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(ToolOutputStore.class);
  private static final String ROOT_PREFIX = "kyuubi-data-agent-";

  private final Path root;

  /**
   * Create a store backed by a fresh, per-engine random temp directory under {@code
   * java.io.tmpdir}.
   */
  public static ToolOutputStore create() {
    try {
      return new ToolOutputStore(Files.createTempDirectory(ROOT_PREFIX));
    } catch (IOException e) {
      throw new IllegalStateException("Failed to create ToolOutputStore temp root", e);
    }
  }

  private ToolOutputStore(Path root) {
    try {
      Files.createDirectories(root);
      this.root = root.toRealPath();
    } catch (IOException e) {
      throw new IllegalStateException("Failed to initialize ToolOutputStore root: " + root, e);
    }
  }

  public Path getRoot() {
    return root;
  }

  /** Write {@code content} to {@code <root>/<sessionId>/tool_<toolCallId>.txt}. */
  public Path write(String sessionId, String toolCallId, String content) throws IOException {
    Path dir = root.resolve(safeSegment(sessionId));
    Files.createDirectories(dir);
    Path file = dir.resolve("tool_" + safeSegment(toolCallId) + ".txt");
    Files.write(file, content.getBytes(StandardCharsets.UTF_8));
    return file;
  }

  /**
   * Read a line window. Returns a human-readable block including a 1-based {@code [lines X-Y of Z
   * total]} header, or an error string on traversal / IO failure / cross-session access.
   */
  public String read(String sessionId, String pathStr, long offset, int limit) {
    Path file = validatePath(sessionId, pathStr);
    if (file == null) {
      return "Error: path is outside this session's tool-output directory or does not exist: "
          + pathStr;
    }
    if (offset < 0) offset = 0;
    if (limit <= 0) limit = 1;

    List<String> taken = new ArrayList<>(limit);
    long totalLines = 0;
    try (BufferedReader br = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
      String line;
      while ((line = br.readLine()) != null) {
        if (totalLines >= offset && taken.size() < limit) {
          taken.add(line);
        }
        totalLines++;
      }
    } catch (IOException e) {
      return "Error reading " + pathStr + ": " + e.getMessage();
    }

    long fromLine = offset + 1; // 1-based
    long toLineExclusive = Math.min(offset + limit, totalLines);
    StringBuilder sb = new StringBuilder();
    sb.append("[lines ")
        .append(fromLine)
        .append("-")
        .append(toLineExclusive)
        .append(" of ")
        .append(totalLines)
        .append(" total]\n");
    for (String line : taken) {
      sb.append(line).append('\n');
    }
    return sb.toString();
  }

  /**
   * Stream-grep the file. Returns at most {@code maxMatches} matches as {@code lineNo:content}, one
   * per line; or an error string on traversal / regex / IO failure / cross-session access.
   */
  public String grep(String sessionId, String pathStr, String patternStr, int maxMatches) {
    Path file = validatePath(sessionId, pathStr);
    if (file == null) {
      return "Error: path is outside this session's tool-output directory or does not exist: "
          + pathStr;
    }
    if (patternStr == null || patternStr.isEmpty()) {
      return "Error: 'pattern' parameter is required.";
    }
    if (maxMatches <= 0) maxMatches = 50;

    Pattern pattern;
    try {
      pattern = Pattern.compile(patternStr);
    } catch (PatternSyntaxException e) {
      return "Error: invalid regex pattern: " + e.getMessage();
    }

    StringBuilder sb = new StringBuilder();
    int matches = 0;
    long lineNo = 0;
    try (BufferedReader br = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
      String line;
      while ((line = br.readLine()) != null) {
        lineNo++;
        if (pattern.matcher(line).find()) {
          sb.append(lineNo).append(':').append(line).append('\n');
          matches++;
          if (matches >= maxMatches) break;
        }
      }
    } catch (IOException e) {
      return "Error reading " + pathStr + ": " + e.getMessage();
    }
    if (matches == 0) {
      return "[no matches for pattern: " + patternStr + "]";
    }
    return "[" + matches + " match" + (matches == 1 ? "" : "es") + "]\n" + sb;
  }

  /** Recursively delete the session's subtree. Safe to call on missing sessions. */
  public void cleanupSession(String sessionId) {
    if (sessionId == null) return;
    Path dir = root.resolve(safeSegment(sessionId));
    deleteTree(dir);
  }

  /** Delete everything below (and including) the root. Idempotent; safe to call multiple times. */
  @Override
  public void close() {
    deleteTree(root);
  }

  private static void deleteTree(Path dir) {
    if (!Files.exists(dir)) return;
    try (Stream<Path> stream = Files.walk(dir)) {
      stream.sorted(Comparator.reverseOrder()).forEach(ToolOutputStore::deleteQuietly);
    } catch (IOException e) {
      LOG.warn("Failed to clean up dir {}", dir, e);
    }
  }

  private static void deleteQuietly(Path p) {
    try {
      Files.deleteIfExists(p);
    } catch (IOException e) {
      LOG.debug("Failed to delete {}", p, e);
    }
  }

  /**
   * Resolve {@code pathStr} and return it only if (a) it exists as a regular file and (b) the real
   * path is under {@code <root>/<sessionId>}. Returns null on any violation — including a null or
   * empty session id, since without one we cannot scope the check.
   */
  private Path validatePath(String sessionId, String pathStr) {
    if (pathStr == null || pathStr.isEmpty()) return null;
    if (sessionId == null || sessionId.isEmpty()) return null;
    Path sessionRoot = root.resolve(safeSegment(sessionId));
    try {
      Path real = Paths.get(pathStr).toRealPath();
      if (!real.startsWith(sessionRoot)) return null;
      if (!Files.isRegularFile(real)) return null;
      return real;
    } catch (IOException | SecurityException e) {
      return null;
    }
  }

  /** Strip anything that could escape a single path segment. */
  private static String safeSegment(String raw) {
    if (raw == null || raw.isEmpty()) return "_";
    StringBuilder sb = new StringBuilder(raw.length());
    for (int i = 0; i < raw.length(); i++) {
      char c = raw.charAt(i);
      if (Character.isLetterOrDigit(c) || c == '-' || c == '_' || c == '.') {
        sb.append(c);
      } else {
        sb.append('_');
      }
    }
    return sb.toString();
  }
}
