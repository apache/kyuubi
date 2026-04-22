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

import static org.junit.Assert.*;

import com.openai.models.chat.completions.ChatCompletionMessageParam;
import com.openai.models.chat.completions.ChatCompletionUserMessageParam;
import java.util.Collections;
import org.junit.Test;

public class ConversationMemoryTest {

  @Test
  public void testReplaceHistoryClearsLastTotalTokensButKeepsCumulative() {
    ConversationMemory memory = new ConversationMemory();
    memory.addCumulativeTokens(100, 50, 150);
    memory.addCumulativeTokens(200, 80, 280);
    assertEquals(280L, memory.getLastTotalTokens());
    assertEquals(300L, memory.getCumulativePromptTokens());
    assertEquals(430L, memory.getCumulativeTotalTokens());

    ChatCompletionMessageParam summary =
        ChatCompletionMessageParam.ofUser(
            ChatCompletionUserMessageParam.builder().content("summary").build());
    memory.replaceHistory(Collections.singletonList(summary));

    assertEquals("lastTotalTokens reset after compaction", 0L, memory.getLastTotalTokens());
    assertEquals("cumulative totals preserved", 300L, memory.getCumulativePromptTokens());
    assertEquals("cumulative totals preserved", 430L, memory.getCumulativeTotalTokens());
  }
}
