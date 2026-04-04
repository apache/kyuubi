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

import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import org.junit.Test;

public class ReactAgentBuilderTest {

  @Test(expected = IllegalStateException.class)
  public void testBuilderRequiresClient() {
    ReactAgent.builder().modelName("gpt-4").build();
  }

  @Test(expected = IllegalStateException.class)
  public void testBuilderRequiresModelName() {
    ReactAgent.builder().client(dummyClient()).build();
  }

  @Test
  public void testBuilderSucceedsWithRequiredFields() {
    ReactAgent agent =
        ReactAgent.builder()
            .client(dummyClient())
            .modelName("gpt-4")
            .systemPrompt("You are a helpful agent.")
            .maxIterations(5)
            .build();
    assertNotNull(agent);
    agent.close();
  }

  @Test
  public void testBuilderDefaultMaxIterations() {
    // Default maxIterations should be 20 — verify build succeeds
    ReactAgent agent = ReactAgent.builder().client(dummyClient()).modelName("gpt-4").build();
    assertNotNull(agent);
    agent.close();
  }

  private static OpenAIClient dummyClient() {
    return OpenAIOkHttpClient.builder().apiKey("test-key").build();
  }
}
