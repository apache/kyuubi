package org.apache.kyuubi.client;

import org.junit.Test;

public class KyuubiRestClientTest {

  @Test(expected = IllegalArgumentException.class)
  public void invalidBuilderTest() {
    KyuubiRestClient basicClient =
        new KyuubiRestClient.Builder("https://localhost:8443", "batch")
            .authSchema(KyuubiRestClient.AuthSchema.BASIC)
            .username("test")
            .build();
  }
}
