package org.apache.hive.beeline;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class KyuubiBeeLineTest {
  @Test
  public void testKyuubiBeelineWithoutArgs() {
    KyuubiBeeLine kyuubiBeeLine = new KyuubiBeeLine();
    int result = kyuubiBeeLine.initArgs(new String[0]);
    assertEquals(0, result);
  }
}
