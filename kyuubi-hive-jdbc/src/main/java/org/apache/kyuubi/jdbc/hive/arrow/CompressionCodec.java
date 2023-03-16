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

package org.apache.kyuubi.jdbc.hive.arrow;

import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;
import net.jpountz.lz4.LZ4FrameInputStream;

public interface CompressionCodec {
  InputStream decompress(byte[] batchBytes) throws IOException;
}

abstract class AbstractCompressionCodec implements CompressionCodec {

  abstract InputStream inputStream(ByteArrayInputStream in) throws IOException;

  @Override
  public InputStream decompress(byte[] batchBytes) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(batchBytes);
    InputStream inputStream = inputStream(in);
    int length = batchBytes.length * 2;
    byte[] buffer = new byte[length];
    int offset = 0;

    int readLength;
    do {
      readLength = inputStream.read(buffer, offset, buffer.length - offset);
      if (-1 == readLength) {
        break;
      }

      offset += readLength;
      if (length <= offset) {
        length *= 2;
        buffer = Arrays.copyOf(buffer, length);
      }
    } while (-1 != readLength);
    inputStream.close();
    in = new ByteArrayInputStream(buffer);
    return in;
  }
}

class NoCompressionCodec implements CompressionCodec {
  @Override
  public InputStream decompress(byte[] batchBytes) throws IOException {
    return new ByteArrayInputStream(batchBytes);
  }
}

class Lz4CompressionCodec extends AbstractCompressionCodec {

  @Override
  InputStream inputStream(ByteArrayInputStream in) throws IOException {
    return new LZ4FrameInputStream(in);
  }
}

class ZstdCompressionCodec extends AbstractCompressionCodec {
  @Override
  InputStream inputStream(ByteArrayInputStream in) throws IOException {
    return new ZstdInputStreamNoFinalizer(in);
  }
}

class GZIPCompressionCodec extends AbstractCompressionCodec {
  @Override
  InputStream inputStream(ByteArrayInputStream in) throws IOException {
    return new GZIPInputStream(in);
  }
}
