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

import static java.util.Arrays.asList;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Collectors;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

public class ArrowColumnarBatchRowTest {

  Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
  Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
  Schema schemaPerson = new Schema(asList(name, age));

  @Test
  public void testInt() throws IOException {

    BufferAllocator allocator =
        ArrowUtils.rootAllocator.newChildAllocator("ReadIntTest", 0, Long.MAX_VALUE);

    byte[] batchBytes = mockData();
    ByteArrayInputStream in = new ByteArrayInputStream(batchBytes);
    boolean isCompressed = true;

    if (isCompressed) {
      LZ4FrameInputStream lz4FrameInputStream = new LZ4FrameInputStream(in);
      int length = batchBytes.length * 2;
      byte[] buffer = new byte[length];
      int offset = 0;

      int readLength;
      do {
        readLength = lz4FrameInputStream.read(buffer, offset, buffer.length - offset);
        if (-1 == readLength) {
          break;
        }

        offset += readLength;
        if (length <= offset) {
          length *= 2;
          buffer = Arrays.copyOf(buffer, length);
        }
      } while (-1 != readLength);
      lz4FrameInputStream.close();
      in = new ByteArrayInputStream(buffer);
    }

    if (isCompressed && false) {

      InputStream decompressed = compressedInputStream(in);

      //      byte[] buffer = new byte[batchBytes.length];
      System.out.println(batchBytes.length);
      byte[] buffer = new byte[batchBytes.length * 2];

      byte[] tmp = new byte[1024];
      int n, pos = 0, i = 0;
      while ((n = decompressed.read(tmp, i, tmp.length - i)) != -1) {
        i += n;
        if (i == tmp.length) {
          System.arraycopy(tmp, 0, buffer, pos, i);
          pos += i;
          i = 0;
        }
      }
      System.arraycopy(tmp, 0, buffer, pos, i);
      pos += i;
      in = new ByteArrayInputStream(buffer);
    }

    ArrowRecordBatch recordBatch =
        MessageSerializer.deserializeRecordBatch(
            new ReadChannel(Channels.newChannel(in)), allocator);

    VectorSchemaRoot root = VectorSchemaRoot.create(schemaPerson, allocator);
    VectorLoader vectorLoader = new VectorLoader(root);
    vectorLoader.load(recordBatch);
    recordBatch.close();

    //    val columns = root.getFieldVectors.asScala.map { vector =>
    //      new ArrowColumnVector(vector).asInstanceOf[ColumnVector]
    //    }.toArray
    java.util.List<ArrowColumnVector> columns =
        root.getFieldVectors().stream()
            .map(vector -> new ArrowColumnVector(vector))
            .collect(Collectors.toList());

    //    ArrowColumnarBatchRow batchRow =
    //        new ArrowColumnarBatchRow((ArrowColumnVector[]) columns.toArray());

    ArrowColumnarBatch batch =
        new ArrowColumnarBatch(columns.toArray(new ArrowColumnVector[0]), root.getRowCount());

    Iterator<ArrowColumnarBatchRow> it = batch.rowIterator();
    while (it.hasNext()) {
      ArrowColumnarBatchRow row = it.next();
      System.out.print(row.getString(0) + "\t");
      System.out.println(row.getInt(1));
    }

    //    System.out.print(columns.get(0).getString(0) + "\t");
    //    System.out.println(columns.get(1).getInt(0));
    //
    //    System.out.print(columns.get(0).getString(1) + "\t");
    //    System.out.println(columns.get(1).getInt(1));
    //
    //    System.out.print(columns.get(0).getString(2) + "\t");
    //    System.out.println(columns.get(1).getInt(2));

  }

  @Test
  public void testForeach() {
    int[] arr = {1, 2, 3};
    java.util.List<Integer> l =
        asList(1, 2, 3).stream().map(i -> i + 1).collect(Collectors.toList());
    System.out.println(l);
  }

  public byte[] mockData() {

    //    BufferAllocator allocator =
    //        ArrowUtils.rootAllocator().newChildAllocator("mockData", 0, Long.MAX_VALUE);
    try (BufferAllocator allocator = new RootAllocator(); ) {
      try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, allocator)) {
        VectorUnloader unloader = new VectorUnloader(vectorSchemaRoot);
        VarCharVector nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
        nameVector.allocateNew(3);
        nameVector.set(0, "David".getBytes());
        nameVector.set(1, "Gladis".getBytes());
        nameVector.set(2, "Juan".getBytes());
        IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
        ageVector.allocateNew(3);
        ageVector.set(0, 10);
        ageVector.set(1, 20);
        ageVector.set(2, 30);
        vectorSchemaRoot.setRowCount(3);
        try {
          ByteArrayOutputStream out = new ByteArrayOutputStream();
          //            ArrowFileWriter writer = new ArrowFileWriter(vectorSchemaRoot, null,
          // Channels.newChannel(out))
          WriteChannel writeChannel = new WriteChannel(Channels.newChannel(out));
          ArrowRecordBatch batch = unloader.getRecordBatch();
          MessageSerializer.serialize(writeChannel, batch);
          batch.close();
          //          writer.start();
          //          writer.writeBatch();
          //          System.out.println("Record batches written: " +
          // writer.getRecordBlocks().size() +
          //              ". Number of rows written: " + vectorSchemaRoot.getRowCount());
          //          LZ4BlockOutputStream lz4FrameOutputStream = new LZ4BlockOutputStream(out);
          //          System.out.println(out.size());
          //          OutputStream os = compressedOutputStream(out);
          //          System.out.println(out.size());
          //          int size = out.size();
          //          byte[] buffer = new byte[size];
          //          os.write(buffer, 0, size);
          //          os.flush();
          //          os.close();
          //          return buffer;
          //          return out.toByteArray();
          if (false) {
            System.out.println(out.size());
            ByteArrayOutputStream o2 = new ByteArrayOutputStream();
            OutputStream os = compressedOutputStream(o2);
            os.write(out.toByteArray());
            os.flush();
            os.close();
            return o2.toByteArray();
          }
          if (true) {
            ByteArrayOutputStream o2 = new ByteArrayOutputStream();
            LZ4FrameOutputStream os = new LZ4FrameOutputStream(o2);
            os.write(out.toByteArray());
            os.flush();
            os.close();
            return o2.toByteArray();
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return null;
  }

  public static int defaultSeed = 0x9747b28c; // LZ4BlockOutputStream.DEFAULT_SEED
  public static int blockSize = 32 * 1024; // 32k
  public static LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
  public static XXHashFactory xxHashFactory = XXHashFactory.fastestInstance();

  public static OutputStream compressedOutputStream(OutputStream s) {
    //    val syncFlush = false
    return new LZ4BlockOutputStream(
        s,
        blockSize,
        lz4Factory.fastCompressor(),
        xxHashFactory.newStreamingHash32(defaultSeed).asChecksum(),
        false);
  }

  public static InputStream compressedInputStream(InputStream s) {
    return new LZ4BlockInputStream(
        s,
        lz4Factory.fastDecompressor(),
        xxHashFactory.newStreamingHash32(defaultSeed).asChecksum(),
        false);
  }
}

// class LZ4CompressionCodec(conf: SparkConf) extends CompressionCodec {
//
//  // SPARK-28102: if the LZ4 JNI libraries fail to initialize then `fastestInstance()` calls fall
//  // back to non-JNI implementations but do not remember the fact that JNI failed to load, so
//  // repeated calls to `fastestInstance()` will cause performance problems because the JNI load
//  // will be repeatedly re-attempted and that path is slow because it throws exceptions from a
//  // static synchronized method (causing lock contention). To avoid this problem, we cache the
//  // result of the `fastestInstance()` calls ourselves (both factories are thread-safe).
//  @transient private[this] lazy val lz4Factory: LZ4Factory = LZ4Factory.fastestInstance()
//      @transient private[this] lazy val xxHashFactory: XXHashFactory =
// XXHashFactory.fastestInstance()
//
//  private[this] val defaultSeed: Int = 0x9747b28c // LZ4BlockOutputStream.DEFAULT_SEED
//  private[this] val blockSize = conf.get(IO_COMPRESSION_LZ4_BLOCKSIZE).toInt
//
//  override def compressedOutputStream(s: OutputStream): OutputStream = {
//    val syncFlush = false
//    new LZ4BlockOutputStream(
//        s,
//        blockSize,
//        lz4Factory.fastCompressor(),
//        xxHashFactory.newStreamingHash32(defaultSeed).asChecksum,
//        syncFlush)
//  }
//
//  override def compressedInputStream(s: InputStream): InputStream = {
//    val disableConcatenationOfByteStream = false
//    new LZ4BlockInputStream(
//        s,
//        lz4Factory.fastDecompressor(),
//        xxHashFactory.newStreamingHash32(defaultSeed).asChecksum,
//        disableConcatenationOfByteStream)
//  }
// }
