package org.apache.kyuubi.jdbc.hive.arrow;

import static java.util.Arrays.asList;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.Iterator;
import java.util.stream.Collectors;
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
import org.apache.kyuubi.jdbc.util.ArrowUtils;
import org.junit.Test;

public class ArrowColumnarBatchRowTest {

  Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
  Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
  Schema schemaPerson = new Schema(asList(name, age));

  @Test
  public void testInt() throws IOException {

    BufferAllocator allocator =
        ArrowUtils.rootAllocator().newChildAllocator("ReadIntTest", 0, Long.MAX_VALUE);

    byte[] batchBytes = mockData();
    ByteArrayInputStream in = new ByteArrayInputStream(batchBytes);
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
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
            //            ArrowFileWriter writer = new ArrowFileWriter(vectorSchemaRoot, null,
            // Channels.newChannel(out))
            WriteChannel writeChannel = new WriteChannel(Channels.newChannel(out))) {
          ArrowRecordBatch batch = unloader.getRecordBatch();
          MessageSerializer.serialize(writeChannel, batch);
          batch.close();
          //          writer.start();
          //          writer.writeBatch();
          //          System.out.println("Record batches written: " +
          // writer.getRecordBlocks().size() +
          //              ". Number of rows written: " + vectorSchemaRoot.getRowCount());
          return out.toByteArray();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return null;
  }
}
