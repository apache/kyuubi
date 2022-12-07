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

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hive.service.rpc.thrift.TTypeId;
import org.apache.kyuubi.jdbc.hive.JdbcColumnAttributes;

public class ArrowUtils {

  public static final RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);

  public static Schema toArrowSchema(
      List<String> columnNames, List<TTypeId> ttypes, List<JdbcColumnAttributes> columnAttributes) {
    List<Field> fields =
        IntStream.range(0, columnNames.size())
            .mapToObj(i -> toArrowField(columnNames.get(i), ttypes.get(i), columnAttributes.get(i)))
            .collect(Collectors.toList());
    return new Schema(fields);
  }

  public static Field toArrowField(
      String name, TTypeId ttype, JdbcColumnAttributes jdbcColumnAttributes) {
    boolean nullable = true;
    return new Field(
        name,
        new FieldType(nullable, toArrowType(ttype, jdbcColumnAttributes), null),
        Collections.emptyList());
  }

  public static ArrowType toArrowType(TTypeId ttype, JdbcColumnAttributes jdbcColumnAttributes) {
    switch (ttype) {
      case NULL_TYPE:
        return ArrowType.Null.INSTANCE;
      case BOOLEAN_TYPE:
        return ArrowType.Bool.INSTANCE;
      case TINYINT_TYPE:
        return new ArrowType.Int(8, true);
      case SMALLINT_TYPE:
        return new ArrowType.Int(8 * 2, true);
      case INT_TYPE:
        return new ArrowType.Int(8 * 4, true);
      case BIGINT_TYPE:
        return new ArrowType.Int(8 * 8, true);
      case FLOAT_TYPE:
        return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      case DOUBLE_TYPE:
        return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      case STRING_TYPE:
        return ArrowType.Utf8.INSTANCE;
      case DECIMAL_TYPE:
        if (jdbcColumnAttributes != null) {
          return ArrowType.Decimal.createDecimal(
              jdbcColumnAttributes.precision, jdbcColumnAttributes.scale, null);
        }
      case DATE_TYPE:
        return new ArrowType.Date(DateUnit.DAY);
      case TIMESTAMP_TYPE:
        if (jdbcColumnAttributes != null) {
          return new ArrowType.Timestamp(TimeUnit.MICROSECOND, jdbcColumnAttributes.timeZone);
        } else {
          throw new IllegalStateException("Missing timezoneId where it is mandatory.");
        }
      case BINARY_TYPE:
        return ArrowType.Binary.INSTANCE;
      case INTERVAL_DAY_TIME_TYPE:
        return new ArrowType.Duration(TimeUnit.MICROSECOND);
      case INTERVAL_YEAR_MONTH_TYPE:
        return new ArrowType.Interval(IntervalUnit.YEAR_MONTH);
      default:
        throw new IllegalArgumentException("Unrecognized type name: " + ttype.name());
    }
  }
}
