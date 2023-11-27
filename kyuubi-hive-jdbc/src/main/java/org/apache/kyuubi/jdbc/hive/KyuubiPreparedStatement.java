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

package org.apache.kyuubi.jdbc.hive;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Scanner;
import org.apache.kyuubi.jdbc.hive.adapter.SQLPreparedStatement;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TCLIService;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TSessionHandle;

/** KyuubiPreparedStatement. */
public class KyuubiPreparedStatement extends KyuubiStatement implements SQLPreparedStatement {
  private final String sql;

  /** save the SQL parameters {paramLoc:paramValue} */
  private final HashMap<Integer, String> parameters = new HashMap<>();

  public KyuubiPreparedStatement(
      KyuubiConnection connection,
      TCLIService.Iface client,
      TSessionHandle sessHandle,
      String sql) {
    super(connection, client, sessHandle);
    this.sql = sql;
  }

  @Override
  public void clearParameters() throws SQLException {
    this.parameters.clear();
  }

  /**
   * Invokes executeQuery(sql) using the sql provided to the constructor.
   *
   * @return boolean Returns true if a resultSet is created, false if not. Note: If the result set
   *     is empty a true is returned.
   */
  @Override
  public boolean execute() throws SQLException {
    return super.execute(updateSql(sql, parameters));
  }

  /** Invokes executeQuery(sql) using the sql provided to the constructor. */
  @Override
  public ResultSet executeQuery() throws SQLException {
    return super.executeQuery(updateSql(sql, parameters));
  }

  @Override
  public int executeUpdate() throws SQLException {
    super.executeUpdate(updateSql(sql, parameters));
    return 0;
  }

  /** update the SQL string with parameters set by setXXX methods of {@link PreparedStatement} */
  private String updateSql(final String sql, HashMap<Integer, String> parameters)
      throws SQLException {
    return Utils.updateSql(sql, parameters);
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    this.parameters.put(parameterIndex, x.toString());
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    String str = new Scanner(x, "UTF-8").useDelimiter("\\A").next();
    setString(parameterIndex, str);
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    this.parameters.put(parameterIndex, "" + x);
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    this.parameters.put(parameterIndex, "" + x);
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    this.parameters.put(parameterIndex, "'" + x.toString() + "'");
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    this.parameters.put(parameterIndex, "" + x);
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    this.parameters.put(parameterIndex, "" + x);
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    this.parameters.put(parameterIndex, "" + x);
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    this.parameters.put(parameterIndex, "" + x);
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    this.parameters.put(parameterIndex, "NULL");
  }

  @Override
  public void setNull(int paramIndex, int sqlType, String typeName) throws SQLException {
    this.parameters.put(paramIndex, "NULL");
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    if (x == null) {
      setNull(parameterIndex, Types.NULL);
    } else if (x instanceof String) {
      setString(parameterIndex, (String) x);
    } else if (x instanceof Short) {
      setShort(parameterIndex, (Short) x);
    } else if (x instanceof Integer) {
      setInt(parameterIndex, (Integer) x);
    } else if (x instanceof Long) {
      setLong(parameterIndex, (Long) x);
    } else if (x instanceof Float) {
      setFloat(parameterIndex, (Float) x);
    } else if (x instanceof Double) {
      setDouble(parameterIndex, (Double) x);
    } else if (x instanceof Boolean) {
      setBoolean(parameterIndex, (Boolean) x);
    } else if (x instanceof Byte) {
      setByte(parameterIndex, (Byte) x);
    } else if (x instanceof Character) {
      setString(parameterIndex, x.toString());
    } else if (x instanceof Timestamp) {
      setTimestamp(parameterIndex, (Timestamp) x);
    } else if (x instanceof BigDecimal) {
      setString(parameterIndex, x.toString());
    } else {
      // Can't infer a type.
      throw new KyuubiSQLException(
          MessageFormat.format(
              "Cannot infer the SQL type to use for an instance of {0}. Use setObject() with an explicit Types value to specify the type to use.",
              x.getClass().getName()));
    }
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    this.parameters.put(parameterIndex, "" + x);
  }

  private String replaceBackSlashSingleQuote(String x) {
    // scrutinize escape pair, specifically, replace \' to '
    StringBuffer newX = new StringBuffer();
    for (int i = 0; i < x.length(); i++) {
      char c = x.charAt(i);
      if (c == '\\' && i < x.length() - 1) {
        char c1 = x.charAt(i + 1);
        if (c1 == '\'') {
          newX.append(c1);
        } else {
          newX.append(c);
          newX.append(c1);
        }
        i++;
      } else {
        newX.append(c);
      }
    }
    return newX.toString();
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    x = replaceBackSlashSingleQuote(x);
    x = x.replace("'", "\\'");
    this.parameters.put(parameterIndex, "'" + x + "'");
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    this.parameters.put(parameterIndex, "'" + x.toString() + "'");
  }
}
