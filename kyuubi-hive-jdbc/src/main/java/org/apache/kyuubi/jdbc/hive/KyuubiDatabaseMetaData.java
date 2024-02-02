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

import static org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTypeId.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.jar.Attributes;
import org.apache.kyuubi.jdbc.KyuubiHiveDriver;
import org.apache.kyuubi.jdbc.hive.adapter.SQLDatabaseMetaData;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.*;
import org.apache.kyuubi.shaded.thrift.TException;

/** KyuubiDatabaseMetaData. */
public class KyuubiDatabaseMetaData implements SQLDatabaseMetaData {

  private final KyuubiConnection connection;
  private final TProtocolVersion protocol;
  private final TCLIService.Iface client;
  private final TSessionHandle sessHandle;
  private static final String CATALOG_SEPARATOR = ".";

  private static final char SEARCH_STRING_ESCAPE = '\\';

  //  The maximum column length = MFieldSchema.FNAME in metastore/src/model/package.jdo
  private static final int maxColumnNameLength = 128;

  //  Cached values, to save on round trips to database.
  private String dbVersion = null;

  public KyuubiDatabaseMetaData(
      KyuubiConnection connection,
      TProtocolVersion protocol,
      TCLIService.Iface client,
      TSessionHandle sessHandle) {
    this.connection = connection;
    this.protocol = protocol;
    this.client = client;
    this.sessHandle = sessHandle;
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    return true;
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    return CATALOG_SEPARATOR;
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    return "catalog";
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    TGetCatalogsResp catalogResp;

    try {
      catalogResp = client.GetCatalogs(new TGetCatalogsReq(sessHandle));
    } catch (TException e) {
      throw new KyuubiSQLException(e.getMessage(), "08S01", e);
    }
    Utils.verifySuccess(catalogResp.getStatus());

    return new KyuubiQueryResultSet.Builder(connection)
        .setClient(client)
        .setSessionHandle(sessHandle)
        .setStmtHandle(catalogResp.getOperationHandle())
        .build();
  }

  private static final class ClientInfoPropertiesResultSet extends KyuubiMetaDataResultSet<Object> {
    private static final String[] COLUMNS = {"NAME", "MAX_LEN", "DEFAULT_VALUE", "DESCRIPTION"};
    private static final TTypeId[] COLUMN_TYPES = {STRING_TYPE, INT_TYPE, STRING_TYPE, STRING_TYPE};

    private static final Object[][] DATA = {
      {"ApplicationName", 1000, null, null},
      // Note: other standard ones include e.g. ClientUser and ClientHostname,
      //       but we don't need them for now.
    };
    private int index = -1;

    public ClientInfoPropertiesResultSet() throws SQLException {
      super(Arrays.asList(COLUMNS), Arrays.asList(COLUMN_TYPES), null);
      List<TColumnDesc> fieldSchemas = new ArrayList<>(COLUMNS.length);
      for (int i = 0; i < COLUMNS.length; ++i) {
        TColumnDesc tColumnDesc = new TColumnDesc();
        tColumnDesc.setColumnName(COLUMNS[i]);
        TTypeDesc tTypeDesc = new TTypeDesc();
        tTypeDesc.addToTypes(TTypeEntry.primitiveEntry(new TPrimitiveTypeEntry(COLUMN_TYPES[i])));
        tColumnDesc.setTypeDesc(tTypeDesc);
        tColumnDesc.setPosition(i);
        fieldSchemas.add(tColumnDesc);
      }
      setSchema(new TTableSchema(fieldSchemas));
    }

    @Override
    public boolean next() throws SQLException {
      if ((++index) >= DATA.length) return false;
      row = Arrays.copyOf(DATA[index], DATA[index].length);
      return true;
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
      for (int i = 0; i < COLUMNS.length; ++i) {
        if (COLUMNS[i].equalsIgnoreCase(columnLabel)) return getObject(i, type);
      }
      throw new KyuubiSQLException("No column " + columnLabel);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
      // TODO: perhaps this could use a better implementation... for now even the Hive query result
      //       set doesn't support this, so assume the user knows what he's doing when calling us.
      return (T) super.getObject(columnIndex);
    }
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    return new ClientInfoPropertiesResultSet();
  }

  /**
   * Convert a pattern containing JDBC catalog search wildcards into Java regex patterns.
   *
   * @param pattern input which may contain '%' or '_' wildcard characters, or these characters
   *     escaped using {@link #getSearchStringEscape()}.
   * @return replace %/_ with regex search characters, also handle escaped characters.
   */
  private String convertPattern(final String pattern) {
    if (pattern == null) {
      return ".*";
    } else {
      StringBuilder result = new StringBuilder(pattern.length());

      boolean escaped = false;
      for (int i = 0, len = pattern.length(); i < len; i++) {
        char c = pattern.charAt(i);
        if (escaped) {
          if (c != SEARCH_STRING_ESCAPE) {
            escaped = false;
          }
          result.append(c);
        } else {
          if (c == SEARCH_STRING_ESCAPE) {
            escaped = true;
            continue;
          } else if (c == '%') {
            result.append(".*");
          } else if (c == '_') {
            result.append('.');
          } else {
            result.append(Character.toLowerCase(c));
          }
        }
      }

      return result.toString();
    }
  }

  @Override
  public ResultSet getColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    TGetColumnsResp colResp;
    TGetColumnsReq colReq = new TGetColumnsReq();
    colReq.setSessionHandle(sessHandle);
    colReq.setCatalogName(catalog);
    colReq.setSchemaName(schemaPattern);
    colReq.setTableName(tableNamePattern);
    colReq.setColumnName(columnNamePattern);
    try {
      colResp = client.GetColumns(colReq);
    } catch (TException e) {
      throw new KyuubiSQLException(e.getMessage(), "08S01", e);
    }
    Utils.verifySuccess(colResp.getStatus());
    // build the resultset from response
    return new KyuubiQueryResultSet.Builder(connection)
        .setClient(client)
        .setSessionHandle(sessHandle)
        .setStmtHandle(colResp.getOperationHandle())
        .build();
  }

  /**
   * We sort the output of getColumns to guarantee jdbc compliance. First check by table name then
   * by ordinal position
   */
  private class GetColumnsComparator implements Comparator<JdbcColumn> {
    @Override
    public int compare(JdbcColumn o1, JdbcColumn o2) {
      int compareName = o1.getTableName().compareTo(o2.getTableName());
      if (compareName == 0) {
        if (o1.getOrdinalPos() > o2.getOrdinalPos()) {
          return 1;
        } else if (o1.getOrdinalPos() < o2.getOrdinalPos()) {
          return -1;
        }
        return 0;
      } else {
        return compareName;
      }
    }
  }

  @Override
  public Connection getConnection() throws SQLException {
    return this.connection;
  }

  @Override
  public ResultSet getCrossReference(
      String primaryCatalog,
      String primarySchema,
      String primaryTable,
      String foreignCatalog,
      String foreignSchema,
      String foreignTable)
      throws SQLException {
    TGetCrossReferenceResp getFKResp;
    TGetCrossReferenceReq getFKReq = new TGetCrossReferenceReq(sessHandle);
    getFKReq.setParentTableName(primaryTable);
    getFKReq.setParentSchemaName(primarySchema);
    getFKReq.setParentCatalogName(primaryCatalog);
    getFKReq.setForeignTableName(foreignTable);
    getFKReq.setForeignSchemaName(foreignSchema);
    getFKReq.setForeignCatalogName(foreignCatalog);

    try {
      getFKResp = client.GetCrossReference(getFKReq);
    } catch (TException e) {
      throw new KyuubiSQLException(e.getMessage(), "08S01", e);
    }
    Utils.verifySuccess(getFKResp.getStatus());

    return new KyuubiQueryResultSet.Builder(connection)
        .setClient(client)
        .setSessionHandle(sessHandle)
        .setStmtHandle(getFKResp.getOperationHandle())
        .build();
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    return Utils.getVersionPart(getDatabaseProductVersion(), 0);
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    return Utils.getVersionPart(getDatabaseProductVersion(), 1);
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    TGetInfoResp resp = getServerInfo(TGetInfoType.CLI_DBMS_NAME);
    return resp.getInfoValue().getStringValue();
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    if (dbVersion != null) { // lazy-caching of the version.
      return dbVersion;
    }

    TGetInfoResp resp = getServerInfo(TGetInfoType.CLI_DBMS_VER);
    this.dbVersion = resp.getInfoValue().getStringValue();
    return dbVersion;
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  @Override
  public int getDriverMajorVersion() {
    return KyuubiHiveDriver.getMajorDriverVersion();
  }

  @Override
  public int getDriverMinorVersion() {
    return KyuubiHiveDriver.getMinorDriverVersion();
  }

  @Override
  public String getDriverName() throws SQLException {
    return KyuubiHiveDriver.fetchManifestAttribute(Attributes.Name.IMPLEMENTATION_TITLE);
  }

  @Override
  public String getDriverVersion() throws SQLException {
    return KyuubiHiveDriver.fetchManifestAttribute(Attributes.Name.IMPLEMENTATION_VERSION);
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    // TODO: verify that this is correct
    return "";
  }

  @Override
  public ResultSet getFunctions(
      String catalogName, String schemaPattern, String functionNamePattern) throws SQLException {
    TGetFunctionsResp funcResp;
    TGetFunctionsReq getFunctionsReq = new TGetFunctionsReq();
    getFunctionsReq.setSessionHandle(sessHandle);
    getFunctionsReq.setCatalogName(catalogName);
    getFunctionsReq.setSchemaName(schemaPattern);
    getFunctionsReq.setFunctionName(functionNamePattern);

    try {
      funcResp = client.GetFunctions(getFunctionsReq);
    } catch (TException e) {
      throw new KyuubiSQLException(e.getMessage(), "08S01", e);
    }
    Utils.verifySuccess(funcResp.getStatus());

    return new KyuubiQueryResultSet.Builder(connection)
        .setClient(client)
        .setSessionHandle(sessHandle)
        .setStmtHandle(funcResp.getOperationHandle())
        .build();
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    return " ";
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table)
      throws SQLException {
    return new KyuubiQueryResultSet.Builder(connection)
        .setClient(client)
        .setEmptyResultSet(true)
        .setSchema(
            Arrays.asList(
                "PKTABLE_CAT",
                "PKTABLE_SCHEM",
                "PKTABLE_NAME",
                "PKCOLUMN_NAME",
                "FKTABLE_CAT",
                "FKTABLE_SCHEM",
                "FKTABLE_NAME",
                "FKCOLUMN_NAME",
                "KEY_SEQ",
                "UPDATE_RULE",
                "DELETE_RULE",
                "FK_NAME",
                "PK_NAME",
                "DEFERRABILITY"),
            Arrays.asList(
                STRING_TYPE,
                STRING_TYPE,
                STRING_TYPE,
                STRING_TYPE,
                STRING_TYPE,
                STRING_TYPE,
                STRING_TYPE,
                STRING_TYPE,
                SMALLINT_TYPE,
                SMALLINT_TYPE,
                SMALLINT_TYPE,
                STRING_TYPE,
                STRING_TYPE,
                STRING_TYPE))
        .build();
  }

  @Override
  public ResultSet getIndexInfo(
      String catalog, String schema, String table, boolean unique, boolean approximate)
      throws SQLException {
    return new KyuubiQueryResultSet.Builder(connection)
        .setClient(client)
        .setEmptyResultSet(true)
        .setSchema(
            Arrays.asList(
                "TABLE_CAT",
                "TABLE_SCHEM",
                "TABLE_NAME",
                "NON_UNIQUE",
                "INDEX_QUALIFIER",
                "INDEX_NAME",
                "TYPE",
                "ORDINAL_POSITION",
                "COLUMN_NAME",
                "ASC_OR_DESC",
                "CARDINALITY",
                "PAGES",
                "FILTER_CONDITION"),
            Arrays.asList(
                STRING_TYPE,
                STRING_TYPE,
                STRING_TYPE,
                BOOLEAN_TYPE,
                STRING_TYPE,
                STRING_TYPE,
                SMALLINT_TYPE,
                SMALLINT_TYPE,
                STRING_TYPE,
                STRING_TYPE,
                INT_TYPE,
                INT_TYPE,
                STRING_TYPE))
        .build();
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    return 3;
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    return 0;
  }

  /** Returns the value of maxColumnNameLength. */
  @Override
  public int getMaxColumnNameLength() throws SQLException {
    return maxColumnNameLength;
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    return "";
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
    TGetPrimaryKeysResp getPKResp;
    TGetPrimaryKeysReq getPKReq = new TGetPrimaryKeysReq(sessHandle);
    getPKReq.setTableName(table);
    getPKReq.setSchemaName(schema);
    getPKReq.setCatalogName(catalog);
    try {
      getPKResp = client.GetPrimaryKeys(getPKReq);
    } catch (TException e) {
      throw new KyuubiSQLException(e.getMessage(), "08S01", e);
    }
    Utils.verifySuccess(getPKResp.getStatus());

    return new KyuubiQueryResultSet.Builder(connection)
        .setClient(client)
        .setSessionHandle(sessHandle)
        .setStmtHandle(getPKResp.getOperationHandle())
        .build();
  }

  @Override
  public ResultSet getProcedureColumns(
      String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern)
      throws SQLException {
    // Hive doesn't support primary keys
    // using local schema with empty resultset
    return new KyuubiQueryResultSet.Builder(connection)
        .setClient(client)
        .setEmptyResultSet(true)
        .setSchema(
            Arrays.asList(
                "PROCEDURE_CAT",
                "PROCEDURE_SCHEM",
                "PROCEDURE_NAME",
                "COLUMN_NAME",
                "COLUMN_TYPE",
                "DATA_TYPE",
                "TYPE_NAME",
                "PRECISION",
                "LENGTH",
                "SCALE",
                "RADIX",
                "NULLABLE",
                "REMARKS",
                "COLUMN_DEF",
                "SQL_DATA_TYPE",
                "SQL_DATETIME_SUB",
                "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION",
                "IS_NULLABLE",
                "SPECIFIC_NAME"),
            Arrays.asList(
                STRING_TYPE,
                STRING_TYPE,
                STRING_TYPE,
                STRING_TYPE,
                SMALLINT_TYPE,
                INT_TYPE,
                STRING_TYPE,
                INT_TYPE,
                INT_TYPE,
                SMALLINT_TYPE,
                SMALLINT_TYPE,
                SMALLINT_TYPE,
                STRING_TYPE,
                STRING_TYPE,
                INT_TYPE,
                INT_TYPE,
                INT_TYPE,
                INT_TYPE,
                STRING_TYPE,
                STRING_TYPE))
        .build();
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    return "UDF";
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
      throws SQLException {
    // Hive doesn't support primary keys
    // using local schema with empty resultset
    return new KyuubiQueryResultSet.Builder(connection)
        .setClient(client)
        .setEmptyResultSet(true)
        .setSchema(
            Arrays.asList(
                "PROCEDURE_CAT",
                "PROCEDURE_SCHEM",
                "PROCEDURE_NAME",
                "RESERVERD",
                "RESERVERD",
                "RESERVERD",
                "REMARKS",
                "PROCEDURE_TYPE",
                "SPECIFIC_NAME"),
            Arrays.asList(
                STRING_TYPE,
                STRING_TYPE,
                STRING_TYPE,
                STRING_TYPE,
                STRING_TYPE,
                STRING_TYPE,
                STRING_TYPE,
                STRING_TYPE,
                STRING_TYPE))
        .build();
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    if (protocol.compareTo(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11) < 0) {
      throw new SQLFeatureNotSupportedException(
          String.format(
              "Feature is not supported, protocol version is %s, requires %s or higher",
              protocol, TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11));
    }
    // Note: the definitions of what ODBC and JDBC keywords exclude are different in different
    //       places. For now, just return the ODBC version here; that excludes Hive keywords
    //       that are also ODBC reserved keywords. We could also exclude SQL:2003.
    TGetInfoResp resp = getServerInfo(TGetInfoType.CLI_ODBC_KEYWORDS);
    return resp.getInfoValue().getStringValue();
  }

  @Override
  public int getSQLStateType() throws SQLException {
    return DatabaseMetaData.sqlStateSQL99;
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    return "database";
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    return getSchemas(null, null);
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    TGetSchemasResp schemaResp;

    TGetSchemasReq schemaReq = new TGetSchemasReq();
    schemaReq.setSessionHandle(sessHandle);
    if (catalog != null) {
      schemaReq.setCatalogName(catalog);
    }
    if (schemaPattern == null) {
      schemaPattern = "%";
    }
    schemaReq.setSchemaName(schemaPattern);

    try {
      schemaResp = client.GetSchemas(schemaReq);
    } catch (TException e) {
      throw new KyuubiSQLException(e.getMessage(), "08S01", e);
    }
    Utils.verifySuccess(schemaResp.getStatus());

    return new KyuubiQueryResultSet.Builder(connection)
        .setClient(client)
        .setSessionHandle(sessHandle)
        .setStmtHandle(schemaResp.getOperationHandle())
        .build();
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    return String.valueOf(SEARCH_STRING_ESCAPE);
  }

  @Override
  public String getStringFunctions() throws SQLException {
    return "";
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    return "";
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    TGetTableTypesResp tableTypeResp;

    try {
      tableTypeResp = client.GetTableTypes(new TGetTableTypesReq(sessHandle));
    } catch (TException e) {
      throw new KyuubiSQLException(e.getMessage(), "08S01", e);
    }
    Utils.verifySuccess(tableTypeResp.getStatus());

    return new KyuubiQueryResultSet.Builder(connection)
        .setClient(client)
        .setSessionHandle(sessHandle)
        .setStmtHandle(tableTypeResp.getOperationHandle())
        .build();
  }

  @Override
  public ResultSet getTables(
      String catalog, String schemaPattern, String tableNamePattern, String[] types)
      throws SQLException {

    TGetTablesReq getTableReq = new TGetTablesReq(sessHandle);
    getTableReq.setCatalogName(catalog);
    getTableReq.setSchemaName(schemaPattern == null ? "%" : schemaPattern);
    getTableReq.setTableName(tableNamePattern);
    if (types != null) {
      getTableReq.setTableTypes(Arrays.asList(types));
    }
    TGetTablesResp getTableResp;
    try {
      getTableResp = client.GetTables(getTableReq);
    } catch (TException rethrow) {
      throw new KyuubiSQLException(rethrow.getMessage(), "08S01", rethrow);
    }
    TStatus tStatus = getTableResp.getStatus();
    if (tStatus.getStatusCode() != TStatusCode.SUCCESS_STATUS) {
      throw new KyuubiSQLException(tStatus);
    }
    return new KyuubiQueryResultSet.Builder(connection)
        .setClient(client)
        .setSessionHandle(sessHandle)
        .setStmtHandle(getTableResp.getOperationHandle())
        .build();
  }

  /**
   * We sort the output of getTables to guarantee jdbc compliance. First check by table type then by
   * table name
   */
  private class GetTablesComparator implements Comparator<JdbcTable> {
    @Override
    public int compare(JdbcTable o1, JdbcTable o2) {
      int compareType = o1.getType().compareTo(o2.getType());
      if (compareType == 0) {
        return o1.getTableName().compareTo(o2.getTableName());
      } else {
        return compareType;
      }
    }
  }

  /** Translate hive table types into jdbc table types. */
  public static String toJdbcTableType(String hivetabletype) {
    if (hivetabletype == null) {
      return null;
    } else if (hivetabletype.equals("MANAGED_TABLE")) {
      return "TABLE";
    } else if (hivetabletype.equals("VIRTUAL_VIEW")) {
      return "VIEW";
    } else if (hivetabletype.equals("EXTERNAL_TABLE")) {
      return "EXTERNAL TABLE";
    } else if (hivetabletype.equals("MATERIALIZED_VIEW")) {
      return "MATERIALIZED VIEW";
    } else {
      return hivetabletype;
    }
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    return "";
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    TGetTypeInfoResp getTypeInfoResp;
    TGetTypeInfoReq getTypeInfoReq = new TGetTypeInfoReq();
    getTypeInfoReq.setSessionHandle(sessHandle);
    try {
      getTypeInfoResp = client.GetTypeInfo(getTypeInfoReq);
    } catch (TException e) {
      throw new KyuubiSQLException(e.getMessage(), "08S01", e);
    }
    Utils.verifySuccess(getTypeInfoResp.getStatus());
    return new KyuubiQueryResultSet.Builder(connection)
        .setClient(client)
        .setSessionHandle(sessHandle)
        .setStmtHandle(getTypeInfoResp.getOperationHandle())
        .build();
  }

  @Override
  public ResultSet getUDTs(
      String catalog, String schemaPattern, String typeNamePattern, int[] types)
      throws SQLException {

    return new KyuubiMetaDataResultSet<Object>(
        Arrays.asList(
            "TYPE_CAT",
            "TYPE_SCHEM",
            "TYPE_NAME",
            "CLASS_NAME",
            "DATA_TYPE",
            "REMARKS",
            "BASE_TYPE"),
        Arrays.asList(
            STRING_TYPE, STRING_TYPE, STRING_TYPE, STRING_TYPE, INT_TYPE, STRING_TYPE, INT_TYPE),
        null) {

      @Override
      public boolean next() throws SQLException {
        return false;
      }
    };
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    return false;
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    return true;
  }

  private TGetInfoResp getServerInfo(TGetInfoType type) throws SQLException {
    TGetInfoReq req = new TGetInfoReq(sessHandle, type);
    TGetInfoResp resp;
    try {
      resp = client.GetInfo(req);
    } catch (TException e) {
      throw new KyuubiSQLException(e.getMessage(), "08S01", e);
    }
    Utils.verifySuccess(resp.getStatus());
    return resp;
  }
}
