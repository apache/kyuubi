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

parser grammar KyuubiTrinoFeBaseParser;

options { tokenVocab = KyuubiTrinoFeBaseLexer; }

singleStatement
    : statement SEMICOLON* EOF
    ;

statement
    : SELECT TABLE_SCHEM COMMA TABLE_CATALOG FROM SYSTEM_JDBC_SCHEMAS
      (WHERE tableCatalogFilter? AND? tableSchemaFilter?)?
      ORDER BY TABLE_CATALOG COMMA TABLE_SCHEM                                                          #getSchemas
    | SELECT TABLE_CAT FROM SYSTEM_JDBC_CATALOGS ORDER BY TABLE_CAT                                     #getCatalogs
    | SELECT TABLE_TYPE FROM SYSTEM_JDBC_TABLE_TYPES ORDER BY TABLE_TYPE                                #getTableTypes
    | SELECT TYPE_NAME COMMA DATA_TYPE COMMA PRECISION COMMA LITERAL_PREFIX COMMA
      LITERAL_SUFFIX COMMA CREATE_PARAMS COMMA NULLABLE COMMA CASE_SENSITIVE COMMA
      SEARCHABLE COMMA UNSIGNED_ATTRIBUTE COMMA FIXED_PREC_SCALE COMMA AUTO_INCREMENT
      COMMA LOCAL_TYPE_NAME COMMA MINIMUM_SCALE COMMA MAXIMUM_SCALE COMMA SQL_DATA_TYPE
      COMMA SQL_DATETIME_SUB COMMA NUM_PREC_RADIX FROM SYSTEM_JDBC_TYPES ORDER BY DATA_TYPE             #getTypeInfo
    | SELECT TABLE_CAT COMMA TABLE_SCHEM COMMA TABLE_NAME COMMA TABLE_TYPE COMMA REMARKS COMMA
      TYPE_CAT COMMA TYPE_SCHEM COMMA TYPE_NAME COMMA SELF_REFERENCING_COL_NAME COMMA REF_GENERATION
      FROM SYSTEM_JDBC_TABLES
      (WHERE tableCatalogFilter? AND? tableSchemaFilter? AND? tableNameFilter? AND? tableTypeFilter?)?
      ORDER BY TABLE_TYPE COMMA TABLE_CAT COMMA TABLE_SCHEM COMMA TABLE_NAME                            #getTables
    | SELECT TABLE_CAT COMMA TABLE_SCHEM COMMA TABLE_NAME COMMA COLUMN_NAME COMMA DATA_TYPE COMMA
      TYPE_NAME COMMA COLUMN_SIZE COMMA BUFFER_LENGTH COMMA DECIMAL_DIGITS COMMA NUM_PREC_RADIX COMMA
      NULLABLE COMMA REMARKS COMMA COLUMN_DEF COMMA SQL_DATA_TYPE COMMA SQL_DATETIME_SUB COMMA
      CHAR_OCTET_LENGTH COMMA ORDINAL_POSITION COMMA IS_NULLABLE COMMA
      SCOPE_CATALOG COMMA SCOPE_SCHEMA COMMA SCOPE_TABLE COMMA
      SOURCE_DATA_TYPE COMMA IS_AUTOINCREMENT COMMA IS_GENERATEDCOLUMN FROM SYSTEM_JDBC_COLUMNS
      (WHERE tableCatalogFilter? AND? tableSchemaFilter? AND? tableNameFilter? AND? colNameFilter?)?
      ORDER BY TABLE_CAT COMMA TABLE_SCHEM COMMA TABLE_NAME COMMA ORDINAL_POSITION                      #getColumns
    | SELECT CAST LEFT_PAREN NULL AS VARCHAR RIGHT_PAREN TABLE_CAT COMMA
      CAST LEFT_PAREN NULL AS VARCHAR RIGHT_PAREN TABLE_SCHEM COMMA
      CAST LEFT_PAREN NULL AS VARCHAR RIGHT_PAREN TABLE_NAME COMMA
      CAST LEFT_PAREN NULL AS VARCHAR RIGHT_PAREN COLUMN_NAME COMMA
      CAST LEFT_PAREN NULL AS SMALLINT RIGHT_PAREN KEY_SEQ COMMA
      CAST LEFT_PAREN NULL AS VARCHAR RIGHT_PAREN PK_NAME
      WHERE FALSE                                                                                       #getPrimaryKeys
    | .*?                                                                                               #passThrough
    ;

tableCatalogFilter
     : (TABLE_CAT | TABLE_CATALOG) IS NULL                                                           #nullCatalog
     | (TABLE_CAT | TABLE_CATALOG) EQ catalog=STRING+                                                #catalogFilter
     ;

tableSchemaFilter
    : TABLE_SCHEM IS NULL                                                                               #nulTableSchema
    | TABLE_SCHEM LIKE schemaPattern=stringLit ESCAPE STRING_ESCAPE                                     #schemaFilter
    ;

tableNameFilter
    : TABLE_NAME LIKE tableNamePattern=stringLit ESCAPE STRING_ESCAPE
    ;

colNameFilter
    : COLUMN_NAME LIKE colNamePattern=stringLit ESCAPE STRING_ESCAPE
    ;

tableTypeFilter
    : FALSE                                                                                             #tableTypesAlwaysFalse
    | TABLE_TYPE IN LEFT_PAREN stringLit (COMMA stringLit)* RIGHT_PAREN                                 #typesFilter
    ;

stringLit
    : STRING
    ;
