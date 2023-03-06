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

// This lexer should follow Trino `https://github.com/trinodb/trino/blob/master/core/trino-parser/src/main/antlr4/io/trino/sql/parser/SqlBase.g4`

lexer grammar KyuubiTrinoFeBaseLexer;

SEMICOLON: ';';
LEFT_PAREN: '(';
RIGHT_PAREN: ')';

AND: 'AND';
BQ: '`';
BY: 'BY';
COMMA: ',';
DOT: '.';
EQ  : '=' | '==';
NSEQ: '<=>';
NEQ : '<>';
NEQJ: '!=';
LT  : '<';
LTE : '<=' | '!>';
GT  : '>';
GTE : '>=' | '!<';
OR: 'OR';

FROM: 'FROM';
FALSE: 'FALSE';
LIKE: 'LIKE';
IN: 'IN';
WHERE: 'WHERE';

ESCAPE: 'ESCAPE';
AUTO_INCREMENT: 'AUTO_INCREMENT';
CASE_SENSITIVE: 'CASE_SENSITIVE';
CREATE_PARAMS: 'CREATE_PARAMS';
DATA_TYPE: 'DATA_TYPE';
FIXED_PREC_SCALE: 'FIXED_PREC_SCALE';
IS: 'IS';
LITERAL_PREFIX: 'LITERAL_PREFIX';
LITERAL_SUFFIX: 'LITERAL_SUFFIX';
LOCAL_TYPE_NAME: 'LOCAL_TYPE_NAME';
MAXIMUM_SCALE: 'MAXIMUM_SCALE';
MINIMUM_SCALE: 'MINIMUM_SCALE';
NULL: 'NULL';
NULLABLE: 'NULLABLE';
NUM_PREC_RADIX: 'NUM_PREC_RADIX';
ORDER: 'ORDER';
PRECISION: 'PRECISION';
REMARKS: 'REMARKS';
REF_GENERATION: 'REF_GENERATION';
SEARCHABLE: 'SEARCHABLE';
SELECT: 'SELECT';
SQL_DATA_TYPE: 'SQL_DATA_TYPE';
SQL_DATETIME_SUB: 'SQL_DATETIME_SUB';
SYSTEM_JDBC_CATALOGS: 'SYSTEM.JDBC.CATALOGS';
SYSTEM_JDBC_SCHEMAS: 'SYSTEM.JDBC.SCHEMAS';
SYSTEM_JDBC_TABLES: 'SYSTEM.JDBC.TABLES';
SYSTEM_JDBC_COLUMNS: 'SYSTEM.JDBC.COLUMNS';
SYSTEM_JDBC_TABLE_TYPES: 'SYSTEM.JDBC.TABLE_TYPES';
SYSTEM_JDBC_TYPES: 'SYSTEM.JDBC.TYPES';
SELF_REFERENCING_COL_NAME: 'SELF_REFERENCING_COL_NAME';
UNSIGNED_ATTRIBUTE: 'UNSIGNED_ATTRIBUTE';
TABLE_CAT: 'TABLE_CAT';
TABLE_CATALOG: 'TABLE_CATALOG';
TABLE_NAME: 'TABLE_NAME';
TABLE_SCHEM: 'TABLE_SCHEM';
TABLE_TYPE: 'TABLE_TYPE';
TYPE_CAT: 'TYPE_CAT';
TYPE_NAME: 'TYPE_NAME';
TYPE_SCHEM: 'TYPE_SCHEM';
COLUMN_NAME: 'COLUMN_NAME';
COLUMN_SIZE: 'COLUMN_SIZE';
BUFFER_LENGTH: 'BUFFER_LENGTH';
DECIMAL_DIGITS: 'DECIMAL_DIGITS';
COLUMN_DEF: 'COLUMN_DEF';
CHAR_OCTET_LENGTH: 'CHAR_OCTET_LENGTH';
ORDINAL_POSITION: 'ORDINAL_POSITION';
IS_NULLABLE: 'IS_NULLABLE';
SCOPE_CATALOG: 'SCOPE_CATALOG';
SCOPE_SCHEMA: 'SCOPE_SCHEMA';
SCOPE_TABLE: 'SCOPE_TABLE';
SOURCE_DATA_TYPE: 'SOURCE_DATA_TYPE';
IS_AUTOINCREMENT: 'IS_AUTOINCREMENT';
IS_GENERATEDCOLUMN: 'IS_GENERATEDCOLUMN';
VARCHAR: 'VARCHAR';
SMALLINT: 'SMALLINT';
CAST: 'CAST';
AS: 'AS';
KEY_SEQ: 'KEY_SEQ';
PK_NAME: 'PK_NAME';

fragment SEARCH_STRING_ESCAPE: '\'' '\\' '\'';

STRING_ESCAPE
    : SEARCH_STRING_ESCAPE
    ;

STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

WS  : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;
