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

options { tokenVocab = KyuubiSqlBaseLexer; }

singleStatement
    : statement SEMICOLON* EOF
    ;

statement
    : SELECT TABLE_SCHEM COMMA TABLE_CATALOG FROM SYSTEM_JDBC_SCHEMAS
      (WHERE (TABLE_CATALOG EQ catalog=STRING+)? AND? (TABLE_SCHEM LIKE schema=STRING+)?)?
      ORDER BY TABLE_CATALOG COMMA TABLE_SCHEM                                                #getSchemas
    | SELECT TABLE_CAT FROM SYSTEM_JDBC_CATALOGS ORDER BY TABLE_CAT                           #getCatalogs
    | SELECT TABLE_TYPE FROM SYSTEM_JDBC_TABLE_TYPES ORDER BY TABLE_TYPE                      #getTableTypes
    | .*?                                                                                     #passThrough
    ;
