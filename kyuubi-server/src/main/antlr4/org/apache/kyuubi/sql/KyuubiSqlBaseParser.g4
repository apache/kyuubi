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

parser grammar KyuubiSqlBaseParser;

options { tokenVocab = KyuubiSqlBaseLexer; }

singleStatement
    : statement SEMICOLON* EOF
    ;

statement
    : runnable                                                      #logicalRunnable
    | translate                                                     #logicalTranslate
    | .*?                                                           #passThrough
    ;

runnable
    : CREATE ENGINE                                                   #creareEngine
    | DROP ENGINE                                                     #dropEngine
    | ALTER SESSION SET propertyList                                  #alterEngineConfig
    ;

// Note: translate only works with mysql fe protocol
translate
    : RENAME TABLE identifier TO identifier                         #renameTable
    ;

propertyList
    : LEFT_PAREN property (COMMA property)* RIGHT_PAREN
    ;

property
    : key=propertyKey (EQ? value=propertyValue)?
    ;

propertyKey
    : identifier (DOT identifier)*
    | stringLit
    ;

propertyValue
    : INTEGER_VALUE
    | DECIMAL_VALUE
    | booleanValue
    | stringLit
    ;

identifier
    : strictIdentifier
    ;

strictIdentifier
    : IDENTIFIER              #unquotedIdentifier
    | quotedIdentifier        #quotedIdentifierAlternative
    | nonReserved             #unquotedIdentifier
    ;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

nonReserved
    : ALTER
    | CREATE
    | DROP
    | ENGINE
    | FALSE
    | RENAME
    | SET
    | SESSION
    | TABLE
    | TO
    | TRUE
    ;

stringLit
    : STRING
    ;

booleanValue
    : TRUE | FALSE
    ;
