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

grammar KyuubiSparkSQL;

@members {
   /**
    * Verify whether current token is a valid decimal token (which contains dot).
    * Returns true if the character that follows the token is not a digit or letter or underscore.
    *
    * For example:
    * For char stream "2.3", "2." is not a valid decimal token, because it is followed by digit '3'.
    * For char stream "2.3_", "2.3" is not a valid decimal token, because it is followed by '_'.
    * For char stream "2.3W", "2.3" is not a valid decimal token, because it is followed by 'W'.
    * For char stream "12.0D 34.E2+0.12 "  12.0D is a valid decimal token because it is followed
    * by a space. 34.E2 is a valid decimal token because it is followed by symbol '+'
    * which is not a digit or letter or underscore.
    */
   public boolean isValidDecimal() {
     int nextChar = _input.LA(1);
     if (nextChar >= 'A' && nextChar <= 'Z' || nextChar >= '0' && nextChar <= '9' ||
       nextChar == '_') {
       return false;
     } else {
       return true;
     }
   }
 }

tokens {
    DELIMITER
}

singleStatement
    : statement EOF
    ;

statement
    : OPTIMIZE multipartIdentifier whereClause? zorderClause        #optimizeZorder
    | .*?                                                           #passThrough
    ;

whereClause
    : WHERE partitionPredicate = predicateToken
    ;

zorderClause
    : ZORDER BY order+=multipartIdentifier (',' order+=multipartIdentifier)*
    ;

// We don't have an expression rule in our grammar here, so we just grab the tokens and defer
// parsing them to later.
predicateToken
    :  .+?
    ;

multipartIdentifier
    : parts+=identifier ('.' parts+=identifier)*
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
    : AND
    | BY
    | FALSE
    | DATE
    | INTERVAL
    | OPTIMIZE
    | OR
    | TABLE
    | TIMESTAMP
    | TRUE
    | WHERE
    | ZORDER
    ;

AND: 'AND';
BY: 'BY';
FALSE: 'FALSE';
DATE: 'DATE';
INTERVAL: 'INTERVAL';
OPTIMIZE: 'OPTIMIZE';
OR: 'OR';
TABLE: 'TABLE';
TIMESTAMP: 'TIMESTAMP';
TRUE: 'TRUE';
WHERE: 'WHERE';
ZORDER: 'ZORDER';

MINUS: '-';

BIGINT_LITERAL
    : DIGIT+ 'L'
    ;

SMALLINT_LITERAL
    : DIGIT+ 'S'
    ;

TINYINT_LITERAL
    : DIGIT+ 'Y'
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ EXPONENT
    | DECIMAL_DIGITS EXPONENT? {isValidDecimal()}?
    ;

DOUBLE_LITERAL
    : DIGIT+ EXPONENT? 'D'
    | DECIMAL_DIGITS EXPONENT? 'D' {isValidDecimal()}?
    ;

BIGDECIMAL_LITERAL
    : DIGIT+ EXPONENT? 'BD'
    | DECIMAL_DIGITS EXPONENT? 'BD' {isValidDecimal()}?
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

fragment DECIMAL_DIGITS
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
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
