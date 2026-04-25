---
title: Operators
displayTitle: Operators
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

# Operators

An SQL operator is a symbol specifying an action that is performed on one or more expressions. Operators are represented by special characters or by keywords.

#### Operator Precedence

When a complex expression has multiple operators, operator precedence determines the sequence of operations in the expression, e.g. in expression `1 + 2 * 3`, `*` has higher precedence than `+`, so the expression is evaluated as `1 + (2 * 3) = 7`. The order of execution can significantly affect the resulting value.

Operators have the precedence levels shown in the following table. An operator on higher precedence is evaluated before an operator on a lower level. In the following table, the operators in descending order of precedence, a.k.a. 1 is the highest level. Operators listed on the same table cell have the same precedence and are evaluated from left to right or right to left based on the associativity.

| Precedence | Operator                                                                                             | Operation                                                                        | Associativity |
| ---------- | ---------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- | ------------- |
| 1          | <p>.<br>[]<br>::</p>                                                                                 | <p>member access<br>element access<br>cast</p>                                   | Left to right |
| 2          | <p>+<br>-<br>~</p>                                                                                   | <p>unary plus<br>unary minus<br>bitwise NOT</p>                                  | Right to left |
| 3          | <p>*<br>/<br>%<br>DIV</p>                                                                            | <p>multiplication<br>division, modulo<br>integral division</p>                   | Left to right |
| 4          | <p>+<br>-<br>||</p>                                                                                  | <p>addition<br>subtraction<br>concatenation</p>                                  | Left to right |
| 5          | <p>&#x3C;&#x3C;<br>>><br>>>></p>                                                                     | <p>bitwise shift left<br>bitwise shift right<br>bitwise shift right unsigned</p> | Left to right |
| 6          | &                                                                                                    | bitwise AND                                                                      | Left to right |
| 7          | ^                                                                                                    | bitwise XOR(exclusive or)                                                        | Left to right |
| 8          | \|                                                                                                   | bitwise OR(inclusive or)                                                         | Left to right |
| 9          | <p>=, ==<br>&#x3C;>, !=<br>&#x3C;, &#x3C;=<br>>, >=</p>                                              | comparison operators                                                             | Left to right |
| 10         | <p>NOT, !<br>EXISTS</p>                                                                              | <p>logical NOT<br>existence</p>                                                  | Right to left |
| 11         | <p>BETWEEN<br>IN<br>RLIKE, REGEXP<br>ILIKE<br>LIKE<br>IS [NULL, TRUE, FALSE]<br>IS DISTINCT FROM</p> | other predicates                                                                 | Left to right |
| 12         | AND                                                                                                  | conjunction                                                                      | Left to right |
| 13         | OR                                                                                                   | disjunction                                                                      | Left to right |
