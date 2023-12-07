/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.engine.chat.ernie.constants;

public enum  Model {
    ERNIE_BOT_4("completions_pro"),

    ERNIE_BOT_8k("ernie_bot_8k"),

    ERNIE_BOT("completions"),

    ERNIE_BOT_TURBO("eb-instant");

    private final String value;

    private Model(String value) {
        this.value = value;
    }

    public String value() {
        return this.value;
    }
}
