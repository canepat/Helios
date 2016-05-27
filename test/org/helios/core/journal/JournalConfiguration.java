/*
 * Copyright 2014 - 2015 Helios Org.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.helios.core.journal;

import java.util.concurrent.TimeUnit;

import static java.lang.Integer.getInteger;
import static java.lang.Long.getLong;

public class JournalConfiguration
{
    public static final String WARMUP_NUMBER_OF_MESSAGES_PROP = "journal.warmup.messages";
    public static final String WARMUP_NUMBER_OF_ITERATIONS_PROP = "journal.warmup.iterations";
    public static final String MESSAGE_LENGTH_PROP = "journal.messageLength";
    public static final String NUMBER_OF_MESSAGES_PROP = "journal.messages";
    public static final String NUMBER_OF_ITERATIONS_PROP = "journal.iterations";
    public static final String LINGER_TIMEOUT_MS_PROP = "journal.lingerTimeout";

    public static final int WARMUP_NUMBER_OF_MESSAGES;
    public static final int WARMUP_NUMBER_OF_ITERATIONS;
    public static final int MESSAGE_LENGTH;
    public static final int NUMBER_OF_MESSAGES;
    public static final int NUMBER_OF_ITERATIONS;
    public static final long LINGER_TIMEOUT_MS;

    static
    {
        MESSAGE_LENGTH = getInteger(MESSAGE_LENGTH_PROP, 256);
        NUMBER_OF_MESSAGES = getInteger(NUMBER_OF_MESSAGES_PROP, 100_000/*1_000_000*/);
        NUMBER_OF_ITERATIONS = getInteger(NUMBER_OF_ITERATIONS_PROP, 1/*5*/);
        WARMUP_NUMBER_OF_MESSAGES = getInteger(WARMUP_NUMBER_OF_MESSAGES_PROP, 1/*10_000*/);
        WARMUP_NUMBER_OF_ITERATIONS = getInteger(WARMUP_NUMBER_OF_ITERATIONS_PROP, 1/*5*/);
        LINGER_TIMEOUT_MS = getLong(LINGER_TIMEOUT_MS_PROP, TimeUnit.SECONDS.toMillis(5));
    }
}
