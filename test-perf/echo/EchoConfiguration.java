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
package echo;

import java.util.concurrent.TimeUnit;

import static java.lang.Integer.getInteger;
import static java.lang.Long.getLong;
import static java.lang.System.getProperty;

public class EchoConfiguration
{
    public static final String SERVICE_INPUT_RING_SIZE_PROP = "echo.service.inputRingSize";
    public static final String SERVICE_OUTPUT_RING_SIZE_PROP = "echo.service.outputRingSize";

    public static final String SERVICE_INPUT_CHANNEL_PROP = "echo.service.inputChannel";
    public static final String SERVICE_INPUT_STREAM_ID_PROP = "echo.service.inputStreamId";
    public static final String SERVICE_OUTPUT_CHANNEL_PROP = "echo.service.outputChannel";
    public static final String SERVICE_OUTPUT_STREAM_ID_PROP = "echo.service.outputStreamId";

    public static final String WARMUP_NUMBER_OF_MESSAGES_PROP = "echo.warmup.messages";
    public static final String WARMUP_NUMBER_OF_ITERATIONS_PROP = "echo.warmup.iterations";
    public static final String MESSAGE_LENGTH_PROP = "echo.messageLength";
    public static final String NUMBER_OF_MESSAGES_PROP = "echo.messages";
    public static final String NUMBER_OF_ITERATIONS_PROP = "echo.iterations";
    public static final String LINGER_TIMEOUT_MS_PROP = "echo.lingerTimeout";

    public static final int SERVICE_INPUT_RING_SIZE;
    public static final int SERVICE_OUTPUT_RING_SIZE;

    public static final String SERVICE_INPUT_CHANNEL;
    public static final int SERVICE_INPUT_STREAM_ID;
    public static final String SERVICE_OUTPUT_CHANNEL;
    public static final int SERVICE_OUTPUT_STREAM_ID;

    public static final int WARMUP_NUMBER_OF_MESSAGES;
    public static final int WARMUP_NUMBER_OF_ITERATIONS;
    public static final int MESSAGE_LENGTH;
    public static final int NUMBER_OF_MESSAGES;
    public static final int NUMBER_OF_ITERATIONS;
    public static final long LINGER_TIMEOUT_MS;

    static
    {
        SERVICE_INPUT_RING_SIZE = getInteger(SERVICE_INPUT_RING_SIZE_PROP, 512 * 1024);
        SERVICE_OUTPUT_RING_SIZE = getInteger(SERVICE_OUTPUT_RING_SIZE_PROP, 512 * 1024);

        SERVICE_INPUT_CHANNEL = getProperty(SERVICE_INPUT_CHANNEL_PROP, "udp://localhost:40123");
        SERVICE_INPUT_STREAM_ID = getInteger(SERVICE_INPUT_STREAM_ID_PROP, 10);
        SERVICE_OUTPUT_CHANNEL = getProperty(SERVICE_OUTPUT_CHANNEL_PROP, "udp://localhost:40124");
        SERVICE_OUTPUT_STREAM_ID = getInteger(SERVICE_OUTPUT_STREAM_ID_PROP, 11);

        MESSAGE_LENGTH = getInteger(MESSAGE_LENGTH_PROP, 256);
        NUMBER_OF_MESSAGES = getInteger(NUMBER_OF_MESSAGES_PROP, 1_000_000/*1_000_000*/);
        NUMBER_OF_ITERATIONS = getInteger(NUMBER_OF_ITERATIONS_PROP, 10/*5*/);
        WARMUP_NUMBER_OF_MESSAGES = getInteger(WARMUP_NUMBER_OF_MESSAGES_PROP, 10_000/*10_000*/);
        WARMUP_NUMBER_OF_ITERATIONS = getInteger(WARMUP_NUMBER_OF_ITERATIONS_PROP, 1/*5*/);
        LINGER_TIMEOUT_MS = getLong(LINGER_TIMEOUT_MS_PROP, TimeUnit.SECONDS.toMillis(5));
    }
}
