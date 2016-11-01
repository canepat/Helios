package org.helios.infra;

import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ringbuffer.RingBuffer;

import java.util.Objects;

import static org.helios.mmb.HeartbeatMessageFactory.heartbeatBuffer;

public final class Heartbeat
{
    private static final int MESSAGE_LENGTH = 128;

    public static void writeMessage(final RingBuffer inputRingBuffer, final IdleStrategy idleStrategy)
    {
        Objects.requireNonNull(idleStrategy, "idleStrategy");

        while (!inputRingBuffer.write(MessageTypes.ADMINISTRATIVE_MSG_ID, heartbeatBuffer, 0, MESSAGE_LENGTH))
        {
            idleStrategy.idle(0);
        }
    }
}
