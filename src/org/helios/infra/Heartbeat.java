package org.helios.infra;

import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ringbuffer.RingBuffer;

import java.util.Objects;

import static org.helios.mmb.HeartbeatMessageFactory.MESSAGE_LENGTH;
import static org.helios.mmb.HeartbeatMessageFactory.heartbeatBuffer;

public final class Heartbeat
{
    public static void writeMessage(final RingBuffer outputRingBuffer, final IdleStrategy idleStrategy)
    {
        Objects.requireNonNull(idleStrategy, "idleStrategy");

        while (!outputRingBuffer.write(MessageTypes.ADMINISTRATIVE_MSG_ID, heartbeatBuffer, 0, MESSAGE_LENGTH))
        {
            idleStrategy.idle(0);
        }
    }
}
