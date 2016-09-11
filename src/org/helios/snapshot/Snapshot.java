package org.helios.snapshot;

import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.infra.MessageTypes;

import java.util.Objects;

import static org.helios.mmb.SnapshotMessageFactory.*;

public final class Snapshot
{
    public static void writeLoadMessage(final RingBuffer inputRingBuffer, final IdleStrategy idleStrategy)
    {
        Objects.requireNonNull(idleStrategy, "idleStrategy");

        while (!inputRingBuffer.write(MessageTypes.ADMINISTRATIVE_MSG_ID, loadSnapshotBuffer, 0, MESSAGE_LENGTH))
        {
            idleStrategy.idle(0);
        }
    }

    public static void writeSaveMessage(final RingBuffer inputRingBuffer, final IdleStrategy idleStrategy)
    {
        Objects.requireNonNull(idleStrategy, "idleStrategy");

        while (!inputRingBuffer.write(MessageTypes.ADMINISTRATIVE_MSG_ID, saveSnapshotBuffer, 0, MESSAGE_LENGTH))
        {
            idleStrategy.idle(0);
        }
    }
}
