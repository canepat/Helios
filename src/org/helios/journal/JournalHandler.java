package org.helios.journal;

import org.agrona.CloseHelper;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.mmb.SnapshotMessage;

import java.util.Objects;

public class JournalHandler implements MessageHandler, JournalDepletionHandler, AutoCloseable
{
    private final JournalWriter writer;
    private final RingBuffer nextRingBuffer;
    private final IdleStrategy idleStrategy;
    private final SnapshotMessage snapshotMessage;

    public JournalHandler(final JournalWriter writer, final RingBuffer nextRingBuffer, final IdleStrategy idleStrategy)
    {
        this.writer = writer.depletionHandler(this);
        this.nextRingBuffer = Objects.requireNonNull(nextRingBuffer);
        this.idleStrategy = Objects.requireNonNull(idleStrategy);

        snapshotMessage = new SnapshotMessage();
    }

    @Override
    public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
    {
        try
        {
            writer.onMessage(msgTypeId, buffer, index, length);
        }
        finally
        {
            while (!nextRingBuffer.write(msgTypeId, buffer, index, length))
            {
                idleStrategy.idle(0);
            }
        }
    }

    @Override
    public void onJournalDepletion(Journalling journalling)
    {
        snapshotMessage.writeSaveMessage(nextRingBuffer, idleStrategy);
    }

    @Override
    public void close() throws Exception
    {
        CloseHelper.quietClose(writer);
    }
}
