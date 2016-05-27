package org.helios.core.journal;

import org.agrona.CloseHelper;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.ringbuffer.RingBuffer;

public class JournalHandler implements MessageHandler, AutoCloseable
{
    private final JournalWriter writer;
    private final RingBuffer nextRingBuffer;
    private final IdleStrategy idleStrategy;

    public JournalHandler(final JournalWriter writer, final RingBuffer nextRingBuffer, final IdleStrategy idleStrategy)
    {
        this.writer = writer;
        this.nextRingBuffer = nextRingBuffer;
        this.idleStrategy = idleStrategy;
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
    public void close() throws Exception
    {
        CloseHelper.quietClose(writer);
    }
}
