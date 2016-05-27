package org.helios.core.journal;

import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.ringbuffer.RingBuffer;

public class JournalPlayer implements Runnable, MessageHandler, AutoCloseable
{
    private final RingBuffer inputRingBuffer;
    private final JournalReader journalReader;
    private final IdleStrategy idleStrategy;
    private int messagesReplayed;

    public JournalPlayer(final RingBuffer inputRingBuffer, final Journalling journalling, final int pageSize)
    {
        this.inputRingBuffer = inputRingBuffer;

        journalReader = new JournalReader(journalling, pageSize);
        idleStrategy = new BusySpinIdleStrategy();
    }

    @Override
    public void run()
    {
        try
        {
            messagesReplayed = journalReader.readFully(this);
        }
        catch (Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    @Override
    public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
    {
        while (!inputRingBuffer.write(msgTypeId, buffer, index, length))
        {
            idleStrategy.idle(0);
        }
    }

    public int messagesReplayed()
    {
        return messagesReplayed;
    }

    @Override
    public void close() throws Exception
    {
        CloseHelper.quietClose(journalReader);
    }
}
