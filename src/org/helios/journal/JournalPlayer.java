package org.helios.journal;

import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.ringbuffer.RingBuffer;

public class JournalPlayer implements Runnable, MessageHandler, AutoCloseable
{
    private final RingBuffer inputRingBuffer;
    private final JournalReader journalReader;
    private final IdleStrategy idleStrategy;
    private int messagesReplayed;

    public JournalPlayer(final JournalReader journalReader, final RingBuffer inputRingBuffer, final IdleStrategy idleStrategy)
    {
        this.journalReader = journalReader;
        this.inputRingBuffer = inputRingBuffer;
        this.idleStrategy = idleStrategy;
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
