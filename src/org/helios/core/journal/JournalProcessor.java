package org.helios.core.journal;

import org.agrona.CloseHelper;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.infra.Processor;

import java.util.concurrent.atomic.AtomicBoolean;

public class JournalProcessor implements Processor
{
    private final RingBuffer inputRingBuffer;
    private final JournalWriter journalWriter;
    private final AtomicBoolean running;
    private final Thread journallerThread;
    private final IdleStrategy idleStrategy;

    public JournalProcessor(final RingBuffer inputRingBuffer, final IdleStrategy idleStrategy, final JournalWriter journalWriter)
    {
        this.inputRingBuffer = inputRingBuffer;
        this.idleStrategy = idleStrategy;
        this.journalWriter = journalWriter;

        running = new AtomicBoolean(false);
        journallerThread = new Thread(this, "journalProcessor");
    }

    @Override
    public void start()
    {
        running.set(true);
        journallerThread.start();
    }

    @Override
    public void run()
    {
        while (running.get())
        {
            final int bytesRead = inputRingBuffer.read(journalWriter);
            idleStrategy.idle(bytesRead);
        }
    }

    @Override
    public void close() throws Exception
    {
        running.set(false);
        journallerThread.join();

        CloseHelper.quietClose(journalWriter);
    }
}
