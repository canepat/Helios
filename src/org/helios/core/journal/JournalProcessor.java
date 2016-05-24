package org.helios.core.journal;

import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.infra.Processor;

import java.util.concurrent.atomic.AtomicBoolean;

public class JournalProcessor implements Processor
{
    private final RingBuffer inputRingBuffer;
    private final JournalHandler journalHandler;
    private final AtomicBoolean running;
    private final Thread journallerThread;
    private final IdleStrategy idleStrategy;

    public JournalProcessor(final RingBuffer inputRingBuffer, final IdleStrategy idleStrategy, final JournalHandler journalHandler)
    {
        this.inputRingBuffer = inputRingBuffer;
        this.idleStrategy = idleStrategy;
        this.journalHandler = journalHandler;

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
            final int bytesRead = inputRingBuffer.read(journalHandler);
            idleStrategy.idle(bytesRead);
        }
    }

    @Override
    public void close() throws Exception
    {
        running.set(false);
        journallerThread.join();
    }
}
