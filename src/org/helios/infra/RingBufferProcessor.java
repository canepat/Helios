package org.helios.infra;

import org.agrona.CloseHelper;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.ringbuffer.RingBuffer;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.agrona.UnsafeAccess.UNSAFE;

public abstract class RingBufferProcessor<T extends MessageHandler & AutoCloseable> implements Processor, MessageHandler
{
    private static final long SUCCESSFUL_READS_OFFSET;
    private static final long FAILED_READS_OFFSET;

    private volatile long successfulReads = 0;
    private volatile long failedReads = 0;

    private final RingBuffer ringBuffer;
    private final IdleStrategy idleStrategy;
    private final T handler;
    private final AtomicBoolean running;
    private final Thread processorThread;

    public RingBufferProcessor(final RingBuffer ringBuffer, final T handler, final IdleStrategy idleStrategy, final String threadName)
    {
        this.ringBuffer = ringBuffer;
        this.handler = handler;
        this.idleStrategy = idleStrategy;

        running = new AtomicBoolean(false);
        processorThread = new Thread(this, threadName);
    }

    @Override
    public void start()
    {
        running.set(true);
        processorThread.start();
    }

    @Override
    public void run()
    {
        while (running.get())
        {
            final int readCount = ringBuffer.read(this);
            if (0 == readCount)
            {
                UNSAFE.putOrderedLong(this, FAILED_READS_OFFSET, failedReads + 1);
                idleStrategy.idle(0);
            }
            else
            {
                UNSAFE.putOrderedLong(this, SUCCESSFUL_READS_OFFSET, successfulReads + 1);
            }
        }

        final double failureRatio = failedReads / (double)(successfulReads + failedReads);
        System.out.format(processorThread.getName() + " read failure ratio: %f\n", failureRatio);
    }

    @Override
    public void onMessage(int msgTypeId, final MutableDirectBuffer buffer, int index, int length)
    {
        handler.onMessage(msgTypeId, buffer, index, length);
    }

    @Override
    public void close() throws Exception
    {
        running.set(false);
        processorThread.join();

        CloseHelper.close(handler);
    }

    public T handler()
    {
        return handler;
    }

    public long successfulReads()
    {
        return successfulReads;
    }

    public long failedReads()
    {
        return failedReads;
    }

    static
    {
        try
        {
            SUCCESSFUL_READS_OFFSET = UNSAFE.objectFieldOffset(RingBufferProcessor.class.getDeclaredField("successfulReads"));
            FAILED_READS_OFFSET = UNSAFE.objectFieldOffset(RingBufferProcessor.class.getDeclaredField("failedReads"));
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
