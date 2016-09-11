package org.helios.infra;

import io.aeron.*;
import org.agrona.CloseHelper;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.AeronStream;
import org.helios.snapshot.Snapshot;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.agrona.UnsafeAccess.UNSAFE;

public class InputMessageProcessor implements Processor, AvailableImageHandler, UnavailableImageHandler
{
    private static final long SUCCESSFUL_READS_OFFSET;
    private static final long FAILED_READS_OFFSET;

    private volatile long successfulReads = 0;
    private volatile long failedReads = 0;

    private final RingBuffer inputRingBuffer;
    private final InputMessageHandler handler;
    private final Subscription inputSubscription;
    private final int frameCountLimit;
    private final IdleStrategy idleStrategy;
    private final AtomicBoolean running;
    private final Thread processorThread;

    public InputMessageProcessor(final RingBuffer inputRingBuffer, final AeronStream stream, final IdleStrategy idleStrategy,
        final int frameCountLimit, final String threadName)
    {
        this(new InputMessageHandler(inputRingBuffer, idleStrategy), inputRingBuffer, stream, idleStrategy, frameCountLimit, threadName);
    }

    public InputMessageProcessor(final InputMessageHandler handler, final RingBuffer inputRingBuffer, final AeronStream stream,
        final IdleStrategy idleStrategy, final int frameCountLimit, final String threadName)
    {
        this.handler = handler;
        this.inputRingBuffer = inputRingBuffer;
        this.idleStrategy = idleStrategy;
        this.frameCountLimit = frameCountLimit;

        inputSubscription = stream.aeron.addSubscription(stream.channel, stream.streamId);

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
        // First of all, write the Save Data Snapshot message into the input pipeline.
        Snapshot.writeLoadMessage(inputRingBuffer, idleStrategy);

        // Poll the input subscription for incoming data until running.
        final FragmentAssembler dataHandler = new FragmentAssembler(handler);

        while (running.get())
        {
            final int fragmentsRead = inputSubscription.poll(dataHandler, frameCountLimit);
            if (0 == fragmentsRead)
            {
                UNSAFE.putOrderedLong(this, FAILED_READS_OFFSET, failedReads + 1);
                idleStrategy.idle(0);
            }
            else
            {
                UNSAFE.putOrderedLong(this, SUCCESSFUL_READS_OFFSET, successfulReads + 1);
                idleStrategy.idle(fragmentsRead);
            }
        }
    }

    @Override
    public void onAvailableImage(final Image image)
    {
        // TODO: when at least one image is present resume subscription polling
    }

    @Override
    public void onUnavailableImage(final Image image)
    {
        // TODO: when no more images are present suspend subscription polling
    }

    @Override
    public void close() throws Exception
    {
        running.set(false);
        processorThread.join();

        CloseHelper.quietClose(inputSubscription);
        CloseHelper.quietClose(handler);
    }

    public Subscription inputSubscription()
    {
        return inputSubscription;
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
            SUCCESSFUL_READS_OFFSET = UNSAFE.objectFieldOffset(InputMessageProcessor.class.getDeclaredField("successfulReads"));
            FAILED_READS_OFFSET = UNSAFE.objectFieldOffset(InputMessageProcessor.class.getDeclaredField("failedReads"));
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
