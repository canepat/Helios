package org.helios.infra;

import io.aeron.*;
import org.agrona.CloseHelper;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.AeronStream;
import org.helios.snapshot.Snapshot;

import java.util.LinkedHashSet;
import java.util.Set;
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
    private final Set<Subscription> inputSubscriptionList;
    private final int frameCountLimit;
    private final IdleStrategy idleStrategy;
    private final AtomicBoolean running;
    private final Thread processorThread;

    public InputMessageProcessor(final RingBuffer inputRingBuffer, final IdleStrategy idleStrategy, final int frameCountLimit, final String threadName)
    {
        this.inputRingBuffer = inputRingBuffer;
        this.idleStrategy = idleStrategy;
        this.frameCountLimit = frameCountLimit;

        handler = new InputMessageHandler(inputRingBuffer, idleStrategy);
        inputSubscriptionList = new LinkedHashSet<>();

        running = new AtomicBoolean(false);
        processorThread = new Thread(this, threadName);
    }

    public long addSubscription(final AeronStream stream)
    {
        final Subscription inputSubscription = stream.aeron.addSubscription(stream.channel, stream.streamId);
        inputSubscriptionList.add(inputSubscription);

        return inputSubscription.registrationId();
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
        // First of all, write the Load Data Snapshot message into the input pipeline.
        Snapshot.writeLoadMessage(inputRingBuffer, idleStrategy); // TODO: is this the right place?

        // Poll ALL the input subscriptions for incoming data until running.
        final FragmentAssembler dataHandler = new FragmentAssembler(handler);

        int idleCount = 0;
        while (running.get())
        {
            int fragmentsRead = 0;
            for (final Subscription inputSubscription: inputSubscriptionList)
            {
                fragmentsRead += inputSubscription.poll(dataHandler, frameCountLimit);
            }

            if (0 == fragmentsRead)
            {
                UNSAFE.putOrderedLong(this, FAILED_READS_OFFSET, failedReads + 1);
                idleStrategy.idle(idleCount++);
            }
            else
            {
                UNSAFE.putOrderedLong(this, SUCCESSFUL_READS_OFFSET, successfulReads + 1);
                idleCount = 0;
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

        inputSubscriptionList.forEach(CloseHelper::quietClose);
        CloseHelper.quietClose(handler);
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
