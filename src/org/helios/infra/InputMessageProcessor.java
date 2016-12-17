package org.helios.infra;

import io.aeron.*;
import org.agrona.CloseHelper;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.AeronStream;
import org.helios.mmb.SnapshotMessage;

import static org.agrona.UnsafeAccess.UNSAFE;

public class InputMessageProcessor implements Processor, AvailableImageHandler, UnavailableImageHandler, InputReport
{
    private static final long SUCCESSFUL_READS_OFFSET;
    private static final long FAILED_READS_OFFSET;

    private volatile long successfulReads = 0;
    private volatile long failedReads = 0;

    private final RingBuffer inputRingBuffer;
    private final Subscription inputSubscription;
    private final InputMessageHandler inputMessageHandler;
    private final FragmentAssembler dataHandler;
    private final SnapshotMessage snapshotMessage;
    private final int frameCountLimit;
    private final IdleStrategy idleStrategy;
    private final long maxHeartbeatLiveness;
    private long heartbeatLiveness;
    private volatile boolean running;
    private volatile boolean polling;
    private final Thread processorThread;

    public InputMessageProcessor(final RingBuffer inputRingBuffer, final IdleStrategy idleStrategy,
        final int frameCountLimit, final long maxHeartbeatLiveness, final AeronStream stream,
        final AssociationHandler associationHandler, final String threadName)
    {
        this.inputRingBuffer = inputRingBuffer;
        this.idleStrategy = idleStrategy;
        this.frameCountLimit = frameCountLimit;
        this.maxHeartbeatLiveness = maxHeartbeatLiveness;

        inputSubscription = stream.aeron.addSubscription(stream.channel, stream.streamId);
        inputMessageHandler = new InputMessageHandler(inputRingBuffer, idleStrategy, associationHandler);
        dataHandler = new FragmentAssembler(inputMessageHandler);
        snapshotMessage = new SnapshotMessage();

        heartbeatLiveness = maxHeartbeatLiveness;

        running = false;
        polling = false;
        processorThread = new Thread(this, threadName);
    }

    @Override
    public String name()
    {
        return processorThread.getName();
    }

    @Override
    public void start()
    {
        running = true;
        processorThread.start();
    }

    @Override
    public void run()
    {
        // First of all, write the Load Data SnapshotMessage message into the input pipeline.
        snapshotMessage.writeLoadMessage(inputRingBuffer, idleStrategy); // TODO: is this the right place?

        // Poll the input subscription for incoming data until running.
        int idleCount = 0;
        while (running)
        {
            if (polling)
            {
                final int fragmentsRead = inputSubscription.poll(dataHandler, frameCountLimit);
                if (0 == fragmentsRead)
                {
                    // No incoming data from poll
                    heartbeatLiveness--;
                    if (heartbeatLiveness == 0)
                    {
                        // TODO: notify heartbeat lost

                        heartbeatLiveness = maxHeartbeatLiveness;
                    }

                    // Update statistics
                    UNSAFE.putOrderedLong(this, FAILED_READS_OFFSET, failedReads + 1);
                    idleStrategy.idle(idleCount++);
                }
                else
                {
                    // Incoming data arrived from poll (it DOES NOT matter if heartbeat or not)
                    heartbeatLiveness = maxHeartbeatLiveness;

                    // Update statistics
                    UNSAFE.putOrderedLong(this, SUCCESSFUL_READS_OFFSET, successfulReads + 1);
                    idleCount = 0;
                }
            }
            else
            {
                idleStrategy.idle(idleCount++);
            }
        }
    }

    @Override
    public void onAvailableImage(final Image image)
    {
        // When at least one image is present resume subscription polling
        if (inputSubscription.imageCount() > 0)
        {
            polling = true;
        }
    }

    @Override
    public void onUnavailableImage(final Image image)
    {
        dataHandler.freeSessionBuffer(image.sessionId());

        // When no more images are present suspend subscription polling
        if (inputSubscription.imageCount() == 0)
        {
            polling = false;
        }
    }

    @Override
    public void close() throws Exception
    {
        running = false;
        processorThread.join();

        CloseHelper.quietClose(inputSubscription);
    }

    public long subscriptionId()
    {
        return inputSubscription.registrationId();
    }

    @Override
    public long successfulReads()
    {
        return successfulReads;
    }

    @Override
    public long failedReads()
    {
        return failedReads;
    }

    @Override
    public long heartbeatReceived()
    {
        return inputMessageHandler.heartbeatReceived();
    }

    @Override
    public long administrativeMessages()
    {
        return inputMessageHandler.administrativeMessages();
    }

    @Override
    public long applicationMessages()
    {
        return inputMessageHandler.applicationMessages();
    }

    @Override
    public long bytesRead()
    {
        return inputMessageHandler.bytesRead();
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
