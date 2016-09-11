package org.helios;

import io.aeron.AvailableImageHandler;
import io.aeron.Image;
import io.aeron.UnavailableImageHandler;
import org.agrona.CloseHelper;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.infra.*;
import org.helios.journal.JournalHandler;
import org.helios.journal.JournalProcessor;
import org.helios.journal.JournalWriter;
import org.helios.journal.Journalling;
import org.helios.replica.ReplicaHandler;
import org.helios.replica.ReplicaProcessor;
import org.helios.service.Service;
import org.helios.service.ServiceHandler;
import org.helios.service.ServiceHandlerFactory;
import org.helios.service.ServiceReport;
import org.helios.util.DirectBufferAllocator;
import org.helios.util.ProcessorHelper;

import java.nio.ByteBuffer;

import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

public class HeliosService<T extends ServiceHandler> implements Service<T>, AssociationHandler,
    AvailableImageHandler, UnavailableImageHandler
{
    private static final int FRAME_COUNT_LIMIT = Integer.getInteger("helios.service.poll.frame_count_limit", 10);

    private final InputMessageProcessor gwRequestProcessor;
    private final OutputMessageProcessor gwResponseProcessor;
    private final JournalProcessor journalProcessor;
    private final ReplicaProcessor replicaProcessor;
    private final RingBufferProcessor<T> serviceProcessor;
    private final RateReport report;
    private AvailableAssociationHandler availableAssociationHandler;
    private UnavailableAssociationHandler unavailableAssociationHandler;

    public HeliosService(final HeliosContext context, final AeronStream reqStream, final AeronStream rspStream,
        final ServiceHandlerFactory<T> factory)
    {
        final IdleStrategy writeIdleStrategy = context.writeIdleStrategy();
        final IdleStrategy pollIdleStrategy = context.subscriberIdleStrategy();

        final ByteBuffer outputBuffer = DirectBufferAllocator.allocateCacheAligned((16 * 1024) + TRAILER_LENGTH); // TODO: configure
        final RingBuffer outputRingBuffer = new OneToOneRingBuffer(new UnsafeBuffer(outputBuffer));

        final ByteBuffer inputBuffer = DirectBufferAllocator.allocateCacheAligned((16 * 1024) + TRAILER_LENGTH); // TODO: configure
        final RingBuffer inputRingBuffer = new OneToOneRingBuffer(new UnsafeBuffer(inputBuffer));

        gwResponseProcessor = new OutputMessageProcessor(outputRingBuffer, rspStream, writeIdleStrategy,
            "gwResponseProcessor");

        final boolean isReplicaEnabled = context.isReplicaEnabled();
        final boolean isJournalEnabled = context.isJournalEnabled();

        final RingBuffer replicaRingBuffer;
        if (isReplicaEnabled)
        {
            final IdleStrategy idleStrategy = new BusySpinIdleStrategy();

            final AeronStream replicaStream = new AeronStream(reqStream.aeron, context.replicaChannel(), context.replicaStreamId());

            final ByteBuffer replicaBuffer = DirectBufferAllocator.allocateCacheAligned((16 * 1024) + TRAILER_LENGTH); // TODO: configure
            replicaRingBuffer = new OneToOneRingBuffer(new UnsafeBuffer(replicaBuffer));

            final ReplicaHandler replicaHandler = new ReplicaHandler(replicaRingBuffer, idleStrategy, replicaStream);
            replicaProcessor = new ReplicaProcessor(inputRingBuffer, idleStrategy, replicaHandler);
        }
        else
        {
            replicaRingBuffer = null;
            replicaProcessor = null;
        }

        final RingBuffer journalRingBuffer;
        if (isJournalEnabled)
        {
            final Journalling journalling = context.journalStrategy();
            final boolean flushingEnabled = context.isJournalFlushingEnabled();

            final IdleStrategy idleStrategy = new BusySpinIdleStrategy();

            final ByteBuffer journalBuffer = DirectBufferAllocator.allocateCacheAligned((16 * 1024) + TRAILER_LENGTH); // TODO: configure
            journalRingBuffer = new OneToOneRingBuffer(new UnsafeBuffer(journalBuffer));

            final JournalWriter journalWriter = new JournalWriter(journalling, flushingEnabled);
            final JournalHandler journalHandler = new JournalHandler(journalWriter, journalRingBuffer, idleStrategy);
            journalProcessor = new JournalProcessor(isReplicaEnabled ? replicaRingBuffer : inputRingBuffer,
                idleStrategy, journalHandler);
        }
        else
        {
            journalRingBuffer = null;
            journalProcessor = null;
        }

        serviceProcessor = new RingBufferProcessor<>(
            isJournalEnabled ? journalRingBuffer : (isReplicaEnabled ? replicaRingBuffer : inputRingBuffer),
            factory.createServiceHandler(outputRingBuffer), new BusySpinIdleStrategy(), "serviceProcessor");

        gwRequestProcessor = new InputMessageProcessor(inputRingBuffer, reqStream, pollIdleStrategy, FRAME_COUNT_LIMIT,
            "gwRequestProcessor");

        report = new ServiceReport(gwRequestProcessor, gwResponseProcessor);
    }

    @Override
    public RateReport report()
    {
        return report;
    }

    @Override
    public T handler()
    {
        return serviceProcessor.handler();
    }

    @Override
    public void start()
    {
        ProcessorHelper.start(serviceProcessor);
        ProcessorHelper.start(replicaProcessor);
        ProcessorHelper.start(journalProcessor);
        ProcessorHelper.start(gwResponseProcessor);
        ProcessorHelper.start(gwRequestProcessor);
    }

    @Override
    public Service<T> availableAssociationHandler(final AvailableAssociationHandler handler)
    {
        availableAssociationHandler = handler;
        return this;
    }

    @Override
    public Service<T> unavailableAssociationHandler(final UnavailableAssociationHandler handler)
    {
        unavailableAssociationHandler = handler;
        return this;
    }

    @Override
    public void onAvailableImage(final Image image)
    {
        gwRequestProcessor.onAvailableImage(image);

        onAssociationEstablished(); // TODO: remove after HEARTBEAT handling
    }

    @Override
    public void onUnavailableImage(final Image image)
    {
        gwRequestProcessor.onUnavailableImage(image);

        onAssociationBroken(); // TODO: remove after HEARTBEAT handling
    }

    @Override
    public void onAssociationEstablished()
    {
        if (availableAssociationHandler != null)
        {
            availableAssociationHandler.onAssociationEstablished();
        }
    }

    @Override
    public void onAssociationBroken()
    {
        if (unavailableAssociationHandler != null)
        {
            unavailableAssociationHandler.onAssociationBroken();
        }
    }

    @Override
    public void close()
    {
        CloseHelper.quietClose(gwRequestProcessor);
        CloseHelper.quietClose(gwResponseProcessor);
        CloseHelper.quietClose(journalProcessor);
        CloseHelper.quietClose(replicaProcessor);
        CloseHelper.quietClose(serviceProcessor);
    }

    long inputSubscriptionId()
    {
        return gwRequestProcessor.inputSubscription().registrationId();
    }
}
