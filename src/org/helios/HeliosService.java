package org.helios;

import org.agrona.CloseHelper;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.core.journal.JournalHandler;
import org.helios.core.journal.JournalProcessor;
import org.helios.core.journal.strategy.JournalStrategy;
import org.helios.core.replica.ReplicaHandler;
import org.helios.core.replica.ReplicaProcessor;
import org.helios.core.service.*;
import org.helios.infra.RateReport;
import org.helios.util.ProcessorHelper;

import java.nio.ByteBuffer;

import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

public class HeliosService<T extends ServiceHandler> implements Service<T>
{
    private final GatewayRequestProcessor gatewayRequestProcessor;
    private final GatewayResponseProcessor gatewayResponseProcessor;
    private final JournalProcessor journalProcessor;
    private final ReplicaProcessor replicaProcessor;
    private final ServiceProcessor<T> serviceProcessor;
    private final RateReport report;

    public HeliosService(final HeliosContext context, final AeronStream reqStream, final AeronStream rspStream,
        final ServiceHandlerFactory<T> factory)
    {
        final IdleStrategy writeIdleStrategy = context.writeIdleStrategy();
        final IdleStrategy pollIdleStrategy = context.subscriberIdleStrategy();

        final ByteBuffer outputBuffer = ByteBuffer.allocateDirect((16 * 1024) + TRAILER_LENGTH); // TODO: configure
        final RingBuffer outputRingBuffer = new OneToOneRingBuffer(new UnsafeBuffer(outputBuffer));

        final ByteBuffer inputBuffer = ByteBuffer.allocateDirect((16 * 1024) + TRAILER_LENGTH); // TODO: configure
        final RingBuffer inputRingBuffer = new OneToOneRingBuffer(new UnsafeBuffer(inputBuffer));

        gatewayResponseProcessor = new GatewayResponseProcessor(outputRingBuffer, rspStream);

        final boolean isReplicaEnabled = context.isReplicaEnabled();
        final boolean isJournalEnabled = context.isJournalEnabled();

        final RingBuffer replicaRingBuffer;
        if (isReplicaEnabled)
        {
            final AeronStream replicaStream = new AeronStream(reqStream.aeron, context.replicaChannel(), context.replicaStreamId());

            final ByteBuffer replicaBuffer = ByteBuffer.allocateDirect((16 * 1024) + TRAILER_LENGTH); // TODO: configure
            replicaRingBuffer = new OneToOneRingBuffer(new UnsafeBuffer(replicaBuffer));

            final ReplicaHandler replicaHandler = new ReplicaHandler(replicaRingBuffer, new BusySpinIdleStrategy(), replicaStream);
            replicaProcessor = new ReplicaProcessor(inputRingBuffer, new BusySpinIdleStrategy(), replicaHandler);
        }
        else
        {
            replicaRingBuffer = null;
            replicaProcessor = null;
        }

        final RingBuffer journalRingBuffer;
        if (isJournalEnabled)
        {
            final JournalStrategy journalStrategy = context.journalStrategy();
            final boolean journalFlushingEnabled = context.isJournalFlushingEnabled();

            final ByteBuffer journalBuffer = ByteBuffer.allocateDirect((16 * 1024) + TRAILER_LENGTH); // TODO: configure
            journalRingBuffer = new OneToOneRingBuffer(new UnsafeBuffer(journalBuffer));

            final JournalHandler journalHandler = new JournalHandler(journalStrategy, journalFlushingEnabled,
                journalRingBuffer, new BusySpinIdleStrategy());
            journalProcessor = new JournalProcessor(isReplicaEnabled ? replicaRingBuffer : inputRingBuffer,
                new BusySpinIdleStrategy(), journalHandler);
        }
        else
        {
            journalRingBuffer = null;
            journalProcessor = null;
        }

        serviceProcessor = new ServiceProcessor<>(
            isJournalEnabled ? journalRingBuffer : (isReplicaEnabled ? replicaRingBuffer : inputRingBuffer),
            factory.createServiceHandler(outputRingBuffer));

        gatewayRequestProcessor = new GatewayRequestProcessor(inputRingBuffer, reqStream, writeIdleStrategy, pollIdleStrategy);

        report = new ServiceReport(gatewayRequestProcessor, gatewayResponseProcessor);
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
        ProcessorHelper.start(gatewayResponseProcessor);
        ProcessorHelper.start(gatewayRequestProcessor);
    }

    @Override
    public void close() throws Exception
    {
        CloseHelper.quietClose(gatewayRequestProcessor);
        CloseHelper.quietClose(gatewayResponseProcessor);
        CloseHelper.quietClose(journalProcessor);
        CloseHelper.quietClose(replicaProcessor);
        CloseHelper.quietClose(serviceProcessor);
    }
}
