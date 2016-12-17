package org.helios;

import io.aeron.AvailableImageHandler;
import io.aeron.Image;
import io.aeron.UnavailableImageHandler;
import org.agrona.CloseHelper;
import org.agrona.TimerWheel;
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
import org.helios.mmb.sbe.ComponentType;
import org.helios.replica.ReplicaHandler;
import org.helios.replica.ReplicaProcessor;
import org.helios.service.Service;
import org.helios.service.ServiceHandler;
import org.helios.service.ServiceHandlerFactory;
import org.helios.service.ServiceReport;
import org.helios.snapshot.SnapshotTimer;
import org.helios.util.DirectBufferAllocator;
import org.helios.util.ProcessorHelper;
import org.helios.util.RingBufferPool;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

public class HeliosService<T extends ServiceHandler> implements Service<T>, AssociationHandler,
    AvailableImageHandler, UnavailableImageHandler
{
    private static final int FRAME_COUNT_LIMIT = Integer.getInteger("helios.service.poll.frame_count_limit", 10);

    private final long TICK_DURATION_NS = TimeUnit.MICROSECONDS.toNanos(100);  // TODO: configure
    private final int TICKS_PER_WHEEL = 512; // TODO: configure
    private static short nextServiceId = 0;

    private final short serviceId;
    private final Helios helios;
    private final InputMessageProcessor svcInputProcessor;
    private final RingBufferPool ringBufferPool;
    private final List<OutputMessageProcessor> gwResponseProcessorList;
    private final JournalProcessor journalProcessor;
    private final ReplicaProcessor replicaProcessor;
    private final RingBufferProcessor<T> serviceProcessor;
    private final List<OutputMessageProcessor> eventProcessorList;
    private final ServiceReport report;
    private final TimerWheel timerWheel;
    private final SnapshotTimer snapshotTimer;
    private final AtomicBoolean timerWheelRunning;
    private final ExecutorService timerExecutor;
    private AvailableAssociationHandler availableAssociationHandler;
    private UnavailableAssociationHandler unavailableAssociationHandler;

    public HeliosService(final Helios helios, final ServiceHandlerFactory<T> factory, final AeronStream reqStream)
    {
        this.helios = helios;

        serviceId = ++nextServiceId;
        ringBufferPool = new RingBufferPool();
        gwResponseProcessorList = new ArrayList<>();
        eventProcessorList = new ArrayList<>();

        final HeliosContext context = helios.context();

        final ByteBuffer inputBuffer = DirectBufferAllocator.allocateCacheAligned((16 * 1024) + TRAILER_LENGTH); // TODO: configure
        final RingBuffer inputRingBuffer = new OneToOneRingBuffer(new UnsafeBuffer(inputBuffer));

        timerWheel = new TimerWheel(TICK_DURATION_NS, TimeUnit.MILLISECONDS, TICKS_PER_WHEEL);
        snapshotTimer = new SnapshotTimer(timerWheel, inputRingBuffer);
        timerWheelRunning = new AtomicBoolean(true);
        timerExecutor = Executors.newSingleThreadExecutor();

        final boolean isReplicaEnabled = context.isReplicaEnabled();
        final boolean isJournalEnabled = context.isJournalEnabled();

        final RingBuffer replicaRingBuffer;
        if (isReplicaEnabled)
        {
            final ByteBuffer replicaBuffer = DirectBufferAllocator.allocateCacheAligned((16 * 1024) + TRAILER_LENGTH); // TODO: configure
            replicaRingBuffer = new OneToOneRingBuffer(new UnsafeBuffer(replicaBuffer));

            final IdleStrategy idleStrategy = new BusySpinIdleStrategy();

            final AeronStream replicaStream = new AeronStream(helios.aeron(), context.replicaChannel(), context.replicaStreamId());

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

        final T serviceHandler = factory.createServiceHandler(ringBufferPool);
        serviceProcessor = new RingBufferProcessor<>(
            isJournalEnabled ? journalRingBuffer : (isReplicaEnabled ? replicaRingBuffer : inputRingBuffer),
            serviceHandler, new BusySpinIdleStrategy(), "svcProcessor");

        final IdleStrategy pollIdleStrategy = context.subscriberIdleStrategy();
        final int heartbeatLiveness = context.heartbeatLiveness();
        svcInputProcessor = new InputMessageProcessor(inputRingBuffer, pollIdleStrategy, FRAME_COUNT_LIMIT,
            heartbeatLiveness, reqStream, this, "svcInputProcessor");

        final long subscriptionId = svcInputProcessor.subscriptionId();
        helios.addServiceSubscription(subscriptionId, this);

        report = new ServiceReport(svcInputProcessor);
    }

    public Service<T> addEndPoint(final AeronStream rspStream)
    {
        Objects.requireNonNull(rspStream, "rspStream");

        rspStream.componentType = ComponentType.Service;
        rspStream.componentId = serviceId;

        final IdleStrategy writeIdleStrategy = helios.context().writeIdleStrategy();

        final ByteBuffer outputBuffer = DirectBufferAllocator.allocateCacheAligned((16 * 1024) + TRAILER_LENGTH); // TODO: configure
        final RingBuffer outputRingBuffer = new OneToOneRingBuffer(new UnsafeBuffer(outputBuffer));

        ringBufferPool.addOutputRingBuffer(rspStream, outputRingBuffer);

        final int heartbeatInterval = helios.context().heartbeatInterval();
        final OutputMessageProcessor svcOutputProcessor =
            new OutputMessageProcessor(outputRingBuffer, rspStream, writeIdleStrategy, heartbeatInterval, "svcOutputProcessor"); // FIXME: gwResponseProcessor name

        gwResponseProcessorList.add(svcOutputProcessor);

        report.addResponseProcessor(svcOutputProcessor);

        return this;
    }

    @Override
    public Service<T> addEventChannel(final AeronStream eventStream)
    {
        Objects.requireNonNull(eventStream, "eventStream");

        eventStream.componentType = ComponentType.Service;
        eventStream.componentId = serviceId;

        final IdleStrategy writeIdleStrategy = helios.context().writeIdleStrategy();

        final ByteBuffer eventBuffer = DirectBufferAllocator.allocateCacheAligned((16 * 1024) + TRAILER_LENGTH); // TODO: configure
        final RingBuffer eventRingBuffer = new OneToOneRingBuffer(new UnsafeBuffer(eventBuffer));

        ringBufferPool.addEventRingBuffer(eventStream, eventRingBuffer);

        final int heartbeatInterval = helios.context().heartbeatInterval();
        final OutputMessageProcessor eventProcessor =
            new OutputMessageProcessor(eventRingBuffer, eventStream, writeIdleStrategy, heartbeatInterval, "eventProcessor"); // FIXME: eventProcessor name

        eventProcessorList.add(eventProcessor);

        return this;
    }

    @Override
    public Report report()
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
        eventProcessorList.forEach(ProcessorHelper::start);
        gwResponseProcessorList.forEach(ProcessorHelper::start);
        ProcessorHelper.start(svcInputProcessor);

        timerExecutor.execute(() -> {
            while (timerWheelRunning.get()) {
                timerWheel.expireTimers();
            }
        });
        snapshotTimer.start();
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
        svcInputProcessor.onAvailableImage(image);
    }

    @Override
    public void onUnavailableImage(final Image image)
    {
        svcInputProcessor.onUnavailableImage(image);
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
        snapshotTimer.close();
        timerWheelRunning.set(false);
        timerExecutor.shutdown();

        CloseHelper.quietClose(svcInputProcessor);
        gwResponseProcessorList.forEach(CloseHelper::quietClose);
        eventProcessorList.forEach(CloseHelper::quietClose);
        CloseHelper.quietClose(journalProcessor);
        CloseHelper.quietClose(replicaProcessor);
        CloseHelper.quietClose(serviceProcessor);
    }
}
