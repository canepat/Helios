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
import org.helios.gateway.Gateway;
import org.helios.gateway.GatewayHandler;
import org.helios.gateway.GatewayHandlerFactory;
import org.helios.gateway.GatewayReport;
import org.helios.infra.*;
import org.helios.mmb.sbe.ComponentType;
import org.helios.util.DirectBufferAllocator;
import org.helios.util.ProcessorHelper;
import org.helios.util.RingBufferPool;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

class HeliosGateway<T extends GatewayHandler> implements Gateway<T>, AssociationHandler,
    AvailableImageHandler, UnavailableImageHandler
{
    private static final int FRAME_COUNT_LIMIT = Integer.getInteger("helios.gateway.poll.frame_count_limit", 10); // TODO: from HeliosContext
    private static short nextGatewayId = 0;

    private final short gatewayId;
    private final Helios helios;
    private final RingBufferPool ringBufferPool;
    private final List<OutputMessageProcessor> gwOutputProcessorList;
    private final List<InputMessageProcessor> gwInputProcessorList;
    private final List<RingBufferProcessor> gatewayProcessorList;
    private final List<InputMessageProcessor> eventProcessorList;
    private final GatewayReport report;
    private AvailableAssociationHandler availableAssociationHandler;
    private UnavailableAssociationHandler unavailableAssociationHandler;

    HeliosGateway(final Helios helios)
    {
        this.helios = helios;

        gatewayId = ++nextGatewayId;
        ringBufferPool = new RingBufferPool();
        gwOutputProcessorList = new ArrayList<>();
        gwInputProcessorList = new ArrayList<>();
        gatewayProcessorList = new ArrayList<>();
        eventProcessorList = new ArrayList<>();

        report = new GatewayReport();
    }

    @Override
    public T addEndPoint(final AeronStream reqStream, final AeronStream rspStream, final GatewayHandlerFactory<T> factory)
    {
        Objects.requireNonNull(reqStream, "reqStream");
        Objects.requireNonNull(rspStream, "rspStream");
        Objects.requireNonNull(factory, "factory");

        reqStream.componentType = ComponentType.Gateway;
        reqStream.componentId = gatewayId;

        final IdleStrategy idleStrategy = new BusySpinIdleStrategy();

        final ByteBuffer outputBuffer = DirectBufferAllocator.allocateCacheAligned((16 * 1024) + TRAILER_LENGTH); // TODO: configure
        final RingBuffer outputRingBuffer = new OneToOneRingBuffer(new UnsafeBuffer(outputBuffer));

        ringBufferPool.addOutputRingBuffer(reqStream, outputRingBuffer);

        final int heartbeatInterval = helios.context().heartbeatInterval();
        final OutputMessageProcessor gwOutputProcessor =
            new OutputMessageProcessor(outputRingBuffer, reqStream, idleStrategy, heartbeatInterval, "gwOutputProcessor");

        gwOutputProcessorList.add(gwOutputProcessor);

        final ByteBuffer inputBuffer = DirectBufferAllocator.allocateCacheAligned((16 * 1024) + TRAILER_LENGTH); // TODO: configure
        final RingBuffer inputRingBuffer = new OneToOneRingBuffer(new UnsafeBuffer(inputBuffer));

        final T gatewayHandler = factory.createGatewayHandler(ringBufferPool);
        final RingBufferProcessor gatewayProcessor = new RingBufferProcessor<>(inputRingBuffer, gatewayHandler, new BusySpinIdleStrategy(), "gwProcessor");
        gatewayProcessorList.add(gatewayProcessor);

        final IdleStrategy pollIdleStrategy = helios.context().subscriberIdleStrategy();
        final int heartbeatLiveness = helios.context().heartbeatLiveness();
        final InputMessageProcessor gwInputProcessor =
            new InputMessageProcessor(inputRingBuffer, pollIdleStrategy, FRAME_COUNT_LIMIT, heartbeatLiveness,
                rspStream, this, "gwInputProcessor");

        gwInputProcessorList.add(gwInputProcessor);

        final long subscriptionId = gwInputProcessor.subscriptionId();
        helios.addGatewaySubscription(subscriptionId, this);

        report.addRequestProcessor(gwOutputProcessor);
        report.addResponseProcessor(gwInputProcessor);

        return gatewayHandler;
    }

    @Override
    public Gateway<T> addEventChannel(final AeronStream eventStream)
    {
        Objects.requireNonNull(eventStream, "eventStream");

        final IdleStrategy readIdleStrategy = helios.context().readIdleStrategy();

        final ByteBuffer eventBuffer = DirectBufferAllocator.allocateCacheAligned((16 * 1024) + TRAILER_LENGTH); // TODO: configure
        final RingBuffer eventRingBuffer = new OneToOneRingBuffer(new UnsafeBuffer(eventBuffer));

        ringBufferPool.addEventRingBuffer(eventStream, eventRingBuffer);

        final int heartbeatLiveness = helios.context().heartbeatLiveness();
        final InputMessageProcessor eventProcessor =
            new InputMessageProcessor(eventRingBuffer, readIdleStrategy, FRAME_COUNT_LIMIT,
                heartbeatLiveness, eventStream, null, "eventProcessor"); // FIXME: eventProcessor name

        eventProcessorList.add(eventProcessor);

        return this;
    }

    @Override
    public Report report()
    {
        return report;
    }

    @Override
    public void start()
    {
        gatewayProcessorList.forEach(ProcessorHelper::start);
        eventProcessorList.forEach(ProcessorHelper::start);
        gwOutputProcessorList.forEach(ProcessorHelper::start);
        gwInputProcessorList.forEach(ProcessorHelper::start);
    }

    @Override
    public Gateway<T> availableAssociationHandler(final AvailableAssociationHandler handler)
    {
        availableAssociationHandler = handler;
        return this;
    }

    @Override
    public Gateway<T> unavailableAssociationHandler(final UnavailableAssociationHandler handler)
    {
        unavailableAssociationHandler = handler;
        return this;
    }

    @Override
    public void onAvailableImage(final Image image)
    {
        gwInputProcessorList.forEach((p) -> p.onAvailableImage(image));
    }

    @Override
    public void onUnavailableImage(final Image image)
    {
        gwInputProcessorList.forEach((p) -> p.onUnavailableImage(image));
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
        gwInputProcessorList.forEach(CloseHelper::quietClose);
        gwOutputProcessorList.forEach(CloseHelper::quietClose);
        eventProcessorList.forEach(CloseHelper::quietClose);
        gatewayProcessorList.forEach(CloseHelper::quietClose);
    }
}
