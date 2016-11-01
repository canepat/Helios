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
import org.helios.util.DirectBufferAllocator;
import org.helios.util.ProcessorHelper;
import org.helios.util.RingBufferPool;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

public class HeliosGateway<T extends GatewayHandler> implements Gateway<T>, AssociationHandler,
    AvailableImageHandler, UnavailableImageHandler
{
    private static final int FRAME_COUNT_LIMIT = Integer.getInteger("helios.gateway.poll.frame_count_limit", 10); // TODO: from HeliosContext

    private final Helios helios;
    private final InputMessageProcessor svcResponseProcessor;
    private final RingBufferPool ringBufferPool;
    private final List<OutputMessageProcessor> svcRequestProcessorList;
    private final RingBufferProcessor<T> gatewayProcessor;
    private final List<InputMessageProcessor> eventProcessorList;
    private final List<RateReport> reportList;
    private AvailableAssociationHandler availableAssociationHandler;
    private UnavailableAssociationHandler unavailableAssociationHandler;

    public HeliosGateway(final Helios helios, final GatewayHandlerFactory<T> factory)
    {
        this.helios = helios;

        ringBufferPool = new RingBufferPool();
        svcRequestProcessorList = new ArrayList<>();
        eventProcessorList = new ArrayList<>();
        reportList = new ArrayList<>();

        final ByteBuffer inputBuffer = DirectBufferAllocator.allocateCacheAligned((16 * 1024) + TRAILER_LENGTH); // TODO: configure
        final RingBuffer inputRingBuffer = new OneToOneRingBuffer(new UnsafeBuffer(inputBuffer));

        final T gatewayHandler = factory.createGatewayHandler(ringBufferPool);
        gatewayProcessor = new RingBufferProcessor<>(inputRingBuffer, gatewayHandler, new BusySpinIdleStrategy(), "gwProcessor");

        final IdleStrategy pollIdleStrategy = helios.context().subscriberIdleStrategy();
        svcResponseProcessor = new InputMessageProcessor(inputRingBuffer, pollIdleStrategy, FRAME_COUNT_LIMIT, "svcResponseProcessor");
    }

    @Override
    public Gateway<T> addEndPoint(final AeronStream reqStream, final AeronStream rspStream)
    {
        Objects.requireNonNull(reqStream, "reqStream");
        Objects.requireNonNull(rspStream, "rspStream");

        final IdleStrategy idleStrategy = new BusySpinIdleStrategy();

        final ByteBuffer outputBuffer = DirectBufferAllocator.allocateCacheAligned((16 * 1024) + TRAILER_LENGTH); // TODO: configure
        final RingBuffer outputRingBuffer = new OneToOneRingBuffer(new UnsafeBuffer(outputBuffer));

        ringBufferPool.addOutputRingBuffer(reqStream, outputRingBuffer);

        final OutputMessageProcessor svcRequestProcessor =
            new OutputMessageProcessor(outputRingBuffer, reqStream, idleStrategy, "svcRequestProcessor");

        svcRequestProcessorList.add(svcRequestProcessor);

        final long subscriptionId = svcResponseProcessor.addSubscription(rspStream);
        helios.addGatewaySubscription(subscriptionId, this);

        reportList.add(new GatewayReport(svcRequestProcessor, svcResponseProcessor));

        return this;
    }

    @Override
    public Gateway<T> addEventChannel(final AeronStream eventStream)
    {
        Objects.requireNonNull(eventStream, "eventStream");

        final IdleStrategy readIdleStrategy = helios.context().readIdleStrategy();

        final ByteBuffer eventBuffer = DirectBufferAllocator.allocateCacheAligned((16 * 1024) + TRAILER_LENGTH); // TODO: configure
        final RingBuffer eventRingBuffer = new OneToOneRingBuffer(new UnsafeBuffer(eventBuffer));

        ringBufferPool.addEventRingBuffer(eventStream, eventRingBuffer);

        final InputMessageProcessor eventProcessor =
            new InputMessageProcessor(eventRingBuffer, readIdleStrategy, FRAME_COUNT_LIMIT, "eventProcessor"); // FIXME: eventProcessor name

        eventProcessorList.add(eventProcessor);

        return this;
    }

    @Override
    public List<RateReport> reportList()
    {
        return reportList;
    }

    @Override
    public T handler()
    {
        return gatewayProcessor.handler();
    }

    @Override
    public void start()
    {
        ProcessorHelper.start(gatewayProcessor);
        eventProcessorList.forEach(ProcessorHelper::start);
        svcRequestProcessorList.forEach(ProcessorHelper::start);
        ProcessorHelper.start(svcResponseProcessor);
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
        svcResponseProcessor.onAvailableImage(image);

        // TODO: send HEARTBEAT to service through GatewayHandler

        onAssociationEstablished(); // TODO: remove after HEARTBEAT handling
    }

    @Override
    public void onUnavailableImage(final Image image)
    {
        svcResponseProcessor.onUnavailableImage(image);

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
        CloseHelper.quietClose(svcResponseProcessor);
        svcRequestProcessorList.forEach(CloseHelper::quietClose);
        eventProcessorList.forEach(CloseHelper::quietClose);
        CloseHelper.quietClose(gatewayProcessor);
    }
}
