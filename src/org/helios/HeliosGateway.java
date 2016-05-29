package org.helios;

import org.agrona.CloseHelper;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.gateway.*;
import org.helios.infra.InputMessageProcessor;
import org.helios.infra.OutputMessageProcessor;
import org.helios.infra.RateReport;
import org.helios.infra.RingBufferProcessor;
import org.helios.util.ProcessorHelper;

import java.nio.ByteBuffer;

import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

public class HeliosGateway<T extends GatewayHandler> implements Gateway<T>
{
    private static final int FRAME_COUNT_LIMIT = Integer.getInteger("helios.gateway.poll.frame_count_limit", 10);

    private final InputMessageProcessor svcResponseProcessor;
    private final OutputMessageProcessor svcRequestProcessor;
    private final RingBufferProcessor<T> gatewayProcessor;
    private final RateReport report;

    public HeliosGateway(final HeliosContext context, final AeronStream reqStream, final AeronStream rspStream,
        final GatewayHandlerFactory<T> factory)
    {
        final IdleStrategy idleStrategy = new BusySpinIdleStrategy();

        final ByteBuffer outputBuffer = ByteBuffer.allocateDirect((16 * 1024) + TRAILER_LENGTH); // TODO: configure
        final RingBuffer outputRingBuffer = new OneToOneRingBuffer(new UnsafeBuffer(outputBuffer));

        svcRequestProcessor = new OutputMessageProcessor(outputRingBuffer, reqStream, idleStrategy, "svcRequestProcessor");

        final ByteBuffer inputBuffer = ByteBuffer.allocateDirect((16 * 1024) + TRAILER_LENGTH); // TODO: configure
        final RingBuffer inputRingBuffer = new OneToOneRingBuffer(new UnsafeBuffer(inputBuffer));

        gatewayProcessor = new RingBufferProcessor<>(inputRingBuffer, factory.createGatewayHandler(outputRingBuffer),
            idleStrategy, "gwProcessor");

        svcResponseProcessor = new InputMessageProcessor(inputRingBuffer, rspStream, idleStrategy, FRAME_COUNT_LIMIT,
            "svcResponseProcessor");

        report = new GatewayReport(svcRequestProcessor, svcResponseProcessor);
    }

    @Override
    public RateReport report()
    {
        return report;
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
        ProcessorHelper.start(svcRequestProcessor);
        ProcessorHelper.start(svcResponseProcessor);
    }

    @Override
    public void close() throws Exception
    {
        CloseHelper.quietClose(svcResponseProcessor);
        CloseHelper.quietClose(svcRequestProcessor);
        CloseHelper.quietClose(gatewayProcessor);
    }
}
