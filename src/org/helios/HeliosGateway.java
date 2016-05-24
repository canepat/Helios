package org.helios;

import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.gateway.*;
import org.helios.infra.RateReport;
import org.helios.util.ProcessorHelper;

import java.nio.ByteBuffer;

import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

public class HeliosGateway<T extends GatewayHandler> implements Gateway<T>
{
    private final ServiceResponseProcessor svcResponseProcessor;
    private final ServiceRequestProcessor svcRequestProcessor;
    private final GatewayProcessor<T> gatewayProcessor;
    private final RateReport report;

    public HeliosGateway(final HeliosContext context, final AeronStream reqStream, final AeronStream rspStream,
        final GatewayHandlerFactory<T> factory)
    {
        final ByteBuffer outputBuffer = ByteBuffer.allocateDirect((16 * 1024) + TRAILER_LENGTH); // TODO: configure
        final RingBuffer outputRingBuffer = new OneToOneRingBuffer(new UnsafeBuffer(outputBuffer));

        svcRequestProcessor = new ServiceRequestProcessor(outputRingBuffer, reqStream);

        final ByteBuffer inputBuffer = ByteBuffer.allocateDirect((16 * 1024) + TRAILER_LENGTH); // TODO: configure
        final RingBuffer inputRingBuffer = new OneToOneRingBuffer(new UnsafeBuffer(inputBuffer));

        gatewayProcessor = new GatewayProcessor<>(inputRingBuffer, factory.createGatewayHandler(outputRingBuffer));

        svcResponseProcessor = new ServiceResponseProcessor(inputRingBuffer, rspStream);

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
