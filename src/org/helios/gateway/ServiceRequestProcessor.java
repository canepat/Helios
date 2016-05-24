package org.helios.gateway;

import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.AeronStream;
import org.helios.infra.RingBufferProcessor;

public final class ServiceRequestProcessor extends RingBufferProcessor<ServiceRequestHandler>
{
    public ServiceRequestProcessor(final RingBuffer outputRingBuffer, final AeronStream reqStream)
    {
        this(outputRingBuffer, new ServiceRequestHandler(reqStream, new BusySpinIdleStrategy()));
    }

    public ServiceRequestProcessor(final RingBuffer outputRingBuffer, final ServiceRequestHandler svcRequestHandler)
    {
        super(outputRingBuffer, svcRequestHandler, new BusySpinIdleStrategy(), "svcRequestProcessor");
    }

    public long msgRequested()
    {
        return handler().msgRequested();
    }

    public long bytesRequested()
    {
        return handler().bytesRequested();
    }
}
